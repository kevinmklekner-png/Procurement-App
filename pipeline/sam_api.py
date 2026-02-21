"""
SAM.gov Opportunities API client.
"""

import os
import sys
import time
import requests
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config


# Map notice type names to SAM.gov ptype codes
NOTICE_TYPE_MAP = {
    "Solicitation": "o",
    "Presolicitation": "p",
    "Sources Sought": "s",
    "Combined Synopsis/Solicitation": "k",
    "Special Notice": "r",
}


@dataclass
class Opportunity:
    notice_id: str = ""
    solicitation_number: str = ""
    title: str = ""
    description: str = ""
    department: str = ""
    sub_tier: str = ""
    office: str = ""
    naics_code: str = ""
    naics_description: str = ""
    set_aside: str = ""
    posted_date: Optional[datetime] = None
    response_deadline: Optional[datetime] = None
    place_of_performance: str = ""
    primary_contact: str = ""
    primary_contact_email: str = ""
    url: str = ""
    description_url: str = ""
    resource_links: list = field(default_factory=list)


class SAMApiClient:
    """Client for the SAM.gov Opportunities API."""

    def __init__(self):
        self.base_url = config.SAM_API_BASE_URL
        self.api_key = config.SAM_API_KEY
        self.session = requests.Session()

    def get_opportunities_paginated(
        self,
        max_results: int = 5000,
        page_size: int = 100,
        posted_from: str = "",
        posted_to: str = "",
        notice_type: str = "Solicitation",
        keyword: str = "",
    ) -> List[Opportunity]:
        """
        Fetch opportunities with automatic pagination.

        Args:
            max_results: Maximum total results to return.
            page_size: Results per API call (max 1000).
            posted_from: Start date as YYYY-MM-DD.
            posted_to: End date as YYYY-MM-DD.
            notice_type: One of the keys in NOTICE_TYPE_MAP.
            keyword: Search keyword to filter results.

        Returns:
            List of Opportunity objects.
        """
        page_size = min(page_size, 1000)
        ptype = NOTICE_TYPE_MAP.get(notice_type, "o")

        # Convert dates from YYYY-MM-DD to MM/dd/yyyy
        from_date = _reformat_date(posted_from)
        to_date = _reformat_date(posted_to)

        opportunities: List[Opportunity] = []
        offset = 0

        while len(opportunities) < max_results:
            params = {
                "api_key": self.api_key,
                "postedFrom": from_date,
                "postedTo": to_date,
                "ptype": ptype,
                "limit": page_size,
                "offset": offset,
            }
            if keyword:
                params["q"] = keyword

            data = self._request_with_retry(params)

            raw_opps = data.get("opportunitiesData", [])
            if not raw_opps:
                break

            for item in raw_opps:
                if len(opportunities) >= max_results:
                    break
                opportunities.append(_parse_opportunity(item))

            # If we got fewer than page_size, there are no more pages
            if len(raw_opps) < page_size:
                break

            offset += page_size

        return opportunities

    def _request_with_retry(self, params: dict, max_retries: int = 5) -> dict:
        """Make an API request with exponential backoff on 429 errors."""
        for attempt in range(max_retries):
            resp = self.session.get(self.base_url, params=params, timeout=60)
            if resp.status_code == 429:
                wait = 2 ** attempt * 10  # 10s, 20s, 40s, 80s, 160s
                print(f"    Rate limited (429). Waiting {wait}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        # Final attempt — let it raise if it fails
        resp = self.session.get(self.base_url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def get_description_html(self, notice_id: str) -> str:
        """Fetch the full HTML description for an opportunity."""
        url = f"https://api.sam.gov/prod/opportunities/v1/noticedesc"
        params = {"noticeid": notice_id, "api_key": self.api_key}
        resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        # The API may return JSON with a 'content' field or raw HTML
        try:
            data = resp.json()
            return data.get("content", data.get("description", resp.text))
        except ValueError:
            return resp.text

    def get_resource_links(self, notice_id: str) -> list:
        """Fetch resource links (attached files) for an opportunity."""
        url = f"https://api.sam.gov/opportunities/v2/search"
        params = {
            "api_key": self.api_key,
            "noticeid": notice_id,
            "limit": 1,
        }
        resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        opps = data.get("opportunitiesData", [])
        if not opps:
            return []
        return opps[0].get("resourceLinks", [])

    def download_attachment(self, file_url: str) -> bytes:
        """Download a file attachment by URL."""
        resp = self.session.get(file_url, timeout=120)
        resp.raise_for_status()
        return resp.content


def _reformat_date(date_str: str) -> str:
    """Convert YYYY-MM-DD to MM/dd/yyyy."""
    if not date_str:
        return ""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%m/%d/%Y")


def _parse_date(value: str) -> Optional[datetime]:
    """Parse a date string from the API into a datetime, or None."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%m/%d/%Y"):
        try:
            return datetime.strptime(value.rstrip("Z"), fmt.replace("%z", ""))
        except ValueError:
            continue
    return None


def _parse_opportunity(item: dict) -> Opportunity:
    """Map a raw API record to an Opportunity."""
    opp = Opportunity()

    opp.notice_id = item.get("noticeId", "")
    opp.solicitation_number = item.get("solicitationNumber", "")
    opp.title = item.get("title", "")
    opp.description = item.get("description", "")

    # Organization hierarchy
    full_path = item.get("fullParentPathName", "")
    parts = [p.strip() for p in full_path.split(".")] if full_path else []
    opp.department = parts[0] if len(parts) > 0 else ""
    opp.sub_tier = parts[1] if len(parts) > 1 else ""
    opp.office = parts[2] if len(parts) > 2 else ""

    # NAICS
    naics = item.get("naicsCodes", [])
    if naics:
        first = naics[0] if isinstance(naics[0], dict) else {"code": str(naics[0])}
        opp.naics_code = first.get("code", str(naics[0]) if naics else "")
        opp.naics_description = first.get("description", "")

    opp.set_aside = item.get("typeOfSetAsideDescription", "")

    opp.posted_date = _parse_date(item.get("postedDate", ""))
    opp.response_deadline = _parse_date(item.get("responseDeadLine", ""))

    # Place of performance
    pop = item.get("placeOfPerformance", {})
    if isinstance(pop, dict):
        city_info = pop.get("city", {})
        state_info = pop.get("state", {})
        city = city_info.get("name", "") if isinstance(city_info, dict) else str(city_info)
        state = state_info.get("code", "") if isinstance(state_info, dict) else str(state_info)
        opp.place_of_performance = f"{city}, {state}".strip(", ")
    elif isinstance(pop, str):
        opp.place_of_performance = pop

    # Point of contact
    contacts = item.get("pointOfContact", [])
    if contacts and isinstance(contacts, list):
        primary = contacts[0]
        name_parts = [primary.get("firstName", ""), primary.get("lastName", "")]
        opp.primary_contact = " ".join(p for p in name_parts if p)
        opp.primary_contact_email = primary.get("email", "")

    opp.url = item.get("uiLink", "")
    opp.description_url = item.get("description", "")
    opp.resource_links = item.get("resourceLinks", []) or []

    return opp
