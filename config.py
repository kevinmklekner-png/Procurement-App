"""
Configuration for SAM.gov API access.
"""

import os

SAM_API_KEY = os.environ.get("SAM_API_KEY", "")
SAM_API_BASE_URL = "https://api.sam.gov/opportunities/v2/search"


def validate_config():
    """Check that required configuration is set."""
    if not SAM_API_KEY:
        raise RuntimeError(
            "SAM_API_KEY environment variable is not set. "
            "Get a key at https://sam.gov/content/entity-registration"
        )
