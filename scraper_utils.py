#!/usr/bin/env python3
"""
Shared utilities for Bina.az scrapers
"""

import re
from typing import Optional


def extract_category_from_html(html: str) -> Optional[str]:
    """Extract category from detail page HTML - looks for 'Kateqoriya' field
    
    Returns one of:
    - 'Yeni tikili' (New building)
    - 'Köhnə tikili' (Old building)
    - Any other category found (e.g., 'Obyekt')
    - None if no category found
    """
    # Pattern 1: Direct text search (most reliable)
    if 'Yeni tikili' in html:
        return 'Yeni tikili'
    elif 'Köhnə tikili' in html:
        return 'Köhnə tikili'
    
    # Pattern 2: Look for "Kateqoriya" label followed by the value (capture any category)
    kateqoriya_match = re.search(r'Kateqoriya["\']?\s*(?:</?\w+[^>]*>)*\s*([^<>\n]+?)(?:</|$)', html, re.IGNORECASE)
    if kateqoriya_match:
        category = kateqoriya_match.group(1).strip()
        if category and len(category) < 100:  # Sanity check
            return category
    
    # Pattern 3: Look in data attributes or JSON
    data_match = re.search(r'"category"\s*:\s*"([^"]+)"', html)
    if data_match:
        return data_match.group(1)
    
    return None
