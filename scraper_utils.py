#!/usr/bin/env python3
"""
Shared utilities for Bina.az scrapers
"""

import re
from typing import Optional


def extract_category_from_html(html: str) -> Optional[str]:
    """Extract category from detail page HTML - ONLY looks for 'Kateqoriya' field
    
    Returns specific property categories like:
    - 'Yeni tikili' (New building)
    - 'Köhnə tikili' (Old building)
    - 'Apartament'
    - 'Ofis'
    - 'Qaraj'
    - 'Torpaq'
    - 'Həyət evi'
    - etc.
    
    Returns None if Kateqoriya field is not found (no generic/fallback categories).
    """
    
    # ONLY extract from the specific Kateqoriya product property
    # This captures specific categories like "Yeni tikili", "İşlənmiş", etc.
    kateqoriya_match = re.search(
        r'<label class="product-properties__i-name">Kateqoriya</label>\s*<span class="product-properties__i-value">([^<]+)</span>',
        html
    )
    if kateqoriya_match:
        category = kateqoriya_match.group(1).strip()
        if category and len(category) < 100:
            return category
    
    return None  # Return None if Kateqoriya not found (no fallback)
