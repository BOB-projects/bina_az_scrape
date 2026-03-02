#!/usr/bin/env python3
"""
Shared utilities for Bina.az scrapers
"""

import re
import logging
import time
import asyncio
from typing import Optional, Dict, Tuple


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


async def get_cloudflare_session() -> Tuple[Dict, str]:
    """
    Launch a visible browser to solve Cloudflare challenge and extract cookies.
    Returns (cookies_dict, user_agent)
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logging.error("Playwright not installed. Cannot bypass Cloudflare.")
        return {}, ""

    print("\\n" + "!" * 80)
    print("CLOUDFLARE BYPASS NEEDED")
    print("Launching browser... Please solve the CAPTCHA if prompted.")
    print("The browser will close automatically once the site loads.")
    print("!" * 80 + "\\n")

    cookies = {}
    user_agent = ""

    async with async_playwright() as p:
        # Launch headed browser
        browser = await p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled']
        )
        
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800}
        )
        
        page = await context.new_page()
        
        try:
            # Go to specific page
            await page.goto('https://bina.az/alqi-satqi', wait_until='domcontentloaded')
            
            # Wait for title to NOT indicate challenge
            max_wait = 180  # Give user 3 minutes max
            start_time = time.time()
            
            print("Waiting for page load/captcha solution...")
            while time.time() - start_time < max_wait:
                try:
                    if page.is_closed():
                        print("\nBrowser file closed by user.")
                        break
                        
                    title = await page.title()
                    url = page.url
                    
                    # Check for success indicators
                    if ("alqi-satqi" in url or "bina.az" in url) and ("Just a moment" not in title and "Cloudflare" not in title):
                         # Double check we are on the site
                        try:
                            if await page.locator(".items-list").count() > 0 or await page.locator("header").count() > 0:
                                print(f"\\n✓ Cloudflare passed successfully! (Title: {title})")
                                # Give it a moment to fully settle cookies
                                await asyncio.sleep(2)
                                break
                        except:
                            pass
                except Exception:
                    # Ignore errors during navigation/reloading (like execution context destroyed)
                    pass
                
                print(f"Waiting for bypass... ({int(max_wait - (time.time() - start_time))}s remaining)   ", end='\\r')
                await asyncio.sleep(1)
            
            # Get cookies and UA
            cookies_list = await context.cookies()
            cookies = {c['name']: c['value'] for c in cookies_list}
            user_agent = await page.evaluate("navigator.userAgent")
            
        except Exception as e:
            logging.error(f"Error getting session: {e}")
        finally:
            print("\\nClosing browser helper...")
            await browser.close()
            
    return cookies, user_agent

