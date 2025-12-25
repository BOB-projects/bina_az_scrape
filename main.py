#!/usr/bin/env python3
"""
Main runner for Bina.az scrapers
Runs both rent and sale property scrapers with category extraction
"""

import asyncio
import sys
from pathlib import Path

# Import scrapers
from rent import BinaRentScraper
from sale import BinaScraper


async def main():
    """Run both scrapers"""
    import logging
    logger = logging.getLogger(__name__)
    
    print("\n" + "=" * 80)
    print("BINA.AZ PROPERTY SCRAPER - RENT & SALE")
    print("=" * 80)
    print("\nChoose what to scrape:")
    print("1. Rent properties only")
    print("2. Sale properties only")
    print("3. Both rent and sale")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == "1" or choice == "3":
        print("\n" + "=" * 80)
        print("SCRAPING RENT PROPERTIES...")
        print("=" * 80)
        async with BinaRentScraper() as scraper:
            items = await scraper.scrape_all()
            if items:
                scraper.save_to_json()
                scraper.save_to_csv()
                scraper.save_to_xlsx()
                print(f"\n✓ Rent scraping complete: {len(items)} properties saved")
    
    if choice == "2" or choice == "3":
        print("\n" + "=" * 80)
        print("SCRAPING SALE PROPERTIES...")
        print("=" * 80)
        async with BinaScraper() as scraper:
            items = await scraper.scrape_all()
            if items:
                scraper.save_to_json()
                scraper.save_to_csv()
                scraper.save_to_xlsx()
                print(f"\n✓ Sale scraping complete: {len(items)} properties saved")
    
    print("\n" + "=" * 80)
    print("DONE!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

