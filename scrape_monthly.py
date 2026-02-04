#!/usr/bin/env python3
"""
Monthly Bina.az Scraper
Simple one-click script to scrape new data for the current month
Saves as: bina_sale_202601.csv, bina_rent_202601.csv, etc.
"""

import asyncio
from datetime import datetime
from pathlib import Path
from rent import BinaRentScraper
from sale import BinaScraper


async def main():
    """Scrape current month data"""
    # Get current month in YYYYMM format
    current_month = datetime.now().strftime('%Y%m')
    
    print("\n" + "=" * 80)
    print(f"BINA.AZ MONTHLY SCRAPER - {datetime.now().strftime('%B %Y')}")
    print("=" * 80)
    print(f"\nFiles will be saved as:")
    print(f"  - bina_sale_{current_month}.csv")
    print(f"  - bina_sale_{current_month}.xlsx")
    print(f"  - bina_rent_{current_month}.csv")
    print(f"  - bina_rent_{current_month}.xlsx")
    print("\n" + "=" * 80)
    
    # Scrape both
    scraped_items = 0
    
    # Scrape SALE
    print("\n🔄 Scraping SALE properties...")
    try:
        async with BinaScraper() as scraper:
            items = await scraper.scrape_all()
            if items:
                scraper.save_to_csv(f"bina_sale_{current_month}.csv")
                scraper.save_to_xlsx(f"bina_sale_{current_month}.xlsx")
                print(f"✓ Saved: {len(items)} sale properties")
                scraped_items += len(items)
    except Exception as e:
        print(f"✗ Error scraping sales: {e}")
    
    # Scrape RENT
    print("\n🔄 Scraping RENT properties...")
    try:
        async with BinaRentScraper() as scraper:
            items = await scraper.scrape_all()
            if items:
                scraper.save_to_csv(f"bina_rent_{current_month}.csv")
                scraper.save_to_xlsx(f"bina_rent_{current_month}.xlsx")
                print(f"✓ Saved: {len(items)} rent properties")
                scraped_items += len(items)
    except Exception as e:
        print(f"✗ Error scraping rentals: {e}")
    
    print("\n" + "=" * 80)
    print(f"✓ Scraping complete! Total items: {scraped_items:,}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
