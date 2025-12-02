"""
Run VLR Scraper 
"""
import sys
import time
from vlr_scraper_enhanced import VLRScraper
from sql_server_integration_enhanced import SQLServerInserter


# SQL Server Connection Settings
SERVER_NAME = 'localhost'  
DATABASE_NAME = 'vlr_matches'
USE_WINDOWS_AUTH = True

# SQL Server Authentication (only if USE_WINDOWS_AUTH = False)
SQL_USER = 'sa'
SQL_PASSWORD = 'zelda'

# Scraping Settings
HEADLESS = True
DELAY_BETWEEN_MATCHES = 2


def main():
    """Main scraper function"""
    
    # Check command line arguments
    if len(sys.argv) != 3:
        print("\n" + "="*70)
        print("VLR.GG MATCH SCRAPER")
        print("="*70)
        print("\nUsage: python run_scraper_enhanced.py START_PAGE END_PAGE")
        print("\nExamples:")
        print("  python run_scraper_enhanced.py 1 1    # Scrape page 1")
        print("  python run_scraper_enhanced.py 1 3    # Scrape pages 1-3")
        print("\n" + "="*70)
        sys.exit(1)
    
    try:
        start_page = int(sys.argv[1])
        end_page = int(sys.argv[2])
    except ValueError:
        print("ERROR: Page numbers must be integers")
        sys.exit(1)
    
    if start_page > end_page or start_page < 1:
        print("ERROR: START_PAGE must be ≤ END_PAGE and > 0")
        sys.exit(1)
    
    print(f"\nScraping pages {start_page} to {end_page}...\n")
    
    # Connect to database
    try:
        db = SQLServerInserter(
            server=SERVER_NAME,
            database=DATABASE_NAME,
            use_windows_auth=USE_WINDOWS_AUTH,
            user=SQL_USER if not USE_WINDOWS_AUTH else "",
            password=SQL_PASSWORD if not USE_WINDOWS_AUTH else ""
        )
    except Exception as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)
    
    try:
        # Step 1: Discover match URLs
        all_urls = []
        
        for page in range(start_page, end_page + 1):
            print(f"Scanning page {page}...", end=' ')
            try:
                links = VLRScraper.get_match_links_by_page_static(page)
                all_urls.extend(links)
                print(f"{len(links)} matches found")
                time.sleep(1)
            except Exception as e:
                print(f"Error: {e}")
        
        unique_urls = list(set(all_urls))
        print(f"\nTotal unique matches: {len(unique_urls)}\n")
        
        if not unique_urls:
            print("No matches found")
            return
        
        # Step 2: Scrape and insert matches
        success_count = 0
        skip_count = 0
        error_count = 0
        
        for i, url in enumerate(unique_urls, 1):
            print(f"[{i}/{len(unique_urls)}] {url.split('/')[-1][:50]}...", end=' ')
            
            try:
                # Scrape the match
                with VLRScraper(headless=HEADLESS) as scraper:
                    match_data = scraper.scrape_match(url)
                
                # Get basic info
                teams = match_data.get('teams', {})
                team1 = teams.get('team1', {}).get('name', 'Unknown')
                team2 = teams.get('team2', {}).get('name', 'Unknown')
                
                # Insert into database
                db.insert_match_data(match_data, skip_if_exists=True)
                
                success_count += 1
                print(f"✓ {team1} vs {team2}")
                
                # Wait before next match
                if i < len(unique_urls):
                    time.sleep(DELAY_BETWEEN_MATCHES)
                
            except Exception as e:
                error_msg = str(e)
                if "already exists" in error_msg.lower() or "skip" in error_msg.lower():
                    skip_count += 1
                    print("(skipped)")
                else:
                    error_count += 1
                    print(f"✗ {str(e)[:50]}")
                
                time.sleep(5)
        
        # Final summary
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        print(f"Successfully inserted: {success_count}")
        print(f"Skipped (duplicates):  {skip_count}")
        print(f"Errors:                {error_count}")
        print(f"Total processed:       {len(unique_urls)}")
        
        # Database stats
        try:
            db.cursor.execute("SELECT COUNT(*) FROM Matches")
            total_matches = db.cursor.fetchone()[0]
            db.cursor.execute("SELECT COUNT(*) FROM Teams WHERE region IS NOT NULL")
            teams_with_region = db.cursor.fetchone()[0]
            db.cursor.execute("SELECT COUNT(*) FROM Teams")
            total_teams = db.cursor.fetchone()[0]
            
            print(f"\nDatabase: {total_matches} matches, {total_teams} teams ({teams_with_region} with regions)")
        except:
            pass
        
        print("="*70 + "\n")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nCritical error: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()