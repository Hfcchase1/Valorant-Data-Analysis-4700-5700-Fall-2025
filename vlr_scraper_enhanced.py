"""
VLR.gg Match Data Scraper 
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
import time
import re
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException


class VLRScraper:
    """Enhanced scraper for VLR.gg match data"""
    
    def __init__(self, headless: bool = False):
        """Initialize the scraper with Selenium WebDriver"""

        self.driver = None
        self.headless = headless
        self._setup_driver()
    
    def _setup_driver(self):
        """Setup Selenium WebDriver with Firefox"""
        try:
            firefox_options = Options()
            if self.headless:
                firefox_options.add_argument('-headless')
                firefox_options.add_argument('--no-sandbox')
                firefox_options.add_argument('--disable-setuid-sandbox')
                firefox_options.add_argument('--disable-dev-shm-usage')
                firefox_options.add_argument('--disable-gpu')
                firefox_options.add_argument('--window-size=1920,1080')
                firefox_options.add_argument('--disable-extensions')
                firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

            service = Service(GeckoDriverManager().install())
            self.driver = webdriver.Firefox(service=service, options=firefox_options)
            print("WebDriver initialized successfully using Firefox")
        except Exception as e:
            print(f"Failed to initialize WebDriver: {e}")
            raise
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            print("WebDriver closed")

    def get_match_links_by_page(self, page_number: int) -> List[str]:
        """
        Get match links from VLR.gg results page using requests + BeautifulSoup
        """
        return self.get_match_links_by_page_static(page_number)
    
    @staticmethod
    def get_match_links_by_page_static(page_number: int) -> List[str]:
        """
        Static method to get match links without requiring a WebDriver instance
        """
        base_url = "https://www.vlr.gg/matches/results"
        full_url = f"{base_url}?page={page_number}"
        
        print(f"Loading results page: {full_url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
            
            response = requests.get(full_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            match_links = []
            all_links = soup.find_all('a', href=True)
            
            for link in all_links:
                href = link.get('href', '')
                if re.match(r'^/\d+.*vs.*', href):
                    full_url = 'https://www.vlr.gg' + href
                    match_links.append(full_url)
            
            unique_links = list(set(match_links))
            
            return unique_links
            
        except Exception as e:
            print(f"❌ Error fetching page {page_number}: {e}")
            return []
    
    def scrape_match(self, match_url: str) -> Dict:
        """
        Scrape complete match data from VLR.gg
        Args:
            match_url: URL of the match page
        Returns:
            Dictionary containing all match data
        """
        try:
            pass
            self.driver.get(match_url)
            
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "match-header"))
            )
            
            # Click spoiler button if exists
            try:
                spoiler_btn = self.driver.find_element(By.CLASS_NAME, 'js-spoiler')
                if spoiler_btn and 'spoiler' in spoiler_btn.get_attribute('class'):
                    spoiler_btn.click()
                    time.sleep(0.5)
            except:
                pass
            
            time.sleep(2)
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Extract data
            match_info = self._extract_match_info(soup)
            teams_data = self._extract_teams(soup)
            maps_data = self._extract_maps(soup)
            
            # Scrape team details (region, etc.)
            team1_details = self._scrape_team_details(teams_data['team1'].get('url'))
            team2_details = self._scrape_team_details(teams_data['team2'].get('url'))
            
            # DEBUG: Print what was scraped before update
            
            teams_data['team1'].update(team1_details)
            teams_data['team2'].update(team2_details)
            
            # DEBUG: Print teams_data after update
            
            # Scrape tournament details
            tournament_url = match_info.get('tournament_url')
            if tournament_url:
                tournament_details = self._scrape_tournament_details(tournament_url)
                match_info.update(tournament_details)
            
            team1_name = teams_data['team1'].get('name', 'Team 1')
            team2_name = teams_data['team2'].get('name', 'Team 2')
            
            # Recalculate scores based on map data
            team1_wins = sum(1 for m in maps_data if m.get('team1_score', 0) > m.get('team2_score', 0))
            team2_wins = sum(1 for m in maps_data if m.get('team2_score', 0) > m.get('team1_score', 0))
            teams_data['team1']['score'] = team1_wins
            teams_data['team2']['score'] = team2_wins

            
            # Extract player stats
            player_stats = self._extract_player_stats_all_maps(team1_name, team2_name, maps_data)
            
            # Scrape player details (region, team join date)
            player_stats = self._enrich_player_stats(player_stats, team1_name, team2_name)
            
            # Add aggregated overall stats
            overall_stats = aggregate_player_stats(player_stats)
            all_stats = overall_stats + player_stats
            
            match_data = {
                'url': match_url,
                'match_info': match_info,
                'teams': teams_data,
                'maps': maps_data,
                'player_stats': all_stats,
            }
            
            return match_data
            
        except Exception as e:
            print(f"Failed to scrape match: {e}")
            raise
    
    def _scrape_team_details(self, team_url: str) -> Dict:
        """
        Scrape team page for region and other details
        """
        details = {'region': None, 'logo_url': None}
        
        if not team_url:
            return details
        
        try:
            pass
            response = requests.get(team_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract region from team header
            region_div = soup.find('div', class_='team-header-country')
            if region_div:
                region_text = region_div.text.strip()
                details['region'] = region_text
                print(f"    Region: {region_text}")
            
            # Extract logo URL
            logo_img = soup.find('img', class_='team-header-logo')
            if logo_img and logo_img.get('src'):
                details['logo_url'] = logo_img.get('src')
            
        except Exception as e:
            print(f"    Warning: Could not fetch team details: {e}")
        
        return details
    
    def _scrape_tournament_details(self, tournament_url: str) -> Dict:
        """
        Scrape tournament page for prize pool, dates, participating teams
        """
        details = {
            'tournament_prize_pool': None,
            'tournament_start_date': None,
            'tournament_end_date': None,
            'tournament_teams': []
        }
        
        if not tournament_url:
            print(f"    Warning: No tournament URL provided")
            return details
        
        # Validate the URL
        if '/event/' not in tournament_url:
            print(f"    Warning: Invalid tournament URL (no /event/): {tournament_url}")
            return details
        
        try:
            pass
            response = requests.get(tournament_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract prize pool
            prize_elem = soup.find('div', class_='event-prize')
            if prize_elem:
                prize_text = prize_elem.text.strip()
                # Extract numeric value from prize (e.g., "$100,000" -> 100000)
                prize_match = re.search(r'[\d,]+', prize_text.replace('$', ''))
                if prize_match:
                    prize_str = prize_match.group().replace(',', '')
                    try:
                        details['tournament_prize_pool'] = int(prize_str)
                        print(f"    Prize Pool: ${details['tournament_prize_pool']:,}")
                    except ValueError:
                        pass
            
            # Extract dates
            dates_elem = soup.find('div', class_='event-dates')
            if dates_elem:
                dates_text = dates_elem.text.strip()
                # Try to parse dates like "Jul 1 - Aug 15, 2024"
                date_match = re.search(r'([A-Z][a-z]{2}\s+\d+)\s*-\s*([A-Z][a-z]{2}\s+\d+),?\s*(\d{4})', dates_text)
                if date_match:
                    try:
                        start_str = f"{date_match.group(1)} {date_match.group(3)}"
                        end_str = f"{date_match.group(2)} {date_match.group(3)}"
                        details['tournament_start_date'] = datetime.strptime(start_str, '%b %d %Y').date()
                        details['tournament_end_date'] = datetime.strptime(end_str, '%b %d %Y').date()
                        print(f"    Dates: {details['tournament_start_date']} to {details['tournament_end_date']}")
                    except ValueError as e:
                        print(f"    Warning: Could not parse dates: {e}")
            
            # Extract participating teams
            team_links = soup.find_all('a', href=re.compile(r'^/team/\d+'))
            team_names = []
            for link in team_links:
                team_name_elem = link.find('div', class_='text-of')
                if team_name_elem:
                    team_name = team_name_elem.text.strip()
                    if team_name and team_name not in team_names:
                        team_names.append(team_name)
            
            details['tournament_teams'] = team_names[:16]  # Limit to reasonable number
            if details['tournament_teams']:
                pass
            
        except Exception as e:
            print(f"    Warning: Could not fetch tournament details: {e}")
        
        return details
    
    def _enrich_player_stats(self, player_stats: List[Dict], team1_name: str, team2_name: str) -> List[Dict]:
        """
        Enrich player stats with region and team join date information
        """
        player_cache = {}
        
        for stat in player_stats:
            player_url = stat.get('player_url')
            if not player_url or player_url in player_cache:
                if player_url in player_cache:
                    stat.update(player_cache[player_url])
                continue
            
            try:
                response = requests.get(player_url, timeout=5)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract player region
                country_elem = soup.find('div', class_='ge-flag')
                if country_elem:
                    country_text = country_elem.text.strip()
                    stat['player_region'] = country_text
                else:
                    stat['player_region'] = 'Unknown'
                
                # Extract team join date (if available)
                # Look in player history for current team
                current_team = stat.get('team_name')
                team_history = soup.find_all('div', class_='wf-card')
                
                for card in team_history:
                    team_link = card.find('a', href=re.compile(r'^/team/'))
                    if team_link:
                        team_name_elem = team_link.find('div', class_='text-of')
                        if team_name_elem and team_name_elem.text.strip() == current_team:
                            # Try to find date
                            date_elem = card.find('div', class_='player-summary-join-date')
                            if date_elem:
                                date_text = date_elem.text.strip()
                                try:
                                    # Parse date like "Jan 15, 2024"
                                    join_date = datetime.strptime(date_text, '%b %d, %Y').date()
                                    stat['team_join_date'] = join_date
                                except ValueError:
                                    pass
                            break
                
                # Cache the results
                player_cache[player_url] = {
                    'player_region': stat.get('player_region', 'Unknown'),
                    'team_join_date': stat.get('team_join_date')
                }
                
                time.sleep(0.2)  # Be nice to the server
                
            except Exception as e:
                print(f"    Warning: Could not fetch player details for {stat.get('player_ign')}: {e}")
                stat['player_region'] = 'Unknown'
        
        return player_stats
    
    def _extract_match_info(self, soup: BeautifulSoup) -> Dict:
        """Extract basic match information including actual match date"""
        match_info = {}
        
        try:
            # Tournament info 
            tournament_link = soup.find('a', class_='match-header-event')
            
            tournament_found = False
            
            if tournament_link:
                href = tournament_link.get('href', '')
                
                # Only use links that go to /event/ pages
                if href and '/event/' in href:
                    match_info['tournament_url'] = 'https://www.vlr.gg' + href
                    
                    # Get tournament name 
                    tournament_name = None
                    tournament_text = tournament_link.find('div', style=lambda x: x and 'font-weight: 700' in x)
                    if tournament_text:
                        tournament_name = tournament_text.text.strip()
                    
                    # Fallback to link text
                    if not tournament_name:
                        tournament_name = tournament_link.get_text(separator=' ', strip=True)
                        tournament_name = ' '.join(tournament_name.split())
                        # Clean up - remove stage info after colon
                        if ':' in tournament_name:
                            tournament_name = tournament_name.split(':')[0].strip()
                    
                    # Parse from URL as last resort
                    if not tournament_name or len(tournament_name) < 3:
                        url_parts = href.split('/')
                        if len(url_parts) >= 3:
                            tournament_name = url_parts[2].replace('-', ' ').title()
                    
                    match_info['tournament_name'] = tournament_name
                    tournament_found = True
                
                # Also try to get match stage info
                stage_elem = tournament_link.find('div', class_='match-header-event-series')
                if stage_elem:
                    stage_text = stage_elem.get_text(strip=True)
                    match_info['match_type'] = stage_text
            
            # If no tournament found, it will default to "Unknown Tournament" in SQL
            if not tournament_found:
                print(f"  WARNING: No tournament information found on page")
            
            # Extract actual match date from timestamp
            date_elem = soup.find('div', class_='moment-tz-convert')
            if date_elem:
                date_ts = date_elem.get('data-utc-ts')
                if date_ts and date_ts.isdigit():
                    # Convert timestamp (milliseconds) to datetime
                    match_datetime = datetime.fromtimestamp(int(date_ts) / 1000)
                    match_info['match_date'] = match_datetime.strftime('%B %d, %Y')
                    match_info['match_datetime'] = match_datetime  # Store full datetime
                    print(f"  Match Date: {match_info['match_date']}")
                else:
                    # Fallback to text - try to parse it
                    date_text = date_elem.text.strip()
                    match_info['match_date'] = date_text if date_text else None
                    # Try to parse text date formats
                    if date_text:
                        try:
                            # Try format: "Thursday, November 13"
                            # Need to add year since it's missing
                            if ',' in date_text and len(date_text.split()) == 3:
                                # Remove day name
                                parts = date_text.split(',')
                                if len(parts) == 2:
                                    date_without_day = parts[1].strip()
                                    # Add current year
                                    from datetime import datetime as dt
                                    current_year = dt.now().year
                                    date_with_year = f"{date_without_day}, {current_year}"
                                    match_datetime = datetime.strptime(date_with_year, '%B %d, %Y')
                                    match_info['match_datetime'] = match_datetime
                                    print(f"  Match Date (parsed): {match_datetime.strftime('%B %d, %Y')}")
                        except Exception as e:
                            print(f"  Warning: Could not parse date text: {e}")
                            pass
            
            # Patch version
            patch_elem = soup.find('div', class_='match-header-date')
            if patch_elem:
                patch_text = patch_elem.text
                patch_match = re.search(r'Patch\s+([\d.]+)', patch_text)
                if patch_match:
                    match_info['patch_version'] = patch_match.group(1)
            
        except Exception as e:
            print(f"Error extracting match info: {e}")
        
        return match_info
    
    def _extract_teams(self, soup: BeautifulSoup) -> Dict:
        """Extract team information and final score"""
        teams = {'team1': {}, 'team2': {}}
        
        try:
            team_elements = soup.find_all('div', class_='match-header-link-name')
            if len(team_elements) >= 2:
                # Extract only the team name, not the entire element text which includes region
                for i, team_elem in enumerate(team_elements[:2]):
                    team_key = f'team{i+1}'
                    
                    # Method 1: Try to find a specific child element with the team name
                    team_name_elem = team_elem.find('div', class_='wf-title-med')
                    if team_name_elem:
                        teams[team_key]['name'] = team_name_elem.text.strip()
                    else:
                        # Method 2: Get only the first string (before any child elements)
                        # This avoids including region text that may be in child divs
                        first_text = None
                        for string in team_elem.stripped_strings:
                            first_text = string.strip()
                            break
                        
                        if first_text:
                            teams[team_key]['name'] = first_text
                        else:
                            # Fallback: Split by newline and take first part
                            # This handles cases where region is on a new line
                            full_text = team_elem.text.strip()
                            teams[team_key]['name'] = full_text.split('\n')[0].strip()
                    
                    # Get team URL
                    link_elem = team_elem.find_parent('a')
                    if link_elem:
                        teams[team_key]['url'] = 'https://www.vlr.gg' + link_elem.get('href', '')
            
            # Extract scores
            score_container = soup.find('div', class_='match-header-vs')
            if score_container:
                score_elements = score_container.find_all('div', class_='match-header-vs-score')
                if len(score_elements) >= 2:
                    try:
                        score1_text = score_elements[0].text.strip()
                        score2_text = score_elements[1].text.strip()
                        
                        teams['team1']['score'] = int(score1_text) if score1_text.isdigit() else 0
                        teams['team2']['score'] = int(score2_text) if score2_text.isdigit() else 0
                    except (ValueError, AttributeError):
                        teams['team1']['score'] = 0
                        teams['team2']['score'] = 0
            
        except Exception as e:
            print(f"Error extracting teams: {e}")
        
        return teams
    
    def _extract_maps(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract map data"""
        maps = []
        
        try:
            map_containers = soup.find_all('div', class_='vm-stats-game')
            
            valid_maps = []
            for container in map_containers:
                map_name_elem = container.find('div', class_='map')
                if map_name_elem and map_name_elem.find('span'):
                    valid_maps.append(container)
            
            
            for idx, map_container in enumerate(valid_maps, 1):
                map_data = {'map_number': idx}
                
                # Map name
                map_name_elem = map_container.find('div', class_='map')
                if map_name_elem:
                    map_name_span = map_name_elem.find('span')
                    if map_name_span:
                        raw_map_name = map_name_span.text.strip()
                        clean_name = ' '.join(raw_map_name.split())
                        clean_name = clean_name.replace('PICK', '').replace('DECIDER', '').strip()
                        map_data['map_name'] = clean_name
                    
                    pick_text = map_name_elem.text.strip()
                    if 'PICK' in pick_text:
                        map_data['pick_type'] = 'PICK'
                    elif 'DECIDER' in pick_text:
                        map_data['pick_type'] = 'DECIDER'
                    else:
                        map_data['pick_type'] = 'N/A'

                # Duration
                duration_elem = map_container.find('div', class_='map-duration')
                if duration_elem:
                    map_data['duration'] = duration_elem.text.strip()
                
                # Scores
                score_elements = map_container.find_all('div', class_='score')
                if len(score_elements) >= 2:
                    try:
                        map_data['team1_score'] = int(score_elements[0].text.strip())
                        map_data['team2_score'] = int(score_elements[1].text.strip())
                    except ValueError:
                        map_data['team1_score'] = 0
                        map_data['team2_score'] = 0
                
                # Half scores
                half_scores = map_container.find_all('span', class_='mod-both')
                if len(half_scores) >= 2:
                    try:
                        team1_halves = half_scores[0].text.strip().split('/')
                        if len(team1_halves) == 2:
                            map_data['team1_half1'] = int(team1_halves[0].strip())
                            map_data['team1_half2'] = int(team1_halves[1].strip())
                        
                        team2_halves = half_scores[1].text.strip().split('/')
                        if len(team2_halves) == 2:
                            map_data['team2_half1'] = int(team2_halves[0].strip())
                            map_data['team2_half2'] = int(team2_halves[1].strip())
                    except (ValueError, IndexError):
                        pass
                
                # Round results
                map_data['rounds'] = self._extract_round_results(map_container)
                
                maps.append(map_data)
                
        except Exception as e:
            print(f"Error extracting maps: {e}")
            import traceback
        
        return maps
    
    def _extract_round_results(self, map_container) -> List[Dict]:
        """Extract round-by-round results"""
        rounds = []
        try:
            vlr_rounds = map_container.find('div', class_='vlr-rounds')
            if vlr_rounds:
                round_elements = vlr_rounds.find_all('div', class_='rnd')
                for idx, round_elem in enumerate(round_elements, 1):
                    round_data = {'round_number': idx}
                    
                    round_classes = round_elem.get('class', [])
                    if 'mod-t' in round_classes:
                        round_data['winner'] = 'team1'
                    elif 'mod-ct' in round_classes:
                        round_data['winner'] = 'team2'
                    
                    win_type_span = round_elem.find('span', class_='rnd-sq')
                    if win_type_span:
                        style = win_type_span.get('style', '')
                        if 'elim' in style:
                            round_data['win_type'] = 'elimination'
                        elif 'defuse' in style:
                            round_data['win_type'] = 'defuse'
                        elif 'boom' in style:
                            round_data['win_type'] = 'boom'
                        elif 'time' in style:
                            round_data['win_type'] = 'time'
                    
                    rounds.append(round_data)
        except Exception as e:
            print(f"Error extracting round results: {e}")
        return rounds

    def _get_visible_stat_tables(self) -> List[WebElement]:
        """Helper to get visible stat tables after a tab switch."""
        try:
            all_tables = self.driver.find_elements(By.CSS_SELECTOR, 'table.wf-table-inset')
            visible_tables = [tbl for tbl in all_tables if tbl.is_displayed()]
            return visible_tables
        except Exception:
            return []

    def _extract_player_stats_all_maps(self, team1_name: str, team2_name: str, maps_data: List[Dict]) -> List[Dict]:
        """Extract player statistics for each individual map only."""
        all_player_stats = []
        
        try:
            map_names = [m['map_name'] for m in maps_data]
            
            try:
                nav_element = self.driver.find_element(By.CLASS_NAME, 'vm-stats-gamesnav')
                is_single_map = False
            except:
                is_single_map = True
                print("Single map match detected - no navigation tabs present")
            
            if is_single_map:
                if len(map_names) != 1:
                    print(f"WARNING: Expected 1 map for single-map match, found {len(map_names)}")
                
                map_name = map_names[0] if map_names else "Unknown"
                
                time.sleep(1)
                visible_tables = self._get_visible_stat_tables()
                
                if len(visible_tables) >= 2:
                    team1_html = visible_tables[0].get_attribute('outerHTML')
                    team2_html = visible_tables[1].get_attribute('outerHTML')

                    team1_stats = self._parse_stats_table_bs(team1_html, team1_name, map_name)
                    team2_stats = self._parse_stats_table_bs(team2_html, team2_name, map_name)
                    
                    all_player_stats.extend(team1_stats)
                    all_player_stats.extend(team2_stats)
                    
                else:
                    print(f"WARNING: Found {len(visible_tables)} tables (expected 2)")
            
            else:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'vm-stats-gamesnav'))
                )
                
                for map_idx in range(len(map_names)):
                    map_name = map_names[map_idx]
                    tab_index = map_idx + 1
                    
                    
                    try:
                        map_tabs = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.vm-stats-gamesnav-item'))
                        )
                        
                        if tab_index >= len(map_tabs):
                            print(f"ERROR: Tab index {tab_index} out of range (only {len(map_tabs)} tabs)")
                            continue
                        
                        current_tab = map_tabs[tab_index]
                        
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", current_tab)
                        time.sleep(0.3)
                        
                        try:
                            current_tab.click()
                        except Exception as e:
                            self.driver.execute_script("arguments[0].click();", current_tab)
                        
                        def active_tab_is_correct(driver):
                            try:
                                active_tab = driver.find_element(By.XPATH, f"//div[contains(@class, 'vm-stats-gamesnav-item') and contains(@class, 'mod-active') and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{map_name.lower()}')]")
                                return active_tab is not None
                            except:
                                return False

                        WebDriverWait(self.driver, 10).until(active_tab_is_correct)
                        
                        def visible_tables_ready(driver):
                            tables = self._get_visible_stat_tables()
                            return len(tables) >= 2

                        WebDriverWait(self.driver, 10).until(visible_tables_ready)
                        time.sleep(0.5)
                        
                        visible_tables = self._get_visible_stat_tables()
                        
                        print(f"Found {len(visible_tables)} visible stat tables on map {map_name}")
                        
                        if len(visible_tables) >= 2:
                            team1_html = visible_tables[0].get_attribute('outerHTML')
                            team2_html = visible_tables[1].get_attribute('outerHTML')

                            team1_stats = self._parse_stats_table_bs(team1_html, team1_name, map_name)
                            team2_stats = self._parse_stats_table_bs(team2_html, team2_name, map_name)
                            
                            all_player_stats.extend(team1_stats)
                            all_player_stats.extend(team2_stats)
                            
                        else:
                            print(f"WARNING: Found {len(visible_tables)} tables (expected 2)")
                            
                    except Exception as tab_error:
                        print(f"ERROR processing tab {tab_index}: {tab_error}")
                        import traceback
                        continue
                        
        except Exception as e:
            print(f"ERROR in player stats extraction: {e}")
            import traceback
        
        print(f"\n{'='*60}")
        print(f"Total player stat entries collected: {len(all_player_stats)}")
        expected_count = len(maps_data) * 10 if maps_data else 0
        print(f"Expected: {expected_count} (10 players × {len(maps_data)} maps)")
        print(f"{'='*60}")
        return all_player_stats

    def _parse_stats_table_bs(self, html_str: str, team_name: str, map_name: str) -> List[Dict]:
        """Parse stats table from HTML string via BeautifulSoup"""
        soup = BeautifulSoup(html_str, 'html.parser')
        table = soup.find('table')
        if not table:
            return []
        return self._parse_stats_table(table, team_name, map_name)

    def _parse_stats_table(self, table, team_name: str, map_name: str) -> List[Dict]:
        """Parse a single stats table for a team"""
        team_stats = []
        
        try:
            tbody = table.find('tbody')
            if not tbody:
                return team_stats
            
            rows = tbody.find_all('tr')
            
            for row in rows:
                player_stat = {
                    'team_name': team_name,
                    'map_name': map_name
                }
                
                # Player name and URL 
                player_cell = row.find('td', class_='mod-player')
                if player_cell:
                    player_link = player_cell.find('a')
                    if player_link:
                        player_name_div = player_link.find('div', class_='text-of')
                        if player_name_div:
                            player_stat['player_ign'] = player_name_div.text.strip()
                            player_stat['player_url'] = 'https://www.vlr.gg' + player_link.get('href', '')
                        else:
                            player_stat['player_ign'] = player_link.text.strip().split('\n')[0].strip()
                            player_stat['player_url'] = 'https://www.vlr.gg' + player_link.get('href', '')
                    else:
                        continue
                else:
                    continue
                
                # Agent
                agent_cell = row.find('td', class_='mod-agents')
                if agent_cell:
                    agent_img = agent_cell.find('img')
                    if agent_img:
                        player_stat['agent'] = agent_img.get('title', '').strip()
                
                # Rating
                rating_cell = row.find('td', class_='mod-stat')
                if rating_cell:
                    rating_text = rating_cell.text.strip().split('\n')[0].strip()
                    try:
                        player_stat['rating'] = float(rating_text)
                    except ValueError:
                        player_stat['rating'] = None
                
                # Get all stat cells
                stat_cells = row.find_all('td', class_='mod-stat')
               
                if len(stat_cells) >= 11:
                    try:
                        acs_text = stat_cells[1].text.strip().split('\n')[0].strip()
                        player_stat['acs'] = int(acs_text) if acs_text else None
                    except (ValueError, IndexError):
                        player_stat['acs'] = None
                    
                    try:
                        kills_text = stat_cells[2].text.strip().split('\n')[0].strip()
                        player_stat['kills'] = int(kills_text) if kills_text else None
                    except (ValueError, IndexError):
                        player_stat['kills'] = None
                    
                    try:
                        deaths_text = stat_cells[3].text.strip().split('\n')
                        player_stat['deaths'] = None
                        for line in deaths_text:
                            line = line.strip()
                            if line and line != '/':
                                player_stat['deaths'] = int(line)
                                break
                    except (ValueError, IndexError):
                        player_stat['deaths'] = None
                    
                    try:
                        assists_text = stat_cells[4].text.strip().split('\n')[0].strip()
                        player_stat['assists'] = int(assists_text) if assists_text else None
                    except (ValueError, IndexError):
                        player_stat['assists'] = None
                    
                    try:
                        plus_minus_text = stat_cells[5].text.strip().split('\n')[0].strip().replace('+', '')
                        player_stat['plus_minus'] = int(plus_minus_text) if plus_minus_text else None
                    except (ValueError, IndexError):
                        player_stat['plus_minus'] = None
                    
                    try:
                        kast_text = stat_cells[6].text.strip().split('\n')[0].strip().replace('%', '')
                        player_stat['kast_percent'] = float(kast_text) if kast_text else None
                    except (ValueError, IndexError):
                        player_stat['kast_percent'] = None
                    
                    try:
                        adr_text = stat_cells[7].text.strip().split('\n')[0].strip()
                        player_stat['adr'] = float(adr_text) if adr_text else None
                    except (ValueError, IndexError):
                        player_stat['adr'] = None
                    
                    try:
                        hs_text = stat_cells[8].text.strip().split('\n')[0].strip().replace('%', '')
                        player_stat['hs_percent'] = float(hs_text) if hs_text else None
                    except (ValueError, IndexError):
                        player_stat['hs_percent'] = None
                    
                    try:
                        fk_text = stat_cells[9].text.strip().split('\n')[0].strip()
                        player_stat['first_kills'] = int(fk_text) if fk_text else None
                    except (ValueError, IndexError):
                        player_stat['first_kills'] = None
                    
                    try:
                        fd_text = stat_cells[10].text.strip().split('\n')[0].strip()
                        player_stat['first_deaths'] = int(fd_text) if fd_text else None
                    except (ValueError, IndexError):
                        player_stat['first_deaths'] = None
                
                team_stats.append(player_stat)
                
        except Exception as e:
            print(f"Error parsing stats table: {e}")
        
        return team_stats


def aggregate_player_stats(player_stats: List[Dict]) -> List[Dict]:
    """Aggregate individual map stats into overall player statistics."""
    from collections import defaultdict
    
    player_map_stats = defaultdict(list)
    
    for stat in player_stats:
        key = (stat['team_name'], stat['player_ign'])
        player_map_stats[key].append(stat)
    
    aggregated_stats = []
    
    for (team_name, player_ign), maps in player_map_stats.items():
        if not maps:
            continue
        
        agg = {
            'team_name': team_name,
            'player_ign': player_ign,
            'map_name': 'Overall',
            'player_url': maps[0].get('player_url', ''),
        }
        
        # Copy region and join date from any map entry
        if 'player_region' in maps[0]:
            agg['player_region'] = maps[0]['player_region']
        if 'team_join_date' in maps[0]:
            agg['team_join_date'] = maps[0]['team_join_date']
        
        total_kills = sum(m.get('kills', 0) for m in maps if m.get('kills') is not None)
        total_deaths = sum(m.get('deaths', 0) for m in maps if m.get('deaths') is not None)
        total_assists = sum(m.get('assists', 0) for m in maps if m.get('assists') is not None)
        total_fk = sum(m.get('first_kills', 0) for m in maps if m.get('first_kills') is not None)
        total_fd = sum(m.get('first_deaths', 0) for m in maps if m.get('first_deaths') is not None)
        
        agg['kills'] = total_kills
        agg['deaths'] = total_deaths
        agg['assists'] = total_assists
        agg['first_kills'] = total_fk
        agg['first_deaths'] = total_fd
        
        valid_ratings = [m.get('rating') for m in maps if m.get('rating') is not None]
        valid_acs = [m.get('acs') for m in maps if m.get('acs') is not None]
        valid_kast = [m.get('kast_percent') for m in maps if m.get('kast_percent') is not None]
        valid_adr = [m.get('adr') for m in maps if m.get('adr') is not None]
        valid_hs = [m.get('hs_percent') for m in maps if m.get('hs_percent') is not None]
        
        agg['rating'] = round(sum(valid_ratings) / len(valid_ratings), 2) if valid_ratings else None
        agg['acs'] = round(sum(valid_acs) / len(valid_acs)) if valid_acs else None
        agg['kast_percent'] = round(sum(valid_kast) / len(valid_kast), 1) if valid_kast else None
        agg['adr'] = round(sum(valid_adr) / len(valid_adr), 1) if valid_adr else None
        agg['hs_percent'] = round(sum(valid_hs) / len(valid_hs), 1) if valid_hs else None
        
        agg['plus_minus'] = sum(m.get('plus_minus', 0) for m in maps if m.get('plus_minus') is not None)
        
        agents = [m.get('agent') for m in maps if m.get('agent')]
        if agents:
            agg['agent'] = agents[0]
        
        aggregated_stats.append(agg)
    
    return aggregated_stats

if __name__ == "__main__":
    pass