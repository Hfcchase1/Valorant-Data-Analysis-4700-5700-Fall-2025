"""
VLR Scraper using FastAPI Frontend
This version uses your partner's DB_frontEnd_API.py
"""
import requests
import pyodbc
from vlr_scraper import VLRScraper
from datetime import datetime
from typing import Dict, List, Optional
import sys
import time


class APIInserter:
    """Handles database operations through the FastAPI endpoint"""
    
    def __init__(self, api_url="http://localhost:8000", 
                 server="localhost", database="vlr_matches",
                 use_windows_auth=True, user="sa", password=""):
        """
        Initialize both API connection and direct database connection
        API generates SQL, we execute it directly
        """
        self.api_url = api_url
        
        # Test API connection
        try:
            response = requests.get(f"{api_url}/docs", timeout=2)
            print(f"✓ API server is running at {api_url}")
        except:
            print(f"⚠️  Warning: API server not reachable at {api_url}")
            print(f"   Start it with: uvicorn DB_frontEnd_API:app --reload")
            print(f"   Continuing with direct SQL execution...")
        
        # Setup database connection for executing SQL
        try:
            if use_windows_auth:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"Trusted_Connection=yes;"
                    f"TrustServerCertificate=yes;"
                    f"Encrypt=no;"
                )
            else:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"UID={user};"
                    f"PWD={password};"
                    f"TrustServerCertificate=yes;"
                    f"Encrypt=no;"
                )
            
            self.conn = pyodbc.connect(conn_str)
            self.cursor = self.conn.cursor()
            print(f"✓ Connected to SQL Server: {database}")
            
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            raise
        
        # Cache for entity IDs
        self.cache = {
            'maps': {},
            'teams': {},
            'agents': {},
            'tournaments': {},
            'players': {}
        }
    
    def generate_sql(self, query: str) -> str:
        """
        Generate SQL using the API
        Falls back to manual generation if API is down
        """
        try:
            response = requests.post(
                f"{self.api_url}/generate-sql",
                json={"query": query},
                timeout=5
            )
            response.raise_for_status()
            sql = response.json().get('sql')
            return sql
        except Exception as e:
            # Fallback: parse the query ourselves
            return self._parse_query_manual(query)
    
    def _parse_query_manual(self, query: str) -> str:
        """Manually parse the query format if API is down"""
        parts = query.strip().split('|')
        if len(parts) != 3:
            raise ValueError(f"Invalid query format: {query}")
        
        action, table, kv_string = parts
        action = action.upper()
        
        # Parse key=value pairs
        kv = {}
        for pair in kv_string.split(','):
            if '=' not in pair:
                continue
            key, value = pair.split('=', 1)
            kv[key.strip()] = value.strip()
        
        # Generate SQL
        if action == "INSERT":
            columns = ', '.join(kv.keys())
            values = ', '.join(f"'{v}'" if not v.replace('.', '').isdigit() else v for v in kv.values())
            return f"INSERT INTO {table} ({columns}) VALUES ({values})"
        elif action == "UPDATE":
            if 'id' not in kv:
                raise ValueError("UPDATE requires 'id' field")
            id_val = kv.pop('id')
            set_clause = ', '.join(f"{k}='{v}'" if not v.replace('.', '').isdigit() else f"{k}={v}" for k, v in kv.items())
            return f"UPDATE {table} SET {set_clause} WHERE id={id_val}"
        elif action == "DELETE":
            if 'id' not in kv:
                raise ValueError("DELETE requires 'id' field")
            return f"DELETE FROM {table} WHERE id={kv['id']}"
        else:
            raise ValueError(f"Unsupported action: {action}")
    
    def execute_sql(self, query: str) -> Optional[int]:
        """Execute a query string (API format) and return inserted ID if applicable"""
        try:
            # Generate SQL using API
            sql = self.generate_sql(query)
            
            # Execute SQL
            self.cursor.execute(sql)
            self.conn.commit()
            
            # Get inserted ID if it was an INSERT
            if sql.strip().upper().startswith("INSERT"):
                self.cursor.execute("SELECT @@IDENTITY")
                result = self.cursor.fetchone()
                if result:
                    return int(result[0])
            
            return None
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error executing query '{query}': {e}")
            raise
    
    def insert_or_get_map(self, map_name: str) -> int:
        """Insert map if not exists, return map_id"""
        if not map_name:
            map_name = "Unknown"
        
        if map_name in self.cache['maps']:
            return self.cache['maps'][map_name]
        
        # Check if exists
        self.cursor.execute("SELECT map_id FROM Maps WHERE name = ?", (map_name,))
        result = self.cursor.fetchone()
        if result:
            map_id = result[0]
            self.cache['maps'][map_name] = map_id
            return map_id
        
        # Insert using API format
        query = f"INSERT|Maps|name={map_name}"
        map_id = self.execute_sql(query)
        self.cache['maps'][map_name] = map_id
        return map_id
    
    def insert_or_get_team(self, team_name: str, region: str = "Unknown") -> int:
        """Insert team if not exists, return team_id"""
        if not team_name:
            team_name = "Unknown Team"
        
        if team_name in self.cache['teams']:
            return self.cache['teams'][team_name]
        
        # Check if exists
        self.cursor.execute("SELECT team_id FROM Teams WHERE name = ?", (team_name,))
        result = self.cursor.fetchone()
        if result:
            team_id = result[0]
            self.cache['teams'][team_name] = team_id
            return team_id
        
        # Insert using API format
        query = f"INSERT|Teams|name={team_name},region={region}"
        team_id = self.execute_sql(query)
        self.cache['teams'][team_name] = team_id
        return team_id
    
    def insert_or_get_agent(self, agent_name: str, role: str = "Unknown") -> int:
        """Insert agent if not exists, return agent_id"""
        if not agent_name:
            agent_name = "Unknown"
        
        if agent_name in self.cache['agents']:
            return self.cache['agents'][agent_name]
        
        # Check if exists
        self.cursor.execute("SELECT agent_id FROM Agents WHERE name = ?", (agent_name,))
        result = self.cursor.fetchone()
        if result:
            agent_id = result[0]
            self.cache['agents'][agent_name] = agent_id
            return agent_id
        
        # Insert using API format
        query = f"INSERT|Agents|name={agent_name},role={role}"
        agent_id = self.execute_sql(query)
        self.cache['agents'][agent_name] = agent_id
        return agent_id
    
    def insert_or_get_tournament(self, tournament_name: str) -> int:
        """Insert tournament if not exists, return tournament_id"""
        if not tournament_name:
            tournament_name = "Unknown Tournament"
        
        if tournament_name in self.cache['tournaments']:
            return self.cache['tournaments'][tournament_name]
        
        # Check if exists
        self.cursor.execute("SELECT tournament_id FROM Tournaments WHERE name = ?", (tournament_name,))
        result = self.cursor.fetchone()
        if result:
            tournament_id = result[0]
            self.cache['tournaments'][tournament_name] = tournament_id
            return tournament_id
        
        # Insert using API format
        query = f"INSERT|Tournaments|name={tournament_name}"
        tournament_id = self.execute_sql(query)
        self.cache['tournaments'][tournament_name] = tournament_id
        return tournament_id
    
    def insert_or_get_player(self, player_ign: str) -> int:
        """Insert player if not exists, return player_id"""
        if not player_ign:
            player_ign = "Unknown Player"
        
        if player_ign in self.cache['players']:
            return self.cache['players'][player_ign]
        
        # Check if exists
        self.cursor.execute("SELECT player_id FROM Players WHERE username = ?", (player_ign,))
        result = self.cursor.fetchone()
        if result:
            player_id = result[0]
            self.cache['players'][player_ign] = player_id
            return player_id
        
        # Insert using API format
        email = f"{player_ign.lower().replace(' ', '_')}@vlr.gg"
        join_date = datetime.now().strftime('%Y-%m-%d')
        query = f"INSERT|Players|username={player_ign},email={email},region=Unknown,join_date={join_date}"
        player_id = self.execute_sql(query)
        self.cache['players'][player_ign] = player_id
        return player_id
    
    def insert_match_data(self, match_data: Dict, skip_if_exists: bool = True):
        """Insert complete match data using API-generated SQL"""
        try:
            # Get foreign keys
            team1_name = match_data.get('teams', {}).get('team1', {}).get('name', 'Team 1')
            team2_name = match_data.get('teams', {}).get('team2', {}).get('name', 'Team 2')
            team1_id = self.insert_or_get_team(team1_name)
            team2_id = self.insert_or_get_team(team2_name)
            
            tournament_name = match_data.get('match_info', {}).get('tournament_name', 'Unknown')
            tournament_id = self.insert_or_get_tournament(tournament_name)
            
            # Parse date
            match_date_str = match_data.get('match_info', {}).get('match_date')
            try:
                match_date = datetime.strptime(match_date_str, '%B %d, %Y').strftime('%Y-%m-%d')
            except:
                match_date = datetime.now().strftime('%Y-%m-%d')
            
            # Insert match using API format
            query = f"INSERT|Matches|match_date={match_date},tournament_id={tournament_id},mode=Competitive,date_played={match_date}"
            match_id = self.execute_sql(query)
            print(f"  ✓ Match ID: {match_id}")
            
            # Insert maps
            maps_data = match_data.get('maps', [])
            match_map_ids = {}
            
            for map_data in maps_data:
                map_name = map_data.get('map_name', 'Unknown')
                map_id = self.insert_or_get_map(map_name)
                map_number = map_data.get('map_number', 1)
                
                # Convert duration
                duration_str = map_data.get('duration', '0')
                duration_sec = 0
                if ':' in str(duration_str):
                    try:
                        parts = duration_str.split(':')
                        duration_sec = int(parts[0]) * 60 + int(parts[1])
                    except:
                        pass
                
                # Insert map using API format
                query = (f"INSERT|MatchMaps|match_id={match_id},map_id={map_id},"
                        f"map_order={map_number},team1_score={map_data.get('team1_score', 0)},"
                        f"team2_score={map_data.get('team2_score', 0)},duration={duration_sec}")
                match_map_id = self.execute_sql(query)
                match_map_ids[map_number] = match_map_id
                
                # Insert rounds
                for round_data in map_data.get('rounds', []):
                    query = (f"INSERT|MatchRounds|match_map_id={match_map_id},"
                            f"round_number={round_data.get('round_number')},"
                            f"winner={round_data.get('winner', 'team1')}")
                    self.execute_sql(query)
                
                # Insert team stats
                query = (f"INSERT|MatchStats|match_map_id={match_map_id},team_id={team1_id},"
                        f"rounds_won={map_data.get('team1_score', 0)},rounds_lost={map_data.get('team2_score', 0)}")
                self.execute_sql(query)
                
                query = (f"INSERT|MatchStats|match_map_id={match_map_id},team_id={team2_id},"
                        f"rounds_won={map_data.get('team2_score', 0)},rounds_lost={map_data.get('team1_score', 0)}")
                self.execute_sql(query)
            
            print(f"  ✓ Inserted {len(maps_data)} maps and rounds")
            
            # Insert player stats
            player_stats = match_data.get('player_stats', [])
            for stat in player_stats:
                if stat.get('map_name') == 'Overall':
                    continue
                
                player_ign = stat.get('player_ign')
                if not player_ign:
                    continue
                
                player_id = self.insert_or_get_player(player_ign)
                agent_id = self.insert_or_get_agent(stat.get('agent', 'Unknown'))
                
                # Find match_map_id
                map_name = stat.get('map_name')
                match_map_id = None
                for m in maps_data:
                    if m['map_name'] == map_name:
                        match_map_id = match_map_ids.get(m['map_number'])
                        break
                
                if not match_map_id:
                    continue
                
                # Insert player match stats
                query = (f"INSERT|PlayerMatches|player_id={player_id},match_id={match_id},"
                        f"match_map_id={match_map_id},agent_id={agent_id},"
                        f"kills={stat.get('kills', 0)},deaths={stat.get('deaths', 0)},"
                        f"assists={stat.get('assists', 0)},score={stat.get('acs', 0)}")
                self.execute_sql(query)
                
                # Insert advanced stats
                query = (f"INSERT|AdvancedStats|match_id={match_id},match_map_id={match_map_id},"
                        f"player_id={player_id},headshots=0,economy_rating=0,utility_used=0,"
                        f"acs={stat.get('acs', 0)},adr={stat.get('adr', 0)},"
                        f"kast={stat.get('kast_percent', 0)},hs_percent={stat.get('hs_percent', 0)},"
                        f"first_kills={stat.get('first_kills', 0)},first_deaths={stat.get('first_deaths', 0)},"
                        f"r2o={stat.get('rating', 0)}")
                self.execute_sql(query)
            
            print(f"  ✓ Inserted player statistics")
            
        except Exception as e:
            self.conn.rollback()
            print(f"✗ Error inserting match: {e}")
            raise
    
    def close(self):
        """Close connections"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Database connection closed")


def main():
    """Main scraper using API"""
    if len(sys.argv) != 3:
        print("\nUsage: python api_integration.py START_PAGE END_PAGE")
        print("Example: python api_integration.py 1 1")
        print("\nNOTE: Make sure FastAPI is running:")
        print("  uvicorn DB_frontEnd_API:app --reload")
        sys.exit(1)
    
    start_page = int(sys.argv[1])
    end_page = int(sys.argv[2])
    
    print("="*70)
    print(f"VLR SCRAPER (Using API) - Pages {start_page}-{end_page}")
    print("="*70)
    
    # Connect
    db = APIInserter(
        api_url="http://localhost:8000",
        server="localhost",
        database="vlr_matches",
        use_windows_auth=True
    )
    
    try:
        # Get match URLs
        print("\n📡 Discovering matches...")
        all_urls = []
        for page in range(start_page, end_page + 1):
            links = VLRScraper.get_match_links_by_page_static(page)
            all_urls.extend(links)
        
        unique_urls = list(set(all_urls))
        print(f"✅ Found {len(unique_urls)} matches\n")
        
        # Scrape and insert
        for i, url in enumerate(unique_urls, 1):
            print(f"[{i}/{len(unique_urls)}] {url}")
            
            try:
                with VLRScraper(headless=True) as scraper:
                    match_data = scraper.scrape_match(url)
                
                teams = match_data.get('teams', {})
                print(f"  📊 {teams.get('team1', {}).get('name')} vs {teams.get('team2', {}).get('name')}")
                
                db.insert_match_data(match_data)
                print(f"  ✅ Success!\n")
                time.sleep(2)
                
            except Exception as e:
                print(f"  ❌ Error: {e}\n")
                time.sleep(5)
        
        print("="*70)
        print("✅ COMPLETE")
        print("="*70)
        
    finally:
        db.close()


if __name__ == "__main__":
    main()