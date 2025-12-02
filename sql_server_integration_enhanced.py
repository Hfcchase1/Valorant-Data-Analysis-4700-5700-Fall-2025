"""
SQL Server Integration for VLR Match Data 
"""
import pyodbc
from datetime import datetime
from typing import Dict, List, Optional
from vlr_constants import AGENT_DATA, MAP_DATA, get_agent_id, get_agent_role, get_map_id


class SQLServerInserter:
    """Handles SQL Server insertions with enhanced data"""
    
    def __init__(self, server="localhost\\SQLEXPRESS", database="vlr_matches", 
                 use_windows_auth=True, user="sa", password=""):
        """Initialize database connection"""
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
            
            # Test connection
            self.cursor.execute("SELECT DB_NAME()")
            db_name = self.cursor.fetchone()[0]
            
            # Initialize hardcoded data
            self._initialize_agents()
            self._initialize_maps()
            
        except Exception as e:
            pass
            raise
    
    def _initialize_agents(self):
        """Insert all hardcoded agents into the database"""
        try:
            for agent_name, agent_info in AGENT_DATA.items():
                agent_id = agent_info['agent_id']
                role = agent_info['role']
                
                # Check if exists
                self.cursor.execute("SELECT agent_id FROM Agents WHERE agent_id = ?", (agent_id,))
                if not self.cursor.fetchone():
                    # Use SET IDENTITY_INSERT to specify exact IDs
                    self.cursor.execute("SET IDENTITY_INSERT Agents ON")
                    self.cursor.execute(
                        "INSERT INTO Agents (agent_id, name, role) VALUES (?, ?, ?)",
                        (agent_id, agent_name, role)
                    )
                    self.cursor.execute("SET IDENTITY_INSERT Agents OFF")
                    self.conn.commit()
            
        except Exception as e:
            print(f"Warning: Could not initialize agents: {e}")
            self.conn.rollback()
    
    def _initialize_maps(self):
        """Insert all hardcoded maps into the database"""
        try:
            for map_name, map_id in MAP_DATA.items():
                # Check if exists
                self.cursor.execute("SELECT map_id FROM Maps WHERE map_id = ?", (map_id,))
                if not self.cursor.fetchone():
                    # Use SET IDENTITY_INSERT to specify exact IDs
                    self.cursor.execute("SET IDENTITY_INSERT Maps ON")
                    self.cursor.execute(
                        "INSERT INTO Maps (map_id, name) VALUES (?, ?)",
                        (map_id, map_name)
                    )
                    self.cursor.execute("SET IDENTITY_INSERT Maps OFF")
                    self.conn.commit()
            
        except Exception as e:
            print(f"Warning: Could not initialize maps: {e}")
            self.conn.rollback()
    
    def insert_tournament(self, tournament_name: str = None, prize_pool: int = None, 
                         start_date = None, end_date = None) -> int:
        """Insert tournament with full details"""
        try:
            pass
            
            if not tournament_name or tournament_name.strip() == "":
                pass
                tournament_name = "Unknown Tournament"
            
            # Check if exists
            self.cursor.execute("SELECT tournament_id FROM Tournaments WHERE name = ?", (tournament_name,))
            result = self.cursor.fetchone()
            if result:
                tournament_id = result[0]
                # Update if we have new information
                if prize_pool or start_date or end_date:
                    self.cursor.execute(
                        """UPDATE Tournaments 
                           SET prize_pool = COALESCE(?, prize_pool),
                               start_date = COALESCE(?, start_date),
                               end_date = COALESCE(?, end_date)
                           WHERE tournament_id = ?""",
                        (prize_pool, start_date, end_date, tournament_id)
                    )
                    self.conn.commit()
                return tournament_id
            
            # Insert new
            self.cursor.execute(
                "INSERT INTO Tournaments (name, prize_pool, start_date, end_date) VALUES (?, ?, ?, ?)", 
                (tournament_name, prize_pool, start_date, end_date)
            )
            self.conn.commit()
            self.cursor.execute("SELECT @@IDENTITY")
            return int(self.cursor.fetchone()[0])
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting tournament {tournament_name}: {e}")
            raise
    
    def insert_tournament_team(self, tournament_id: int, team_id: int):
        """Link a team to a tournament"""
        try:
            # Check if already exists
            self.cursor.execute(
                "SELECT * FROM TournamentTeams WHERE tournament_id = ? AND team_id = ?",
                (tournament_id, team_id)
            )
            if not self.cursor.fetchone():
                self.cursor.execute(
                    "INSERT INTO TournamentTeams (tournament_id, team_id) VALUES (?, ?)",
                    (tournament_id, team_id)
                )
                self.conn.commit()
        except Exception as e:
            print(f"Warning: Could not insert tournament team link: {e}")
            self.conn.rollback()
    
    def insert_team(self, team_name: str, region: str = None, logo_url: str = None) -> int:
        """Insert team with region and logo"""
        try:
            if not team_name:
                team_name = "Unknown Team"
            
            # Check if exists
            self.cursor.execute("SELECT team_id, region, logo_url FROM Teams WHERE name = ?", (team_name,))
            result = self.cursor.fetchone()
            if result:
                team_id = result[0]
                existing_region = result[1]
                existing_logo = result[2]
                
                
                # Update if we have new information and existing is NULL/empty
                needs_update = False
                if region and (not existing_region or existing_region.strip() == ''):
                    needs_update = True
                if logo_url and (not existing_logo or existing_logo.strip() == ''):
                    needs_update = True
                
                if needs_update:
                    # Build dynamic update query
                    update_parts = []
                    params = []
                    
                    if region and (not existing_region or existing_region.strip() == ''):
                        update_parts.append("region = ?")
                        params.append(region)
                    
                    if logo_url and (not existing_logo or existing_logo.strip() == ''):
                        update_parts.append("logo_url = ?")
                        params.append(logo_url)
                    
                    update_parts.append("updated_at = GETDATE()")
                    params.append(team_id)
                    
                    update_query = f"UPDATE Teams SET {', '.join(update_parts)} WHERE team_id = ?"
                    
                    self.cursor.execute(update_query, params)
                    self.conn.commit()
                    
                    # Verify the update
                    self.cursor.execute("SELECT region FROM Teams WHERE team_id = ?", (team_id,))
                    new_region = self.cursor.fetchone()[0]
                    
                    if region:
                        pass
                else:
                    pass
                
                return team_id
            
            # Insert new
            self.cursor.execute(
                "INSERT INTO Teams (name, region, logo_url) VALUES (?, ?, ?)", 
                (team_name, region, logo_url)
            )
            self.conn.commit()
            self.cursor.execute("SELECT @@IDENTITY")
            team_id = int(self.cursor.fetchone()[0])
            
            # Verify the insert
            self.cursor.execute("SELECT region FROM Teams WHERE team_id = ?", (team_id,))
            inserted_region = self.cursor.fetchone()[0]
            
            if region:
                pass
            
            return team_id
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting team {team_name}: {e}")
            import traceback
            raise
    
    def insert_player(self, player_ign: str, email: str = None, region: str = None, 
                     team_id: int = None, join_date = None) -> int:
        """Insert player with region and team join date"""
        try:
            if not player_ign:
                player_ign = "Unknown Player"
            
            # Check if exists by username
            self.cursor.execute("SELECT player_id, region FROM Players WHERE username = ?", (player_ign,))
            result = self.cursor.fetchone()
            if result:
                player_id = result[0]
                existing_region = result[1]
                
                # Update if we have new region info and existing is NULL or 'Unknown'
                if region and (not existing_region or existing_region == 'Unknown' or existing_region.strip() == ''):
                    self.cursor.execute(
                        "UPDATE Players SET region = ? WHERE player_id = ?",
                        (region, player_id)
                    )
                    self.conn.commit()
                    print(f"    Updated player region: {player_ign} -> {region}")
                
                # Link to team with join date if provided
                if team_id and join_date:
                    self._link_player_to_team(player_id, team_id, join_date)
                
                return player_id
            
            # Create email if not provided
            if not email:
                email = f"{player_ign.lower().replace(' ', '_')}@vlr.gg"
            
            # Use provided region or default
            if not region:
                region = "Unknown"
            
            # Insert new
            self.cursor.execute(
                "INSERT INTO Players (username, email, region, join_date) VALUES (?, ?, ?, ?)",
                (player_ign, email, region, join_date or datetime.now().date())
            )
            self.conn.commit()
            self.cursor.execute("SELECT @@IDENTITY")
            player_id = int(self.cursor.fetchone()[0])
            
            if region and region != "Unknown":
                print(f"    Inserted player with region: {player_ign} -> {region}")
            
            # Link to team if provided
            if team_id and join_date:
                self._link_player_to_team(player_id, team_id, join_date)
            
            return player_id
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting player {player_ign}: {e}")
            raise
    
    def _link_player_to_team(self, player_id: int, team_id: int, join_date):
        """Link player to team with join date"""
        try:
            # Check if already exists
            self.cursor.execute(
                "SELECT * FROM TeamPlayers WHERE team_id = ? AND player_id = ?",
                (team_id, player_id)
            )
            if not self.cursor.fetchone():
                self.cursor.execute(
                    "INSERT INTO TeamPlayers (team_id, player_id, join_date) VALUES (?, ?, ?)",
                    (team_id, player_id, join_date)
                )
                self.conn.commit()
        except Exception as e:
            print(f"Warning: Could not link player to team: {e}")
            self.conn.rollback()
    
    def update_team_stats(self, team_id: int, won: bool):
        """Update team win/loss statistics"""
        try:
            # Check if team stats exist
            self.cursor.execute("SELECT id FROM TeamStats WHERE team_id = ?", (team_id,))
            result = self.cursor.fetchone()
            
            if result:
                # Update existing
                if won:
                    self.cursor.execute(
                        """UPDATE TeamStats 
                           SET matches_played = matches_played + 1,
                               matches_won = matches_won + 1
                           WHERE team_id = ?""",
                        (team_id,)
                    )
                else:
                    self.cursor.execute(
                        """UPDATE TeamStats 
                           SET matches_played = matches_played + 1,
                               matches_lost = matches_lost + 1
                           WHERE team_id = ?""",
                        (team_id,)
                    )
            else:
                # Create new
                if won:
                    self.cursor.execute(
                        "INSERT INTO TeamStats (team_id, matches_played, matches_won, matches_lost) VALUES (?, 1, 1, 0)",
                        (team_id,)
                    )
                else:
                    self.cursor.execute(
                        "INSERT INTO TeamStats (team_id, matches_played, matches_won, matches_lost) VALUES (?, 1, 0, 1)",
                        (team_id,)
                    )
            
            self.conn.commit()
        except Exception as e:
            print(f"Warning: Could not update team stats: {e}")
            self.conn.rollback()
    
    def check_match_exists(self, team1_id: int, team2_id: int, match_date) -> Optional[int]:
        """Check if match exists and return match_id"""
        try:
            if team1_id and team2_id and match_date:
                self.cursor.execute(
                    """SELECT TOP 1 m.match_id 
                       FROM Matches m
                       JOIN MatchMaps mm ON m.match_id = mm.match_id
                       JOIN MatchStats ms ON mm.match_map_id = ms.match_map_id
                       WHERE CAST(m.match_date AS DATE) = CAST(? AS DATE)
                       AND ms.team_id IN (?, ?)
                       GROUP BY m.match_id
                       HAVING COUNT(DISTINCT ms.team_id) = 2""",
                    (match_date, team1_id, team2_id)
                )
                result = self.cursor.fetchone()
                if result:
                    return result[0]
            return None
        except Exception as e:
            print(f"Error checking for existing match: {e}")
            return None
    
    def delete_match_data(self, match_id: int):
        """Delete all data associated with a match"""
        try:
            print(f"  Deleting existing data for Match ID: {match_id}...")
            
            self.cursor.execute("DELETE FROM AdvancedStats WHERE match_id = ?", (match_id,))
            self.cursor.execute("DELETE FROM PlayerMatches WHERE match_id = ?", (match_id,))
            self.cursor.execute(
                "DELETE FROM MatchRounds WHERE match_map_id IN (SELECT match_map_id FROM MatchMaps WHERE match_id = ?)", 
                (match_id,)
            )
            self.cursor.execute(
                "DELETE FROM MatchStats WHERE match_map_id IN (SELECT match_map_id FROM MatchMaps WHERE match_id = ?)", 
                (match_id,)
            )
            self.cursor.execute("DELETE FROM MatchMaps WHERE match_id = ?", (match_id,))
            self.cursor.execute("DELETE FROM Matches WHERE match_id = ?", (match_id,))
            
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise
    
    def insert_match_data(self, match_data: Dict, skip_if_exists: bool = True):
        """
        Insert all match data into SQL Server with enhanced data
        
        Args:
            match_data: Dictionary containing match data from scraper
            skip_if_exists: If True, skip insertion if match already exists
        """
        try:
            # --- 1. Get/Insert Foreign Keys ---
            teams_info = match_data.get('teams', {})
            team1_data = teams_info.get('team1', {})
            team2_data = teams_info.get('team2', {})
            
            team1_name = team1_data.get('name', 'Team 1')
            team2_name = team2_data.get('name', 'Team 2')
            
            # Insert teams with region and logo
            team1_id = self.insert_team(
                team1_name,
                region=team1_data.get('region'),
                logo_url=team1_data.get('logo_url')
            )
            team2_id = self.insert_team(
                team2_name,
                region=team2_data.get('region'),
                logo_url=team2_data.get('logo_url')
            )
            
            # Insert tournament with full details
            match_info = match_data.get('match_info', {})
            
            tournament_name = match_info.get('tournament_name', 'Unknown Tournament')
            
            # If tournament_name is None or empty, it will use the default
            if not tournament_name:
                print(f"  WARNING: tournament_name is None/empty, will default to 'Unknown Tournament'")
            
            tournament_id = self.insert_tournament(
                tournament_name,
                prize_pool=match_info.get('tournament_prize_pool'),
                start_date=match_info.get('tournament_start_date'),
                end_date=match_info.get('tournament_end_date')
            )
            
            # Link teams to tournament
            self.insert_tournament_team(tournament_id, team1_id)
            self.insert_tournament_team(tournament_id, team2_id)
            
            # Link tournament teams if available
            for team_name in match_info.get('tournament_teams', []):
                if team_name and team_name not in [team1_name, team2_name]:
                    try:
                        other_team_id = self.insert_team(team_name)
                        self.insert_tournament_team(tournament_id, other_team_id)
                    except:
                        pass  # Skip if there's an issue with a team
            
            # Parse match date - use the actual match datetime if available
            match_datetime = match_info.get('match_datetime')
            if not match_datetime:
                match_date_str = match_info.get('match_date')
                if match_date_str:
                    try:
                        match_datetime = datetime.strptime(match_date_str, '%B %d, %Y')
                    except (ValueError, TypeError) as e:
                        print(f"  Warning: Could not parse date '{match_date_str}': {e}")
                        match_datetime = datetime.now()
                else:
                    match_datetime = datetime.now()
            
            # Print what we're about to insert
            print(f"  Inserting match with date: {match_datetime}")
            
            # Check if match exists
            existing_match_id = self.check_match_exists(team1_id, team2_id, match_datetime)
            
            if existing_match_id:
                if skip_if_exists:
                    print(f"  ⭐️ Match already exists (ID: {existing_match_id}) - SKIPPING")
                    return
                else:
                    print(f"  🔄 Match exists (ID: {existing_match_id}) - REPLACING")
                    self.delete_match_data(existing_match_id)
            
            # --- 2. Insert Match Record ---
            self.cursor.execute(
                """INSERT INTO Matches (match_date, tournament_id, mode, date_played)
                   VALUES (?, ?, ?, ?)""",
                (match_datetime, tournament_id, 'Competitive', match_datetime)
            )
            self.conn.commit()
            
            self.cursor.execute("SELECT @@IDENTITY")
            match_id = int(self.cursor.fetchone()[0])
            
            # --- 3. Insert Maps and Rounds ---
            maps_data = match_data.get('maps', [])
            match_map_ids = {}
            
            for map_data in maps_data:
                map_name = map_data.get('map_name', 'Unknown')
                
                # Use hardcoded map ID
                map_id = get_map_id(map_name)
                if not map_id:
                    print(f"  Warning: Unknown map '{map_name}', skipping")
                    continue
                
                map_number = map_data.get('map_number', 1)
                
                # Convert duration to seconds
                duration_str = map_data.get('duration', '0')
                duration_seconds = 0
                if duration_str and ':' in str(duration_str):
                    try:
                        parts = duration_str.split(':')
                        duration_seconds = int(parts[0]) * 60 + int(parts[1])
                    except:
                        duration_seconds = 0
                
                # Insert MatchMap
                self.cursor.execute(
                    """INSERT INTO MatchMaps (match_id, map_id, map_order, team1_score, team2_score, duration)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (match_id, map_id, map_number, 
                     map_data.get('team1_score', 0),
                     map_data.get('team2_score', 0),
                     duration_seconds)
                )
                self.conn.commit()
                
                self.cursor.execute("SELECT @@IDENTITY")
                match_map_id = int(self.cursor.fetchone()[0])
                match_map_ids[map_number] = match_map_id
                
                # Insert Rounds
                rounds_data = map_data.get('rounds', [])
                for round_data in rounds_data:
                    winner = round_data.get('winner', 'team1')
                    self.cursor.execute(
                        """INSERT INTO MatchRounds (match_map_id, round_number, winner)
                           VALUES (?, ?, ?)""",
                        (match_map_id, round_data.get('round_number'), winner)
                    )
                
                # Insert MatchStats for both teams
                self.cursor.execute(
                    """INSERT INTO MatchStats (match_map_id, team_id, rounds_won, rounds_lost)
                       VALUES (?, ?, ?, ?)""",
                    (match_map_id, team1_id, map_data.get('team1_score', 0), map_data.get('team2_score', 0))
                )
                self.cursor.execute(
                    """INSERT INTO MatchStats (match_map_id, team_id, rounds_won, rounds_lost)
                       VALUES (?, ?, ?, ?)""",
                    (match_map_id, team2_id, map_data.get('team2_score', 0), map_data.get('team1_score', 0))
                )
            
            self.conn.commit()
            
            # --- 4. Update Team Stats (wins/losses) ---
            team1_score = team1_data.get('score', 0)
            team2_score = team2_data.get('score', 0)
            
            if team1_score > team2_score:
                self.update_team_stats(team1_id, won=True)
                self.update_team_stats(team2_id, won=False)
            elif team2_score > team1_score:
                self.update_team_stats(team1_id, won=False)
                self.update_team_stats(team2_id, won=True)
            # If tie, don't update wins/losses, just matches played would be updated
            
            # --- 5. Insert Player Stats ---
            player_stats = match_data.get('player_stats', [])
            player_id_cache = {}
            
            for p_stat in player_stats:
                # Skip "Overall" stats as they're aggregated
                if p_stat.get('map_name') == 'Overall':
                    continue
                
                player_ign = p_stat.get('player_ign')
                if not player_ign:
                    continue
                
                # Get team ID for this player
                team_name = p_stat.get('team_name')
                player_team_id = team1_id if team_name == team1_name else team2_id
                
                # Get or create player with region and join date
                if player_ign not in player_id_cache:
                    player_id_cache[player_ign] = self.insert_player(
                        player_ign,
                        region=p_stat.get('player_region', 'Unknown'),
                        team_id=player_team_id,
                        join_date=p_stat.get('team_join_date')
                    )
                player_id = player_id_cache[player_ign]
                
                # Get agent using hardcoded ID
                agent_name = p_stat.get('agent', 'Unknown')
                agent_id = get_agent_id(agent_name)
                if not agent_id:
                    print(f"  Warning: Unknown agent '{agent_name}', using NULL")
                    agent_id = None
                
                # Find the corresponding match_map_id
                map_name = p_stat.get('map_name')
                match_map_id = None
                for map_data in maps_data:
                    if map_data['map_name'] == map_name:
                        match_map_id = match_map_ids.get(map_data['map_number'])
                        break
                
                if not match_map_id:
                    continue
                
                # Insert PlayerMatches
                self.cursor.execute(
                    """INSERT INTO PlayerMatches (player_id, match_id, match_map_id, agent_id, kills, deaths, assists, score)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (player_id, match_id, match_map_id, agent_id,
                     p_stat.get('kills', 0),
                     p_stat.get('deaths', 0),
                     p_stat.get('assists', 0),
                     p_stat.get('acs', 0))
                )
                
                # Insert AdvancedStats
                self.cursor.execute(
                    """INSERT INTO AdvancedStats (match_id, match_map_id, player_id, headshots, economy_rating, 
                                                   utility_used, acs, adr, kast, hs_percent, first_kills, first_deaths, r2o)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (match_id, match_map_id, player_id,
                     0,  # headshots - not available
                     0,  # economy_rating - not available
                     0,  # utility_used - not available
                     p_stat.get('acs', 0),
                     p_stat.get('adr', 0),
                     p_stat.get('kast_percent', 0),
                     p_stat.get('hs_percent', 0),
                     p_stat.get('first_kills', 0),
                     p_stat.get('first_deaths', 0),
                     p_stat.get('rating', 0))
                )
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            import traceback
            raise
    
    def close(self):
        """Close the database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()