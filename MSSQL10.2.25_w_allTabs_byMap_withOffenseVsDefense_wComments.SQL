-- Maps used in matches
CREATE TABLE [Maps] ( -- Table for storing map names
  [map_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each map
  [name] VARCHAR(50) UNIQUE NOT NULL -- Name of the map
)
GO

-- Tournaments metadata
CREATE TABLE [Tournaments] ( -- Table for storing tournament info
  [tournament_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each tournament
  [name] VARCHAR(100) UNIQUE NOT NULL, -- Tournament name
  [prize_pool] INT, -- Total prize pool amount
  [start_date] DATE, -- Tournament start date
  [end_date] DATE -- Tournament end date
)
GO

-- Teams metadata
CREATE TABLE [Teams] ( -- Table for storing team info
  [team_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each team
  [name] VARCHAR(50) UNIQUE NOT NULL, -- Team name
  [region] VARCHAR(50), -- Region the team belongs to
  [logo_url] VARCHAR(255), -- Optional logo URL
  [created_at] DATETIME, -- Timestamp of team creation
  [updated_at] DATETIME -- Timestamp of last update
)
GO

-- Players metadata
CREATE TABLE [Players] ( -- Table for storing player info
  [player_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each player
  [username] VARCHAR(50) UNIQUE NOT NULL, -- Player's in-game name
  [email] VARCHAR(100) UNIQUE NOT NULL, -- Player's email address
  [rank] VARCHAR(20), -- Player's rank (e.g., Immortal)
  [region] VARCHAR(50), -- Region the player belongs to
  [join_date] DATE -- Date the player joined
)
GO

-- Agents metadata
CREATE TABLE [Agents] ( -- Table for storing agent info
  [agent_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each agent
  [name] VARCHAR(50) UNIQUE NOT NULL, -- Agent name
  [role] VARCHAR(20), -- Agent role (e.g., Duelist)
  [description] TEXT -- Optional description of the agent
)
GO

-- Weapons metadata
CREATE TABLE [Weapons] ( -- Table for storing weapon info
  [weapon_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each weapon
  [name] VARCHAR(50) UNIQUE NOT NULL, -- Weapon name
  [type] VARCHAR(20), -- Weapon type (e.g., Rifle)
  [cost] INT -- Weapon cost in credits
)
GO

-- Match series metadata
CREATE TABLE [Matches] ( -- Table for storing match series info
  [match_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each match
  [match_date] DATETIME, -- Date the match was played
  [tournament_id] INT, -- Linked tournament ID
  [mode] VARCHAR(20), -- Game mode (e.g., Competitive)
  [date_played] DATETIME, -- Actual date and time played
  FOREIGN KEY ([tournament_id]) REFERENCES [Tournaments]([tournament_id]) -- FK to tournament
)
GO

-- Each map played in a match series (Game 1, 2, 3)
CREATE TABLE [MatchMaps] ( -- Table for storing maps played in a match
  [match_map_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each map instance
  [match_id] INT, -- Linked match ID
  [map_id] INT, -- Linked map ID
  [map_order] INT, -- Order of map in series (1 = Game 1)
  [team1_score] INT, -- Score for team 1
  [team2_score] INT, -- Score for team 2
  [duration] INT, -- Duration of map in seconds
  FOREIGN KEY ([match_id]) REFERENCES [Matches]([match_id]), -- FK to match
  FOREIGN KEY ([map_id]) REFERENCES [Maps]([map_id]) -- FK to map
)
GO

-- Round-by-round outcomes
CREATE TABLE [MatchRounds] ( -- Table for storing round outcomes
  [round_id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID for each round
  [match_map_id] INT, -- Linked map ID
  [round_number] INT, -- Round number
  [winner] VARCHAR(50), -- Winner of the round
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]) -- FK to map
)
GO

-- Team performance per map
CREATE TABLE [MatchStats] ( -- Table for storing team stats per map
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_map_id] INT, -- Linked map ID
  [team_id] INT, -- Linked team ID
  [rounds_won] INT, -- Rounds won by team
  [rounds_lost] INT, -- Rounds lost by team
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([team_id]) REFERENCES [Teams]([team_id]) -- FK to team
)
GO

-- Player performance per map
CREATE TABLE [PlayerMatches] ( -- Table for storing player stats per map
  [player_id] INT, -- Linked player ID
  [match_id] INT, -- Linked match ID
  [match_map_id] INT, -- Linked map ID
  [agent_id] INT, -- Agent played
  [kills] INT, -- Total kills
  [deaths] INT, -- Total deaths
  [assists] INT, -- Total assists
  [score] INT, -- In-game score
  FOREIGN KEY ([player_id]) REFERENCES [Players]([player_id]), -- FK to player
  FOREIGN KEY ([match_id]) REFERENCES [Matches]([match_id]), -- FK to match
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([agent_id]) REFERENCES [Agents]([agent_id]) -- FK to agent
)
GO

-- Advanced scoreboard metrics per map
CREATE TABLE [AdvancedStats] ( -- Table for storing detailed player metrics
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_id] INT, -- Linked match ID
  [match_map_id] INT, -- Linked map ID
  [player_id] INT, -- Linked player ID
  [headshots] INT, -- Total headshots
  [economy_rating] FLOAT, -- Economy rating
  [utility_used] INT, -- Utility usage count
  [acs] INT, -- Average Combat Score
  [adr] FLOAT, -- Average Damage per Round
  [kast] FLOAT, -- Kill/Assist/Survive/Trade %
  [hs_percent] FLOAT, -- Headshot percentage
  [first_kills] INT, -- First kills
  [first_deaths] INT, -- First deaths
  [r2o] INT, -- Rating
  FOREIGN KEY ([match_id]) REFERENCES [Matches]([match_id]), -- FK to match
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([player_id]) REFERENCES [Players]([player_id]) -- FK to player
)
GO

-- Combat log: who killed whom, when, with what
CREATE TABLE [CombatLog] ( -- Table for storing kill events
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_map_id] INT, -- Linked map ID
  [round_number] INT, -- Round number
  [timestamp] INT, -- Seconds into round
  [attacker_id] INT, -- Killer player ID
  [victim_id] INT, -- Victim player ID
  [weapon_id] INT, -- Weapon used
  [is_first_kill] BIT, -- Was it first kill of round?
  [is_op_kill] BIT, -- Was it an Operator kill?
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([attacker_id]) REFERENCES [Players]([player_id]), -- FK to attacker
  FOREIGN KEY ([victim_id]) REFERENCES [Players]([player_id]), -- FK to victim
  FOREIGN KEY ([weapon_id]) REFERENCES [Weapons]([weapon_id]) -- FK to weapon
)
GO

-- Multi-kill rounds (1K–5K)
CREATE TABLE [MultiKillStats] ( -- Table for tracking multi-kill rounds
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_map_id] INT, -- Linked map ID
  [round_number] INT, -- Round number
  [player_id] INT, -- Player who got the multi-kill
  [kills_in_round] INT, -- Number of kills (1–5)
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([player_id]) REFERENCES [Players]([player_id]) -- FK to player
)
GO

-- Clutch scenarios (1v1–1v5)
CREATE TABLE [ClutchStats] ( -- Table for tracking clutch attempts
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_map_id] INT, -- Linked map ID
  [round_number] INT, -- Round number
  [player_id] INT, -- Player attempting the clutch
  [clutch_type] VARCHAR(10), -- Format like '1v2', '1v3'
  [was_successful] BIT, -- Whether the clutch was successful
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([player_id]) REFERENCES [Players]([player_id]) -- FK to player
)
GO

-- Economy summary per team per map
CREATE TABLE [EconomyStats] ( -- Table for tracking economy round wins
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_map_id] INT, -- Linked map ID
  [team_id] INT, -- Linked team ID
  [pistol_rounds_won] INT, -- Number of pistol rounds won
  [eco_rounds_won] INT, -- Wins with 0–5k credits
  [semi_eco_rounds_won] INT, -- Wins with 5–10k credits
  [semi_buy_rounds_won] INT, -- Wins with 10–20k credits
  [full_buy_rounds_won] INT, -- Wins with 20k+ credits
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([team_id]) REFERENCES [Teams]([team_id]) -- FK to team
)
GO

-- Per-round team economy values
CREATE TABLE [RoundEconomy] ( -- Table for tracking team money per round
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_map_id] INT, -- Linked map ID
  [round_number] INT, -- Round number
  [team_id] INT, -- Linked team ID
  [money] INT, -- Total team money in credits
  [economy_category] VARCHAR(10), -- Category: 'Eco', '$', '$$', '$$$'
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([team_id]) REFERENCES [Teams]([team_id]) -- FK to team
)
GO

-- Team-side performance per map
CREATE TABLE [TeamSideMetrics] ( -- Table for tracking team stats by side
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_map_id] INT, -- Linked map ID
  [team_id] INT, -- Linked team ID
  [side] VARCHAR(10), -- 'Attack' or 'Defense'
  [rounds_played] INT, -- Rounds played on this side
  [rounds_won] INT, -- Rounds won on this side
  [total_kills] INT, -- Total kills on this side
  [total_deaths] INT, -- Total deaths on this side
  [plants] INT, -- Spike plants
  [defuses] INT, -- Spike defuses
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([team_id]) REFERENCES [Teams]([team_id]) -- FK to team
)
GO

-- Player-side scoreboard metrics per map
CREATE TABLE [PlayerSideMetrics] ( -- Table for tracking player stats by side
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [match_map_id] INT, -- Linked map ID
  [player_id] INT, -- Linked player ID
  [side] VARCHAR(10), -- 'Attack' or 'Defense'
  [r2o] FLOAT, -- Rating
  [acs] INT, -- Average Combat Score
  [kills] INT, -- Total kills
  [deaths] INT, -- Total deaths
  [assists] INT, -- Total assists
  [kast] FLOAT, -- Kill/Assist/Survive/Trade %
  [adr] FLOAT, -- Average Damage per Round
  [hs_percent] FLOAT, -- Headshot percentage
  [first_kills] INT, -- First kills
  [first_deaths] INT, -- First deaths
  FOREIGN KEY ([match_map_id]) REFERENCES [MatchMaps]([match_map_id]), -- FK to map
  FOREIGN KEY ([player_id]) REFERENCES [Players]([player_id]) -- FK to player
)
GO

-- Links teams to tournaments
CREATE TABLE [TournamentTeams] ( -- Table linking teams to tournaments
  [tournament_id] INT, -- Linked tournament ID
  [team_id] INT, -- Linked team ID
  FOREIGN KEY ([tournament_id]) REFERENCES [Tournaments]([tournament_id]), -- FK to tournament
  FOREIGN KEY ([team_id]) REFERENCES [Teams]([team_id]) -- FK to team
)
GO

-- Links players to teams
CREATE TABLE [TeamPlayers] ( -- Table linking players to teams
  [team_id] INT, -- Linked team ID
  [player_id] INT, -- Linked player ID
  [join_date] DATE, -- Date player joined the team
  FOREIGN KEY ([team_id]) REFERENCES [Teams]([team_id]), -- FK to team
  FOREIGN KEY ([player_id]) REFERENCES [Players]([player_id]) -- FK to player
)
GO

-- Team-level aggregate stats across all matches
CREATE TABLE [TeamStats] ( -- Table for storing team-wide match stats
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [team_id] INT, -- Linked team ID
  [matches_played] INT, -- Total matches played
  [matches_won] INT, -- Total matches won
  [matches_lost] INT, -- Total matches lost
  FOREIGN KEY ([team_id]) REFERENCES [Teams]([team_id]) -- FK to team
)
GO

-- Prize distribution by tournament placement
CREATE TABLE [PrizePool] ( -- Table for storing prize payouts
  [id] INT PRIMARY KEY IDENTITY(1, 1), -- Unique ID
  [tournament_id] INT NOT NULL, -- Linked tournament ID
  [placement] INT UNIQUE NOT NULL, -- Placement (e.g., 1st, 2nd)
  [prize] VARCHAR(20), -- Prize amount or label
  FOREIGN KEY ([tournament_id]) REFERENCES [Tournaments]([tournament_id]) -- FK to tournament
)
GO