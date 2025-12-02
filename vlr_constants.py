"""
VLR.gg Constants - Hardcoded data for agents and maps
"""

# Hardcoded agent roles with IDs
AGENT_DATA = {
    'Brimstone': {'agent_id': 1, 'role': 'Controller'},
    'Viper': {'agent_id': 2, 'role': 'Controller'},
    'Omen': {'agent_id': 3, 'role': 'Controller'},
    'Killjoy': {'agent_id': 4, 'role': 'Sentinel'},
    'Cypher': {'agent_id': 5, 'role': 'Sentinel'},
    'Sova': {'agent_id': 6, 'role': 'Initiator'},
    'Sage': {'agent_id': 7, 'role': 'Sentinel'},
    'Phoenix': {'agent_id': 8, 'role': 'Duelist'},
    'Jett': {'agent_id': 9, 'role': 'Duelist'},
    'Reyna': {'agent_id': 10, 'role': 'Duelist'},
    'Raze': {'agent_id': 11, 'role': 'Duelist'},
    'Breach': {'agent_id': 12, 'role': 'Initiator'},
    'Skye': {'agent_id': 13, 'role': 'Initiator'},
    'Yoru': {'agent_id': 14, 'role': 'Duelist'},
    'Astra': {'agent_id': 15, 'role': 'Controller'},
    'KAY/O': {'agent_id': 16, 'role': 'Initiator'},
    'KAYO': {'agent_id': 16, 'role': 'Initiator'},
    'Chamber': {'agent_id': 17, 'role': 'Sentinel'},
    'Neon': {'agent_id': 18, 'role': 'Duelist'},
    'Fade': {'agent_id': 19, 'role': 'Initiator'},
    'Harbor': {'agent_id': 20, 'role': 'Controller'},
    'Gekko': {'agent_id': 21, 'role': 'Initiator'},
    'Deadlock': {'agent_id': 22, 'role': 'Sentinel'},
    'Iso': {'agent_id': 23, 'role': 'Duelist'},
    'Clove': {'agent_id': 24, 'role': 'Controller'},
    'Vyse': {'agent_id': 25, 'role': 'Sentinel'},
    'Tejo': {'agent_id': 26, 'role': 'Initiator'},
    'Waylay': {'agent_id': 27, 'role': 'Duelist'},
    'Veto': {'agent_id': 28, 'role': 'Sentinel'}
}

# Hardcoded map IDs by release order 
MAP_DATA = {
    'Bind': 1,
    'Haven': 2,
    'Split': 3,
    'Ascent': 4,
    'Icebox': 5,
    'Breeze': 6,
    'Fracture': 7,
    'Pearl': 8,
    'Lotus': 9,
    'Sunset': 10,
    'Abyss': 11,
    'Corrode': 12
}

def get_agent_id(agent_name: str) -> int:
    """Get agent ID from name (case-insensitive, handles variations)"""
    if not agent_name:
        return None
    
    # Normalize the name
    agent_name = agent_name.strip()
    
    # Direct lookup
    if agent_name in AGENT_DATA:
        return AGENT_DATA[agent_name]['agent_id']
    
    # Case-insensitive lookup
    for key in AGENT_DATA:
        if key.upper() == agent_name.upper():
            return AGENT_DATA[key]['agent_id']
    
    # Handle KAY/O variations
    if agent_name.upper() in ['KAYO', 'KAY-O', 'KAY O']:
        return AGENT_DATA['KAY/O']['agent_id']
    
    return None

def get_agent_role(agent_name: str) -> str:
    """Get agent role from name (case-insensitive, handles variations)"""
    if not agent_name:
        return 'Unknown'
    
    # Normalize the name
    agent_name = agent_name.strip()
    
    # Direct lookup
    if agent_name in AGENT_DATA:
        return AGENT_DATA[agent_name]['role']
    
    # Case-insensitive lookup
    for key in AGENT_DATA:
        if key.upper() == agent_name.upper():
            return AGENT_DATA[key]['role']
    
    # Handle KAY/O variations
    if agent_name.upper() in ['KAYO', 'KAY-O', 'KAY O']:
        return AGENT_DATA['KAY/O']['role']
    
    return 'Unknown'

def get_map_id(map_name: str) -> int:
    """Get map ID from name"""
    return MAP_DATA.get(map_name, None)
