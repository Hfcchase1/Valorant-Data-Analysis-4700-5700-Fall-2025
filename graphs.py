import pyodbc
import matplotlib.pyplot as plt
from PyQt5.QtWidgets import QMessageBox

SERVER = r"localhost\SQLEXPRESS"
DATABASE = "vlr_matches1"
USER = "sa"
PASSWORD = "zelda"
DRIVER = "ODBC Driver 18 for SQL Server"


def connect_db():
    return pyodbc.connect(
        f"Driver={{{DRIVER}}};"
        f"Server={SERVER};"
        f"Database={DATABASE};"
        f"UID={USER};PWD={PASSWORD};"
        "Encrypt=no;"
    )


# =======================================================
# PLAYER GRAPH — KDA LINE (BEST for player development)
# =======================================================
def show_player_kda(player_name: str):
    conn = connect_db()
    cursor = conn.cursor()

    sql = """
    SELECT 
        COALESCE(m.date_played, m.match_date) AS match_date,
        pm.kills, pm.deaths, pm.assists
    FROM PlayerMatches pm
    JOIN Players p ON p.player_id = pm.player_id
    JOIN Matches m ON m.match_id = pm.match_id
    WHERE p.username = ?
    ORDER BY COALESCE(m.date_played, m.match_date)
    """
    cursor.execute(sql, player_name)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        QMessageBox.information(None, "No Data", f"No stats data for {player_name}")
        return

    dates = []
    kda = []

    for r in rows:
        deaths = r.deaths if r.deaths != 0 else 1
        ratio = round((r.kills + r.assists) / deaths, 2)
        dates.append(r.match_date)
        kda.append(ratio)

    plt.style.use("dark_background")
    plt.figure(figsize=(9, 4))

    plt.plot(dates, kda, marker="o", linewidth=2)

    plt.title(f"KDA Progression — {player_name}")
    plt.xlabel("Match Date")
    plt.ylabel("KDA Ratio")
    plt.grid(alpha=0.25)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# =======================================================
# TEAM GRAPH — WIN RATE % LINE (BEST for performance)
# =======================================================
def show_team_win_loss(team_name: str):
    conn = connect_db()
    cursor = conn.cursor()

    sql = """
    SELECT
        COALESCE(m.date_played, m.match_date) AS match_date,
        ms.rounds_won,
        ms.rounds_lost
    FROM MatchStats ms
    JOIN Teams t      ON t.team_id = ms.team_id
    JOIN MatchMaps mm ON mm.match_map_id = ms.match_map_id
    JOIN Matches m    ON m.match_id = mm.match_id
    WHERE t.name = ?
    ORDER BY COALESCE(m.date_played, m.match_date)
    """

    cursor.execute(sql, team_name)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        QMessageBox.information(None, "No Data", f"No match history for {team_name}")
        return

    dates = []
    winrate = []

    for r in rows:
        total = r.rounds_won + r.rounds_lost
        rate = (r.rounds_won / total) * 100 if total else 0
        dates.append(r.match_date)
        winrate.append(round(rate, 1))

    plt.style.use("dark_background")
    plt.figure(figsize=(9, 4))

    plt.plot(dates, winrate, marker="o", linewidth=2, color="lime")

    plt.title(f"Win Rate % Over Time — {team_name}")
    plt.xlabel("Match Date")
    plt.ylabel("Win Rate (%)")
    plt.ylim(0, 100)
    plt.grid(alpha=0.25)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def show_player_agent_pie(player_name: str):
    """
    Displays a pie chart of which agents a player used.
    """

    conn = connect_db()
    cursor = conn.cursor()

    sql = """
    SELECT 
        a.name AS agent,
        COUNT(*) AS games_played
    FROM PlayerMatches pm
    JOIN Players p ON p.player_id = pm.player_id
    JOIN Agents a ON a.agent_id = pm.agent_id
    WHERE p.username = ?
    GROUP BY a.name
    ORDER BY games_played DESC
    """

    cursor.execute(sql, player_name)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        QMessageBox.information(None, "No Data", "No agent data found for this player.")
        return

    labels = [r.agent for r in rows]
    values = [r.games_played for r in rows]

    import matplotlib.pyplot as plt
    plt.style.use("dark_background")

    plt.figure(figsize=(7, 7))
    plt.pie(values, labels=labels, autopct="%1.1f%%", startangle=140)
    plt.title(f"Agent Usage — {player_name}")
    plt.axis("equal")
    plt.tight_layout()
    plt.show()

# =======================================================
# EXCEL-STYLE CHART ENGINE (FROM TABLE SELECTION)
# =======================================================
from collections import defaultdict

def _parse_table_selection(headers, selected_items):
    """
    Convert QTableWidget selection into numeric columns grouped per header.
    """
    data = defaultdict(list)

    for item in selected_items:
        col = item.column()
        header = headers[col]
        text = item.text()

        # accept numbers and % values
        try:
            value = float(text.replace('%',''))
            data[header].append(value)
        except:
            pass

    return data


def show_chart_from_selection(headers, selected_items, chart_type="bar", title="Chart"):
    """
    Build a graph using the user's current table selection.
    chart_type: 'line', 'bar', 'pie'
    """
    import matplotlib.pyplot as plt

    data = _parse_table_selection(headers, selected_items)

    if not data:
        raise ValueError("No numeric values selected. Select numeric columns like kills, ACS, ADR, etc.")

    keys = list(data.keys())
    values = [data[k] for k in keys]

    plt.style.use("dark_background")
    plt.figure(figsize=(8, 4))
    plt.title(title)

    # ---------- BAR ----------
    if chart_type == "bar":
        plt.bar(keys, [sum(v)/len(v) for v in values])
        plt.ylabel("Average Value")

    # ---------- LINE ---------
    elif chart_type == "line":
        for key, vals in data.items():
            plt.plot(vals, marker="o", label=key)
        plt.legend()
        plt.ylabel("Value")
        plt.xlabel("Selection Order")

    # ---------- PIE ----------
    elif chart_type == "pie":
        plt.pie([sum(v) for v in values], labels=keys, autopct="%1.1f%%")

    else:
        raise ValueError("Unknown chart type.")

    plt.tight_layout()
    plt.show()

# ======================================================
# ADVANCED GRAPH BUILDER
# ======================================================
import matplotlib.pyplot as plt

def smart_graph(query, x_col, y_col, chart_type):
    try:
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            QMessageBox.warning(None, "No Data", "No data returned from query.")
            return

        # Extract header names
        columns = [column[0] for column in cursor.description]

        # Convert row data into dictionaries
        data = [dict(zip(columns, row)) for row in rows]

        # X-axis ALWAYS string labels
        x_vals = [str(row[x_col]) for row in data]

        # Y-axis ALWAYS numeric
        def num(x):
            try:
                return float(x)
            except:
                return 0  # or None, but 0 avoids graph crashes

        y_raw = [row[y_col] for row in data]
        y_vals = [num(v) for v in y_raw]

        plt.figure(figsize=(10, 5))

        # ---------- GRAPH TYPES ----------
        if chart_type == "bar":
            plt.bar(x_vals, y_vals)
            plt.ylabel(y_col)
            plt.xlabel(x_col)

        elif chart_type == "line":
            plt.plot(x_vals, y_vals, marker="o")
            plt.ylabel(y_col)
            plt.xlabel(x_col)

        elif chart_type == "pie":
            plt.pie(y_vals, labels=x_vals, autopct="%1.1f%%")

        else:
            raise ValueError("Invalid chart type.")

        plt.tight_layout()
        plt.show()

    except Exception as e:
        QMessageBox.critical(None, "Graph Error", str(e))

import matplotlib.pyplot as plt

def smart_graph_from_table(headers, rows, x_header, y_header, chart_type):
    # Map header to column index
    col_index = {h: i for i, h in enumerate(headers)}

    x_idx = col_index[x_header]
    y_idx = col_index[y_header]

    # Extract
    x_vals = [row[x_idx] for row in rows]

    # Convert y-values to numbers
    def num(v):
        if v is None:
            return 0
        v = str(v).replace("%", "").strip()
        try:
            return float(v)
        except:
            return 0

    y_vals = [num(row[y_idx]) for row in rows]

    # Plot
    plt.figure(figsize=(10, 5))

    if chart_type == "bar":
        plt.bar(x_vals, y_vals)
    elif chart_type == "line":
        plt.plot(x_vals, y_vals, marker="o")
    elif chart_type == "pie":
        plt.pie(y_vals, labels=x_vals, autopct="%1.1f%%")
    else:
        raise ValueError("Unknown chart type")

    plt.xlabel(x_header)
    plt.ylabel(y_header)
    plt.tight_layout()
    plt.show()
