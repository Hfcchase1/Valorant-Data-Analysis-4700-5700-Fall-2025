import sys
import pyodbc
from typing import List, Set

import matplotlib.pyplot as plt

from graphs import show_player_kda, show_team_win_loss, show_player_agent_pie, smart_graph

from PyQt5.QtCore import (
    Qt, QThread, pyqtSignal, QPropertyAnimation, QRect, QEasingCurve
)
from PyQt5.QtGui import QFont, QIcon
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QMessageBox, QFrame, QCheckBox, QSpacerItem, QSizePolicy,
    QGraphicsOpacityEffect
)


# DATABASE CONFIGURATION
SERVER = r"localhost\SQLEXPRESS"
DATABASE = "vlr_matches1" #adjust to your database name
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

# SEARCH WORKER
class RelationalSearchWorker(QThread):
    results_ready = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, terms: List[str]):
        super().__init__()
        self.terms = [t.strip() for t in terms if t.strip()]

    def run(self):
        try:
            conn = connect_db()
            cur = conn.cursor()

            sql = """
            SELECT DISTINCT
                m.match_id,
                COALESCE(m.date_played, m.match_date) AS match_date,
                m.mode,
                ISNULL(tour.name,'') AS tournament,
                ISNULL(mp.name,'') AS map,
                mm.map_order,
                ISNULL(t.name,'') AS team,
                ISNULL(p.username,'') AS player,
                ISNULL(a.name,'') AS agent
            FROM Matches m
            JOIN MatchMaps mm ON mm.match_id = m.match_id
            LEFT JOIN MatchStats ms ON ms.match_map_id = mm.match_map_id
            LEFT JOIN Teams t ON t.team_id = ms.team_id
            LEFT JOIN PlayerMatches pm ON pm.match_map_id = mm.match_map_id
            LEFT JOIN Players p ON p.player_id = pm.player_id
            LEFT JOIN Agents a ON a.agent_id = pm.agent_id
            LEFT JOIN Maps mp ON mp.map_id = mm.map_id
            LEFT JOIN Tournaments tour ON tour.tournament_id = m.tournament_id
            """

            search_fields = [
                "ISNULL(p.username,'')",
                "ISNULL(t.name,'')",
                "ISNULL(a.name,'')",
                "ISNULL(a.role,'')",
                "ISNULL(mp.name,'')",
                "ISNULL(tour.name,'')",
                "ISNULL(m.mode,'')",
                "CONVERT(VARCHAR,m.match_date,23)"
            ]

            where = []
            params = []

            for term in self.terms:
                ors = []
                for field in search_fields:
                    ors.append(f"{field} LIKE ?")
                    params.append(f"%{term}%")
                if ors:
                    where.append("(" + " OR ".join(ors) + ")")

            if where:
                sql += " WHERE " + " AND ".join(where)

            sql += " ORDER BY COALESCE(m.date_played,m.match_date) DESC"

            cur.execute(sql, params)
            rows = cur.fetchall()
            conn.close()

            results = []
            for r in rows:
                results.append((
                    r.match_id,
                    r.match_date,
                    r.mode,
                    r.tournament,
                    r.map,
                    r.map_order,
                    r.team,
                    r.player,
                    r.agent
                ))

            self.results_ready.emit(results)

        except Exception as e:
            self.error.emit(str(e))


# MAIN GUI
class ValorantSearch(QWidget):
    def __init__(self):
        super().__init__()
        self.worker = None
        self._anim = None
        self.all_rows: List[tuple] = []
        self.search_history: List[str] = []
        # keep secondary windows alive
        self.open_match_windows: List[QWidget] = []

        self.setWindowTitle("Radiant Report")
        self.setWindowIcon(QIcon("logo.png"))

        self.setGeometry(200, 100, 1300, 750)

        self.setup_ui()
        self.setStyleSheet(self.style_sheet())

    #CLICK HANDLER
    def handle_graph_click(self, row, column):
        """
        Double-click behavior:
        - Match ID → Match Details window (grouped by map)
        - Team → Team win-rate graph
        - Player → Agent usage pie + KDA graph
        """
        if self.table.columnCount() == 0:
            return

        headers = [self.table.horizontalHeaderItem(i).text()
                   for i in range(self.table.columnCount())]
        header = headers[column]
        item = self.table.item(row, column)
        if not item:
            return
        value = item.text().strip()

        # Match details on Match ID
        if header == "Match ID":
            self.show_match_details(value)
            return

        # Team win-rate graph
        if header == "Team" and value:
            show_team_win_loss(value)
            return

        # Player graphs
        if "Player" in headers:
            player_col = headers.index("Player")
            player_item = self.table.item(row, player_col)
            if player_item and player_item.text().strip():
                player_name = player_item.text().strip()
                show_player_agent_pie(player_name)
                show_player_kda(player_name)
                return

        QMessageBox.information(self, "No Graph", "No player or team data in this row.")

    #MATCH DETAILS (GROUPED BY MAP)
    def show_match_details(self, match_id):
        """
        Modern match viewer:
        - MAP tabs
        - Team headers
        - Individual player stats
        - Scrollable window
        - Per-round win/loss timeline
        """

        from PyQt5.QtWidgets import QTabWidget, QGridLayout, QScrollArea

        try:
            conn = connect_db()
            cursor = conn.cursor()

            # PLAYER STATS QUERY
            sql = """
            SELECT
                mp.name AS map_name,
                t.name AS team,
                p.username AS player,
                a.name AS agent,
                pm.kills,
                pm.deaths,
                pm.assists,
                ast.acs,
                ast.adr,
                ast.hs_percent,
                ast.kast,
                ast.first_kills,
                ast.first_deaths,
                ast.r2o
            FROM PlayerMatches pm
            JOIN MatchMaps mm ON mm.match_map_id = pm.match_map_id
            JOIN Maps mp ON mp.map_id = mm.map_id
            JOIN Players p ON p.player_id = pm.player_id
            JOIN Agents a ON a.agent_id = pm.agent_id
            LEFT JOIN AdvancedStats ast 
                ON ast.match_map_id = pm.match_map_id AND ast.player_id = pm.player_id
            LEFT JOIN MatchStats ms ON ms.match_map_id = mm.match_map_id
            LEFT JOIN Teams t ON t.team_id = ms.team_id
            WHERE mm.match_id = ?
            ORDER BY mp.name, t.name, pm.kills DESC
            """
            cursor.execute(sql, match_id)
            rows = cursor.fetchall()

            # ROUND TIMELINE QUERY
            round_sql = """
            SELECT
                mp.name AS map_name,
                t.name AS team,
                mr.round_number,
                mr.winner
            FROM MatchRounds mr
            JOIN MatchMaps mm ON mm.match_map_id = mr.match_map_id
            JOIN Maps mp ON mp.map_id = mm.map_id
            JOIN MatchStats ms ON ms.match_map_id = mm.match_map_id
            JOIN Teams t ON t.team_id = ms.team_id
            WHERE mm.match_id = ?
            ORDER BY mp.name, mr.round_number
            """
            cursor.execute(round_sql, match_id)
            round_rows = cursor.fetchall()

            conn.close()

            if not rows:
                QMessageBox.warning(self, "No Data", "No stats found for this match.")
                return

            # GROUP PLAYER DATA
            maps = {}
            all_players = []

            for r in rows:
                maps.setdefault(r.map_name, {})
                maps[r.map_name].setdefault(r.team, [])
                maps[r.map_name][r.team].append(r)
                all_players.append(r)

            # GROUP ROUND DATA
            rounds = {}
            for r in round_rows:
                rounds.setdefault(r.map_name, {})
                rounds[r.map_name].setdefault(r.team, [])
                rounds[r.map_name][r.team].append((r.round_number, r.winner))

            # WINDOW
            win = QWidget()
            self.open_match_windows.append(win)
            win.setWindowTitle(f"Match Details — Match {match_id}")
            win.resize(1250, 720)
            win.setStyleSheet("background:#0b121a;")

            main = QVBoxLayout(win)
            main.setContentsMargins(20, 16, 20, 16)
            main.setSpacing(12)

            title = QLabel("MATCH ANALYTICS")
            title.setFont(QFont("Segoe UI", 16, QFont.Bold))
            title.setAlignment(Qt.AlignCenter)
            title.setStyleSheet("color:#ff4655; letter-spacing:1px;")
            main.addWidget(title)

            # TABS + SCROLL AREA
            tabs = QTabWidget()
            tabs.setStyleSheet("""
                QTabWidget::pane { border:none; }
                QTabBar::tab {
                    background:#141e2b;
                    color:#8faac2;
                    padding:10px 20px;
                    border-radius:8px;
                    margin-right:4px;
                    font-weight:600;
                }
                QTabBar::tab:selected {
                    background:#ff4655;
                    color:white;
                }
            """)

            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            scroll.setFrameShape(QFrame.NoFrame)
            scroll.setWidget(tabs)
            main.addWidget(scroll)

            # ROUND STRIP BUILD FUNC
            def build_round_strip(team_data):
                strip = QVBoxLayout()

                for team, rounds_list in team_data.items():
                    row = QHBoxLayout()

                    team_lbl = QLabel(team)
                    team_lbl.setFixedWidth(120)
                    team_lbl.setStyleSheet("font-weight:700;color:#ddd;")
                    row.addWidget(team_lbl)

                    for rnum, winner in rounds_list:
                        win_flag = team in winner
                        box = QLabel()
                        box.setFixedSize(22, 22)
                        box.setStyleSheet(f"""
                            background: {'#1fe4b3' if win_flag else '#ff5a5a'};
                            border-radius:5px;
                        """)
                        row.addWidget(box)

                    row.addStretch()
                    strip.addLayout(row)

                return strip

            # PLAYER TABLE GENERATOR
            def stat_cell(text, positive=None):
                lbl = QLabel(str(text))
                lbl.setAlignment(Qt.AlignCenter)
                color = "#e6edf6"
                if positive is True:
                    color = "#00ff9c"
                elif positive is False:
                    color = "#ff5c5c"

                lbl.setStyleSheet(f"""
                    font-size:13px;
                    font-weight:600;
                    color:{color};
                    background:#16202d;
                    padding:8px;
                    border-radius:8px;
                """)
                return lbl

            headers = ["PLAYER", "AGENT", "R", "ACS", "K / D / A", "+/-",
                       "KAST", "ADR", "HS%", "FK", "FD", "+/-"]

            def build_team_block(team_name, players):
                from PyQt5.QtWidgets import QGridLayout

                frame = QFrame()
                frame.setStyleSheet("""
                    background:#101b27;
                    border-radius:14px;
                    padding:12px;
                """)
                layout = QVBoxLayout(frame)
                layout.setSpacing(8)
                layout.setContentsMargins(14, 10, 14, 12)

                label = QLabel(team_name)
                label.setStyleSheet("""
                    font-size:18px;
                    font-weight:700;
                    color:#e6edf6;
                    border-bottom:1px solid rgba(255,255,255,0.05);
                    padding-bottom:6px;
                """)
                layout.addWidget(label)

                grid = QGridLayout()
                grid.setSpacing(8)

                for c, text in enumerate(headers):
                    hdr = QLabel(text)
                    hdr.setAlignment(Qt.AlignCenter)
                    hdr.setStyleSheet("font-size:11px;color:#8faac2;")
                    grid.addWidget(hdr, 0, c)

                r = 1
                for p in players:
                    k = p.kills or 0
                    d = p.deaths or 0
                    a = p.assists or 0
                    diff = k - d

                    fk = p.first_kills or 0
                    fd = p.first_deaths or 0
                    entry = fk - fd

                    rating = round(k / d if d else k, 2)

                    values = [
                        p.player,
                        p.agent,
                        rating,
                        p.acs or "—",
                        f"{k}/{d}/{a}",
                        diff,
                        f"{int(p.kast)}%" if p.kast else "—",
                        int(p.adr) if p.adr else "—",
                        f"{int(p.hs_percent)}%" if p.hs_percent else "—",
                        fk,
                        fd,
                        entry
                    ]

                    for c, v in enumerate(values):
                        pos = None
                        if c in (5, 11):
                            pos = v > 0
                        grid.addWidget(stat_cell(v, pos), r, c)

                    r += 1

                layout.addLayout(grid)
                return frame

            # MAP TABS
            for map_name, teams in maps.items():
                tab = QWidget()
                outer = QVBoxLayout(tab)
                outer.setSpacing(12)

                # ROUND TIMELINE
                if map_name in rounds:
                    strip = QWidget()
                    strip_layout = QVBoxLayout(strip)
                    strip_layout.addLayout(build_round_strip(rounds[map_name]))

                    legend = QLabel("🟩 = Win      🟥 = Loss")
                    legend.setStyleSheet("font-size:11px;color:#9eb2c3;")
                    strip_layout.addWidget(legend)

                    outer.addWidget(strip)

                # PLAYER STATS
                for team, players in teams.items():
                    outer.addWidget(build_team_block(team, players))

                outer.addStretch()
                tabs.addTab(tab, map_name.upper())

            # ALL TAB
            all_tab = QWidget()
            outer = QVBoxLayout(all_tab)
            grouped = {}

            for r in all_players:
                grouped.setdefault(r.team, []).append(r)

            for team, players in grouped.items():
                outer.addWidget(build_team_block(team, players))

            outer.addStretch()
            tabs.insertTab(0, all_tab, "ALL")

            win.show()

        except Exception as e:
            QMessageBox.critical(self, "Match View Error", str(e))

    #UI SETUP
    def setup_ui(self):
        # Root layout: left nav + main content
        root_h = QHBoxLayout(self)
        root_h.setContentsMargins(12, 12, 12, 12)
        root_h.setSpacing(12)

        #MAIN CONTENT
        self.root = QVBoxLayout()
        self.root.setContentsMargins(4, 4, 4, 4)
        self.root.setSpacing(10)
        root_h.addLayout(self.root, 1)

        header = QHBoxLayout()
        header.setSpacing(12)

        title_block = QVBoxLayout()
        title = QLabel("RADIANT REPORT")
        title.setStyleSheet("""
            font-size: 22px;
            font-weight: 800;
            letter-spacing: 1px;
            color: #ffffff;
        """)
        subtitle = QLabel("Search · Analyze · Visualize — Player & Match Insights")
        subtitle.setStyleSheet("color: #9eb2c3; font-size: 12px;")

        title_block.addWidget(title)
        title_block.addWidget(subtitle)
        header.addLayout(title_block)
        header.addStretch()

        self.btn_filters = QPushButton("Filters")
        self.btn_filters.setFixedHeight(36)
        self.btn_filters.clicked.connect(self.toggle_filters)
        header.addWidget(self.btn_filters)

        self.root.addLayout(header)

        divider = QFrame()
        divider.setFixedHeight(3)
        divider.setStyleSheet(
            "background: qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 #ff4655, stop:1 #ff7a5a);"
        )
        self.root.addWidget(divider)

        # Search row
        search_row = QHBoxLayout()
        search_row.setSpacing(8)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search players, teams, maps, tournaments, dates...")
        self.search_input.setMinimumHeight(44)
        self.search_input.returnPressed.connect(self.run_search)

        self.btn_search = QPushButton("SEARCH")
        self.btn_search.setMinimumHeight(44)
        self.btn_search.clicked.connect(self.run_search)

        search_row.addWidget(self.search_input)
        search_row.addWidget(self.btn_search)
        self.root.addLayout(search_row)

        # Graph Builder row
        graph_row = QHBoxLayout()
        graph_row.addStretch()
        self.btn_graph_builder = QPushButton("Graph Builder")
        self.btn_graph_builder.setFixedHeight(36)
        self.btn_graph_builder.clicked.connect(self.open_graph_builder)
        graph_row.addWidget(self.btn_graph_builder)
        self.root.addLayout(graph_row)

        # Search history row
        self.history_frame = QFrame()
        history_layout = QHBoxLayout(self.history_frame)
        history_layout.setContentsMargins(0, 0, 0, 0)
        history_layout.setSpacing(6)
        self.history_layout = history_layout
        self.root.addWidget(self.history_frame)

        # Summary stat cards
        cards_row = QHBoxLayout()
        cards_row.setSpacing(10)

        self.card_matches = self.build_stat_card("MATCHES", "0")
        self.card_teams = self.build_stat_card("TEAMS", "0")
        self.card_players = self.build_stat_card("PLAYERS", "0")
        self.card_daterange = self.build_stat_card("DATE RANGE", "—")

        cards_row.addWidget(self.card_matches)
        cards_row.addWidget(self.card_teams)
        cards_row.addWidget(self.card_players)
        cards_row.addWidget(self.card_daterange)

        self.root.addLayout(cards_row)

        # Table card
        self.table = QTableWidget()
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.cellDoubleClicked.connect(self.handle_graph_click)
        self.table.setMouseTracking(True)

        table_card = QFrame()
        table_layout = QVBoxLayout(table_card)
        table_layout.setContentsMargins(10, 10, 10, 10)
        table_layout.addWidget(self.table)
        self.root.addWidget(table_card, 1)

        # Status
        self.status = QLabel("Ready.")
        self.status.setAlignment(Qt.AlignRight)
        self.status.setStyleSheet("color:#9eb2c3; font-size:12px; padding-right:6px;")
        self.root.addWidget(self.status)

        # Dim & loading overlays
        self.dim = QFrame(self)
        self.dim.setStyleSheet("background: rgba(0,0,0,0.60);")
        self.dim.hide()

        self.loading_overlay = QFrame(self)
        self.loading_overlay.setStyleSheet("background: rgba(0,0,0,0.65);")
        loading_layout = QVBoxLayout(self.loading_overlay)
        loading_layout.setAlignment(Qt.AlignCenter)
        loading_label = QLabel("Searching…")
        loading_label.setStyleSheet("font-size:16px; color:white;")
        loading_layout.addWidget(loading_label)
        self.loading_overlay.hide()

        # Filter drawer
        self.drawer = QFrame(self)
        self.drawer.setFixedWidth(320)
        self.drawer.setObjectName("drawer")
        self.drawer.hide()

        dv = QVBoxLayout(self.drawer)
        dv.setContentsMargins(16, 16, 16, 16)
        dv.setSpacing(10)

        hdr = QLabel("COLUMN VIEW FILTER")
        hdr.setStyleSheet("font-size:16px; font-weight:600;")
        dv.addWidget(hdr)

        desc = QLabel("Toggle which types of data you see in the table.")
        desc.setStyleSheet("font-size:12px; color:#9eb2c3;")
        dv.addWidget(desc)

        dv.addSpacing(8)

        self.chk_matches = QCheckBox("Matches (ID / Date / Mode)")
        self.chk_teams = QCheckBox("Teams")
        self.chk_players = QCheckBox("Players")
        self.chk_agents = QCheckBox("Agents")
        self.chk_maps = QCheckBox("Maps")
        self.chk_tournaments = QCheckBox("Tournaments")

        for cb in [
            self.chk_matches, self.chk_teams, self.chk_players,
            self.chk_agents, self.chk_maps, self.chk_tournaments
        ]:
            cb.stateChanged.connect(self.apply_column_filters)
            dv.addWidget(cb)

        dv.addStretch()

        self.btn_close = QPushButton("Close")
        self.btn_close.setFixedHeight(36)
        self.btn_close.clicked.connect(self.toggle_filters)
        dv.addWidget(self.btn_close)

        self.disable_filters()

        #Table fade effect
        self.table_effect = QGraphicsOpacityEffect(self.table)
        self.table.setGraphicsEffect(self.table_effect)

    #STAT CARD BUILDER
    def build_stat_card(self, title: str, value: str) -> QFrame:
        card = QFrame()
        layout = QVBoxLayout(card)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(4)

        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("font-size:11px; color:#9eb2c3;")
        lbl_value = QLabel(value)
        lbl_value.setStyleSheet("font-size:18px; font-weight:700;")

        layout.addWidget(lbl_title)
        layout.addWidget(lbl_value)
        layout.addStretch()

        card.title_label = lbl_title
        card.value_label = lbl_value
        return card

    #NAV PRESETS
    def preset_overview(self):
        for cb in [
            self.chk_matches, self.chk_teams, self.chk_players,
            self.chk_agents, self.chk_maps, self.chk_tournaments
        ]:
            cb.setChecked(False)
        self.apply_column_filters()

    def preset_players(self):
        self.chk_matches.setChecked(False)
        self.chk_teams.setChecked(False)
        self.chk_players.setChecked(True)
        self.chk_agents.setChecked(True)
        self.chk_maps.setChecked(False)
        self.chk_tournaments.setChecked(False)
        self.apply_column_filters()

    def preset_teams(self):
        self.chk_matches.setChecked(False)
        self.chk_teams.setChecked(True)
        self.chk_players.setChecked(False)
        self.chk_agents.setChecked(False)
        self.chk_maps.setChecked(True)
        self.chk_tournaments.setChecked(True)
        self.apply_column_filters()

    def preset_matches(self):
        self.chk_matches.setChecked(True)
        self.chk_teams.setChecked(True)
        self.chk_players.setChecked(False)
        self.chk_agents.setChecked(False)
        self.chk_maps.setChecked(True)
        self.chk_tournaments.setChecked(True)
        self.apply_column_filters()

    def preset_tournaments(self):
        self.chk_matches.setChecked(False)
        self.chk_teams.setChecked(True)
        self.chk_players.setChecked(False)
        self.chk_agents.setChecked(False)
        self.chk_maps.setChecked(False)
        self.chk_tournaments.setChecked(True)
        self.apply_column_filters()

    #FILTER ENABLE/DISABLE
    def disable_filters(self):
        for cb in [
            self.chk_matches, self.chk_teams, self.chk_players,
            self.chk_agents, self.chk_maps, self.chk_tournaments
        ]:
            cb.setChecked(False)
            cb.setEnabled(False)

    def enable_filters(self):
        for cb in [
            self.chk_matches, self.chk_teams, self.chk_players,
            self.chk_agents, self.chk_maps, self.chk_tournaments
        ]:
            cb.setEnabled(True)

    #RESIZE / OVERLAYS
    def resizeEvent(self, e):
        super().resizeEvent(e)
        self.dim.setGeometry(self.rect())
        self.loading_overlay.setGeometry(self.rect())
        if not self.drawer.isVisible():
            self.drawer.move(-self.drawer.width(), 0)
        else:
            self.drawer.move(0, 0)

    def toggle_filters(self):
        showing = self.drawer.isVisible()
        if showing:
            self.animate_drawer(False)
        else:
            self.dim.show()
            self.animate_drawer(True)

    def animate_drawer(self, open_it: bool):
        w = self.drawer.width()
        h = self.height()
        self.drawer.setFixedHeight(h)
        self.drawer.show()

        anim = QPropertyAnimation(self.drawer, b"geometry", self)
        anim.setDuration(260)
        anim.setEasingCurve(QEasingCurve.OutCubic)

        if open_it:
            anim.setStartValue(QRect(-w, 0, w, h))
            anim.setEndValue(QRect(0, 0, w, h))
        else:
            anim.setStartValue(QRect(0, 0, w, h))
            anim.setEndValue(QRect(-w, 0, w, h))

        def done():
            if not open_it:
                self.drawer.hide()
                self.dim.hide()

        anim.finished.connect(done)
        anim.start()
        self._anim = anim

    #SEARCH
    def run_search(self):
        text = self.search_input.text().strip()
        if not text:
            self.status.setText("Enter search text.")
            return

        if text not in self.search_history:
            self.search_history.insert(0, text)
            if len(self.search_history) > 6:
                self.search_history.pop()
        self.update_search_history_pills()

        self.status.setText("Searching…")
        self.table.clear()
        self.table.setRowCount(0)
        self.table.setColumnCount(0)
        self.disable_filters()
        self.loading_overlay.show()

        self.worker = RelationalSearchWorker(text.split(","))
        self.worker.results_ready.connect(self.display_results)
        self.worker.error.connect(self.show_error)
        self.worker.start()

    #SEARCH HISTORY PILLS
    def update_search_history_pills(self):
        while self.history_layout.count():
            item = self.history_layout.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()

        if not self.search_history:
            self.history_frame.hide()
            return

        self.history_frame.show()
        for q in self.search_history:
            pill = QPushButton(q)
            pill.setCursor(Qt.PointingHandCursor)
            pill.setStyleSheet("""
                QPushButton {
                    background: rgba(20, 30, 45, 0.85);
                    border-radius: 14px;
                    padding: 4px 10px;
                    font-size: 11px;
                    color:#d0d7e2;
                }
                QPushButton:hover {
                    background: rgba(255,70,85,0.35);
                    color: white;
                }
            """)
            pill.clicked.connect(lambda _, txt=q: self.reuse_search(txt))
            self.history_layout.addWidget(pill)

        spacer = QSpacerItem(20, 10, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.history_layout.addItem(spacer)

    def reuse_search(self, text: str):
        self.search_input.setText(text)
        self.run_search()

    #DISPLAY RESULTS
    def display_results(self, rows):
        self.loading_overlay.hide()
        self.all_rows = rows

        if not rows:
            self.status.setText("No results found.")
            return

        self.enable_filters()

        self.column_map = {
            "Matches": [0, 1, 2],
            "Tournaments": [3],
            "Maps": [4, 5],
            "Teams": [6],
            "Players": [7],
            "Agents": [8]
        }

        self.col_names = [
            "Match ID", "Date", "Mode", "Tournament",
            "Map", "Map #", "Team", "Player", "Agent"
        ]

        self.update_summary_cards(rows)
        self.apply_column_filters()
        self.status.setText(f"{len(self.all_rows)} result(s)")

    #SUMMARY CARDS
    def update_summary_cards(self, rows: List[tuple]):
        matches = len(rows)

        teams: Set[str] = set()
        players: Set[str] = set()
        dates: List = []

        for r in rows:
            if r[6]:
                teams.add(str(r[6]))
            if r[7]:
                players.add(str(r[7]))
            if r[1]:
                dates.append(r[1])

        self.card_matches.value_label.setText(str(matches))
        self.card_teams.value_label.setText(str(len(teams)))
        self.card_players.value_label.setText(str(len(players)))

        if dates:
            d_min = min(dates)
            d_max = max(dates)
            self.card_daterange.value_label.setText(f"{d_min.date()} → {d_max.date()}")
        else:
            self.card_daterange.value_label.setText("—")

    #COLUMN FILTERING
    def apply_column_filters(self):
        if not self.all_rows:
            return

        filters = {
            "Matches": self.chk_matches.isChecked(),
            "Teams": self.chk_teams.isChecked(),
            "Players": self.chk_players.isChecked(),
            "Agents": self.chk_agents.isChecked(),
            "Maps": self.chk_maps.isChecked(),
            "Tournaments": self.chk_tournaments.isChecked()
        }

        active = [k for k, v in filters.items() if v]
        if not active:
            active = list(self.column_map.keys())

        indices = []
        headers = []
        for group in active:
            for idx in self.column_map[group]:
                if idx not in indices:
                    indices.append(idx)
                    headers.append(self.col_names[idx])

        self.table.clear()
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        self.table.setRowCount(len(self.all_rows))

        for r, row in enumerate(self.all_rows):
            for c, idx in enumerate(indices):
                self.table.setItem(r, c, QTableWidgetItem(str(row[idx])))

        self.table.resizeColumnsToContents()

        # fade-in animation
        self.table_effect.setOpacity(0.0)
        anim = QPropertyAnimation(self.table_effect, b"opacity", self)
        anim.setDuration(220)
        anim.setStartValue(0.0)
        anim.setEndValue(1.0)
        anim.setEasingCurve(QEasingCurve.OutQuad)
        anim.start()
        self._anim = anim

    #GRAPH BUILDER
    def open_graph_builder(self):
        from PyQt5.QtWidgets import (
            QDialog, QVBoxLayout, QComboBox, QLabel, QPushButton, QMessageBox
        )

        if self.table.rowCount() == 0 or self.table.columnCount() == 0:
            QMessageBox.warning(self, "No Data", "Run a search first.")
            return

        row_idx = self.table.currentRow()
        if row_idx < 0:
            QMessageBox.warning(self, "No Selection", "Select a row first.")
            return

        # Build context from selected row
        context = {}
        for col in range(self.table.columnCount()):
            header_item = self.table.horizontalHeaderItem(col)
            if not header_item:
                continue
            header = header_item.text()
            item = self.table.item(row_idx, col)
            context[header] = item.text() if item else None

        dialog = QDialog(self)
        dialog.setWindowTitle("Context Graph Builder")
        dialog.setFixedSize(380, 330)

        layout = QVBoxLayout(dialog)

        # Graph type
        layout.addWidget(QLabel("Graph Type"))
        graph_type = QComboBox()
        graph_type.addItems(["bar", "line", "pie"])
        layout.addWidget(graph_type)

        # X axis dropdown
        layout.addWidget(QLabel("X Axis (entity type)"))
        x_axis = QComboBox()
        x_axis.addItems([
            "Player (Total)",
            "Player per Match",
            "Player per Map",
            "Player per Round",
            "Team",
            "Agent"
        ])
        layout.addWidget(x_axis)

        # Y axis dropdown
        layout.addWidget(QLabel("Y Axis (numeric stat)"))
        y_axis = QComboBox()
        layout.addWidget(y_axis)

        # Metrics based on X-axis
        metrics_by_x = {
            "Player (Total)": [
                "Total Kills", "Total Deaths", "Total Assists",
                "Average K/D", "Average KDA",
                "Average ACS", "Average ADR", "Average HS%",
                "Total First Kills", "Total First Deaths", "Entry Rating (FK-FD)"
            ],
            "Player per Match": [
                "Total Kills", "Total Deaths", "Total Assists",
                "Average K/D", "Average KDA"
            ],
            "Player per Map": [
                "Total Kills", "Total Deaths", "Total Assists",
                "Average K/D", "Average KDA"
            ],
            "Player per Round": [
                "Total Kills", "Total Deaths", "Total Assists",
                "Average K/D", "Average KDA"
            ],
            "Team": [
                "Total Rounds Won (NYI)",
                "Total Rounds Played (NYI)"
            ],
            "Agent": [
                "Pick Count (NYI)",
                "Average ACS (NYI)",
                "Average KDA (NYI)"
            ]
        }

        # Populate Y-axis based on chosen X-axis
        def populate_y():
            y_axis.clear()
            x_choice = x_axis.currentText()
            for metric in metrics_by_x.get(x_choice, []):
                y_axis.addItem(metric)
            if y_axis.count() == 0:
                y_axis.addItem("No metrics available")

        x_axis.currentTextChanged.connect(populate_y)
        populate_y()

        # Button to generate graph
        btn = QPushButton("Generate Graph")
        layout.addWidget(btn)

        def build():
            x_choice = x_axis.currentText()
            y_choice = y_axis.currentText()
            gtype = graph_type.currentText()

            # Player-based graph types supported
            if not x_choice.startswith("Player"):
                QMessageBox.information(
                    dialog,
                    "Not Implemented",
                    f"Graphing for X axis '{x_choice}' is not implemented yet.\n"
                    "You can extend this inside generate_context_graph()."
                )
                return

            # Inject x-mode for _build_player_series()
            context["__x_mode__"] = x_choice

            # Close dialog BEFORE graphing (smooth UI)
            dialog.close()

            try:
                self.generate_context_graph(context, x_choice, y_choice, gtype)
            except Exception as e:
                QMessageBox.critical(self, "Graph Error", str(e))

        btn.clicked.connect(build)
        dialog.exec_()


    #CONTEXT GRAPH GENERATION
    def generate_context_graph(self, context: dict, x_axis: str, y_metric: str, chart_type: str):
        """
        Build a graph based on the selected row (context),
        the logical X axis (Player/Team/Map/Agent/Match),
        and the numeric Y metric.
        Currently fully implemented for X='Player'.
        """
        conn = None
        try:
            conn = connect_db()

            if x_axis.startswith("Player"):
                labels, values = self._build_player_series(conn, context, y_metric)

            else:
                # Should never reach here due to earlier guard,
                # but i keep this for safety.
                raise ValueError(f"X axis '{x_axis}' not implemented.")

            if not labels or not values:
                raise ValueError("No data available for this selection.")

            plt.figure(figsize=(8, 4))

            if chart_type == "bar":
                plt.bar(labels, values)
            elif chart_type == "line":
                plt.plot(labels, values, marker="o")
            elif chart_type == "pie":
                plt.pie(values, labels=labels, autopct="%1.1f%%")
            else:
                raise ValueError("Invalid chart type.")

            plt.title(f"{x_axis} — {y_metric}")
            plt.ylabel(y_metric)
            plt.tight_layout()
            plt.show()

        finally:
            if conn is not None:
                conn.close()

    #PLAYER SERIES BUILDER
    def _build_player_series(self, conn, context: dict, y_metric: str):
        """
        Supports all Player-based X-axis modes:
            - Player (Total)
            - Player per Match
            - Player per Map
            - Player per Round
        """

        player_name = context.get("Player")
        x_mode = context.get("__x_mode__", "Player (Total)") 

        if not player_name:
            raise ValueError("Selected row does not contain a 'Player' value.")

        cur = conn.cursor()

        # MODE 1 — PLAYER (TOTAL): Aggregate all matches together
        if x_mode == "Player (Total)":
            sql = """
            SELECT
                SUM(pm.kills),
                SUM(pm.deaths),
                SUM(pm.assists),
                AVG(CAST(ast.acs AS FLOAT)),
                AVG(CAST(ast.adr AS FLOAT)),
                AVG(CAST(ast.hs_percent AS FLOAT)),
                SUM(COALESCE(ast.first_kills,0)),
                SUM(COALESCE(ast.first_deaths,0))
            FROM PlayerMatches pm
            JOIN Players p ON p.player_id = pm.player_id
            LEFT JOIN AdvancedStats ast
                ON ast.match_map_id = pm.match_map_id AND ast.player_id = pm.player_id
            WHERE p.username = ?
            """
            cur.execute(sql, (player_name,))
            row = cur.fetchone()

            total_kills = row[0] or 0
            total_deaths = row[1] or 0
            total_assists = row[2] or 0
            avg_acs = row[3] or 0
            avg_adr = row[4] or 0
            avg_hs = row[5] or 0
            total_fk = row[6] or 0
            total_fd = row[7] or 0

            avg_kd = (total_kills / total_deaths) if total_deaths else total_kills
            avg_kda = ((total_kills + total_assists) / total_deaths) if total_deaths else (total_kills + total_assists)
            entry_rating = total_fk - total_fd

            metric_map = {
                "Total Kills": total_kills,
                "Total Deaths": total_deaths,
                "Total Assists": total_assists,
                "Average K/D": avg_kd,
                "Average KDA": avg_kda,
                "Average ACS": avg_acs,
                "Average ADR": avg_adr,
                "Average HS%": avg_hs,
                "Total First Kills": total_fk,
                "Total First Deaths": total_fd,
                "Entry Rating (FK-FD)": entry_rating
            }

            return [player_name], [metric_map[y_metric]]

        # MODE 2 — PLAYER PER MATCH: Time series per match
        if x_mode == "Player per Match":
            sql = """
            SELECT
                m.match_id,
                COALESCE(m.date_played, m.match_date),
                pm.kills, pm.deaths, pm.assists
            FROM PlayerMatches pm
            JOIN Players p ON p.player_id = pm.player_id
            JOIN Matches m ON m.match_id = pm.match_id
            WHERE p.username = ?
            ORDER BY COALESCE(m.date_played, m.match_date)
            """
            cur.execute(sql, (player_name,))
            rows = cur.fetchall()

            labels, values = [], []

            for r in rows:
                match_id = r.match_id
                kills = r.kills or 0
                deaths = r.deaths or 1
                assists = r.assists or 0

                if y_metric == "Average KDA":
                    val = (kills + assists) / deaths
                elif y_metric == "Average K/D":
                    val = kills / deaths
                else:
                    metric_map = {
                        "Total Kills": kills,
                        "Total Deaths": deaths,
                        "Total Assists": assists
                    }
                    val = metric_map.get(y_metric, 0)

                labels.append(f"Match {match_id}")
                values.append(round(val, 2))

            return labels, values

        # MODE 3 — PLAYER PER MAP: Stats grouped by map
        if x_mode == "Player per Map":
            sql = """
            SELECT
                mp.name AS map_name,
                SUM(pm.kills),
                SUM(pm.deaths),
                SUM(pm.assists)
            FROM PlayerMatches pm
            JOIN MatchMaps mm ON mm.match_map_id = pm.match_map_id
            JOIN Maps mp ON mp.map_id = mm.map_id
            JOIN Players p ON p.player_id = pm.player_id
            WHERE p.username = ?
            GROUP BY mp.name
            ORDER BY mp.name
            """
            cur.execute(sql, (player_name,))
            rows = cur.fetchall()

            labels, values = [], []

            for r in rows:
                map_name = r.map_name
                kills = r[1] or 0
                deaths = r[2] or 1
                assists = r[3] or 0

                if y_metric == "Average KDA":
                    val = (kills + assists) / deaths
                elif y_metric == "Average K/D":
                    val = kills / deaths
                else:
                    metric_map = {
                        "Total Kills": kills,
                        "Total Deaths": deaths,
                        "Total Assists": assists,
                    }
                    val = metric_map.get(y_metric, 0)

                labels.append(map_name)
                values.append(round(val, 2))

            return labels, values

        # MODE 4 — PLAYER PER ROUND: If round stats exist
        if x_mode == "Player per Round":
            sql = """
            SELECT 
                mr.round_number,
                pr.kills, pr.deaths, pr.assists
            FROM PlayerRounds pr
            JOIN MatchRounds mr ON mr.round_id = pr.round_id
            JOIN Players p ON p.player_id = pr.player_id
            WHERE p.username = ?
            ORDER BY mr.round_number
            """
            try:
                cur.execute(sql, (player_name,))
                rows = cur.fetchall()
            except:
                raise ValueError("Round-level stats table not found.")

            labels, values = [], []

            for r in rows:
                rn = r.round_number
                kills = r.kills or 0
                deaths = r.deaths or 1
                assists = r.assists or 0

                if y_metric == "Average KDA":
                    val = (kills + assists) / deaths
                elif y_metric == "Average K/D":
                    val = kills / deaths
                else:
                    metric_map = {
                        "Total Kills": kills,
                        "Total Deaths": deaths,
                        "Total Assists": assists,
                    }
                    val = metric_map.get(y_metric, 0)

                labels.append(f"R{rn}")
                values.append(round(val, 2))

            return labels, values

        # No match for X-axis mode
        raise ValueError(f"Unknown X axis mode: {x_mode}")


    #ERROR
    def show_error(self, msg):
        self.loading_overlay.hide()
        QMessageBox.critical(self, "Error", msg)
        self.status.setText("Error during search.")

    #STYLING
    def style_sheet(self):
        return """
        QWidget {
            background: qlineargradient(
                x1:0, y1:0, x2:1, y2:1,
                stop:0 #05080d,
                stop:0.5 #0b1220,
                stop:1 #101822
            );
            color: #e6e6e6;
            font-family: "Segoe UI";
        }

        QFrame {
            background: rgba(16, 26, 42, 0.78);
            border-radius: 14px;
        }

        QLineEdit {
            background: rgba(14, 22, 35, 0.92);
            border-radius: 16px;
            padding: 12px 16px;
            border: 1px solid rgba(255, 70, 85, 0.3);
            font-size: 15px;
            color: #ffffff;
        }
        QLineEdit:focus {
            border: 1px solid #ff4655;
            background: rgba(20, 30, 48, 0.98);
        }

        QPushButton {
            background: qlineargradient(
                x1:0, y1:0, x2:1, y2:0,
                stop:0 #ff4655,
                stop:1 #ff7a5a
            );
            border-radius: 16px;
            padding: 8px 16px;
            font-weight: bold;
            font-size: 14px;
            color: white;
            border: none;
        }
        QPushButton:hover {
            background: qlineargradient(
                x1:0, y1:0, x2:1, y2:0,
                stop:0 #ff7a5a,
                stop:1 #ff4655
            );
        }
        QPushButton:pressed {
            background: #cc3a45;
        }

        QTableWidget {
            background: rgba(6, 12, 20, 0.96);
            border-radius: 16px;
            border: 1px solid rgba(255,255,255,0.03);
            gridline-color: rgba(255,255,255,0.05);
            color: #f0f0f0;
            font-size: 13px;
        }
        QTableWidget::item {
            padding: 8px;
            border-bottom: 1px solid rgba(255,255,255,0.03);
        }
        QTableWidget::item:selected {
            background: rgba(255,70,85,0.35);
        }

        QHeaderView::section {
            background: rgba(18, 28, 44, 0.98);
            color: #9eb2c3;
            padding: 8px;
            border: none;
            font-weight: bold;
            font-size: 12px;
        }

        #drawer {
            background: rgba(8,12,20,0.99);
            border-right: 1px solid rgba(255,70,85,0.4);
            border-radius: 0;
        }

        QLabel {
            font-size: 14px;
        }

        QCheckBox {
            font-size: 15px;
            color: #f0f0f0;
            padding: 4px 0;
        }
        QCheckBox::indicator {
            width: 20px;
            height: 20px;
            border-radius: 6px;
            background: rgba(255,255,255,0.08);
            border: 1px solid rgba(255,255,255,0.15);
        }
        QCheckBox::indicator:checked {
            background: #ff4655;
            border: 1px solid #ff7a5a;
        }
        """

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setFont(QFont("Segoe UI", 10))
    win = ValorantSearch()
    win.show()
    sys.exit(app.exec_())
