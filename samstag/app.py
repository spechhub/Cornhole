import os
import sqlite3
import traceback
from threading import Timer
import webbrowser
import csv
import io
from collections import defaultdict
from datetime import datetime, timedelta

from flask import Flask, config, render_template, request, redirect, url_for, jsonify, send_file

# ============================================================================
# FLASK-APP INITIALISIERUNG
# ============================================================================

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
TEMPLATES_FOLDER = os.path.join(BASE_DIR, "templates")
TOURNAMENT_FOLDER = os.path.join(BASE_DIR, "tournaments")

app = Flask(__name__, template_folder=TEMPLATES_FOLDER)
app.secret_key = 'ultimate-frisbee-tournament-2025'

os.makedirs(TOURNAMENT_FOLDER, exist_ok=True)

# ============================================================================
# KONSTANTEN & MAPPING-SYSTEME
# ============================================================================

# BRACKET A MAPPINGS — Korrekte 16-Team DE Struktur
# WB: R1=8, R2=4, R3=2, R4=1 (WB Final)
# LB: R1=8, R2=8, R3=4, R4=4, R5=2, R6=2, R7=1, R8=1 (LB Final)

WINNER_MAPPING_A = {
    # WB R1 (8 Spiele) → WB R2 (4 Spiele): Sieger-Paare
    (1, 0): (2, 0, 'team1'), (1, 1): (2, 0, 'team2'),
    (1, 2): (2, 1, 'team1'), (1, 3): (2, 1, 'team2'),
    (1, 4): (2, 2, 'team1'), (1, 5): (2, 2, 'team2'),
    (1, 6): (2, 3, 'team1'), (1, 7): (2, 3, 'team2'),

    # WB R2 (4 Spiele) → WB R3 (2 Spiele)
    (2, 0): (3, 0, 'team1'), (2, 1): (3, 0, 'team2'),
    (2, 2): (3, 1, 'team1'), (2, 3): (3, 1, 'team2'),

    # WB R3 (2 Spiele) → WB R4 (WB Final, 1 Spiel)
    (3, 0): (4, 0, 'team1'), (3, 1): (4, 0, 'team2'),
}

LOSER_MAPPING_A = {
    # WB R1 Verlierer → LB R1 (8 Spiele)
    # Seeding-Kreuztabelle: Verlierer des stärkeren Matches gegen Verlierer des schwächeren
    (1, 0): (1, 0, 'team1'), (1, 1): (1, 0, 'team2'),
    (1, 2): (1, 1, 'team1'), (1, 3): (1, 1, 'team2'),
    (1, 4): (1, 2, 'team1'), (1, 5): (1, 2, 'team2'),
    (1, 6): (1, 3, 'team1'), (1, 7): (1, 3, 'team2'),

    # WB R2 Verlierer → LB R3 (als team2, 4 Spiele)
    (2, 0): (3, 0, 'team2'), (2, 1): (3, 1, 'team2'),
    (2, 2): (3, 2, 'team2'), (2, 3): (3, 3, 'team2'),

    # WB R3 Verlierer → LB R5 (als team2, 2 Spiele)
    (3, 0): (5, 0, 'team2'), (3, 1): (5, 1, 'team2'),

    # WB Final Verlierer → LB R7 (als team2, 1 Spiel)
    (4, 0): (7, 0, 'team2'),
}

LOSER_WINNER_MAPPING_A = {
    # LB R1 Sieger → LB R2 (8 Spiele, als team1)
    (1, 0): (2, 0, 'team1'), (1, 1): (2, 1, 'team1'),
    (1, 2): (2, 2, 'team1'), (1, 3): (2, 3, 'team1'),

    # LB R2 Sieger → LB R3 (4 Spiele)
    (2, 0): (3, 0, 'team1'), (2, 1): (3, 1, 'team1'),
    (2, 2): (3, 2, 'team1'), (2, 3): (3, 3, 'team1'),
    # LB R2 hat 8 Spiele (R1-Sieger + WB-R2-Verlierer), daher 4-7 auch
    (2, 4): (4, 0, 'team1'), (2, 5): (4, 1, 'team1'),
    (2, 6): (4, 2, 'team1'), (2, 7): (4, 3, 'team1'),

    # LB R3 Sieger → LB R4 (4 Spiele)
    (3, 0): (4, 0, 'team2'), (3, 1): (4, 1, 'team2'),
    (3, 2): (4, 2, 'team2'), (3, 3): (4, 3, 'team2'),

    # LB R4 Sieger → LB R5 (2 Spiele)
    (4, 0): (5, 0, 'team1'), (4, 1): (5, 0, 'team2'),
    (4, 2): (5, 1, 'team1'), (4, 3): (5, 1, 'team2'),

    # LB R5 Sieger → LB R6 (2 Spiele)
    (5, 0): (6, 0, 'team1'), (5, 1): (6, 0, 'team2'),

    # LB R6 Sieger → LB R7 (1 Spiel, als team1)
    (6, 0): (7, 0, 'team1'),

    # LB R7 Sieger → LB R8 (LB Final, als team1)
    (7, 0): (8, 0, 'team1'),
}

# BRACKET B MAPPINGS (identisch zu A)
WINNER_MAPPING_B = WINNER_MAPPING_A.copy()
LOSER_MAPPING_B = LOSER_MAPPING_A.copy()
LOSER_WINNER_MAPPING_B = LOSER_WINNER_MAPPING_A.copy()

# SUPER FINALS MAPPINGS
SUPER_FINALS_QUALIFICATION = {
    ('A', 1): ('HF1', 'team1'),
    ('A', 2): ('HF2', 'team2'),
    ('B', 1): ('HF2', 'team1'),
    ('B', 2): ('HF1', 'team2')
}

SUPER_FINALS_PROGRESSION = {
    ('HF1', 'winner'): ('FINAL', 'team1'),
    ('HF1', 'loser'): ('THIRD', 'team1'),
    ('HF2', 'winner'): ('FINAL', 'team2'),
    ('HF2', 'loser'): ('THIRD', 'team2')
}

# GHOST TEAM NAMEN
GHOST_TEAM_NAMES = [
    "Ghost Team Alpha", "Ghost Team Beta", "Ghost Team Gamma", "Ghost Team Delta",
    "Ghost Team Epsilon", "Ghost Team Zeta", "Ghost Team Eta", "Ghost Team Theta",
    "Phantom Squad 1", "Phantom Squad 2", "Phantom Squad 3", "Phantom Squad 4",
    "Invisible Warriors", "Shadow Players", "Bye Team Red", "Bye Team Blue",
    "Bye Team Green", "Bye Team Yellow", "Placeholder United 1", "Placeholder United 2",
    "Dummy FC Alpha", "Dummy FC Beta", "Specter Team 1", "Specter Team 2",
    "Wraith Squad", "Apparition Team", "Spirit Squad", "Ectoplasm United",
    "Ghostly Guardians", "Phantom Force", "Shadow Brigade", "Invisible Legion",
    "Bye Team Orange", "Bye Team Purple", "Bye Team Pink", "Bye Team Cyan",
    "Ghost Riders", "Phantom Menace", "Shadow Syndicate", "Invisible Infantry",
    "Specter Squadron", "Wraith Warriors", "Apparition Army", "Ghost Battalion",
    "Phantom Patrol", "Shadow Strikers", "Invisible Invaders", "Ghost Guard",
    "Phantom Phalanx", "Shadow Squad", "Invisible Unit", "Ghost Group",
    "Phantom Platoon", "Shadow Soldiers", "Invisible Team", "Ghost Gang",
    "Phantom Players", "Shadow Side", "Invisible XI", "Ghost Eleven"
]


# ============================================================================
# DATENBANK-FUNKTIONEN
# ============================================================================

def get_db_connection(db_path):
    """Stellt eine Verbindung zur SQLite-Datenbank her"""
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db(db_path):
    """Initialisiert alle Datenbanktabellen für ein neues Turnier"""
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            group_number INTEGER,
            is_ghost INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_number INTEGER UNIQUE,
            round INTEGER NOT NULL,
            team1 TEXT,
            team2 TEXT,
            group_number INTEGER NOT NULL,
            field INTEGER DEFAULT NULL,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            time TEXT DEFAULT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rankings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team TEXT NOT NULL,
            group_number INTEGER NOT NULL,
            matches_played INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            draws INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            goals_for INTEGER DEFAULT 0,
            goals_against INTEGER DEFAULT 0,
            goal_difference INTEGER DEFAULT 0,
            points INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS double_elim_matches_a (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_number INTEGER UNIQUE,
            round INTEGER NOT NULL,
            bracket TEXT NOT NULL,
            match_index INTEGER NOT NULL,
            team1 TEXT,
            team2 TEXT,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            winner TEXT DEFAULT NULL,
            loser TEXT DEFAULT NULL,
            court INTEGER DEFAULT NULL,
            time TEXT DEFAULT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS double_elim_matches_b (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_number INTEGER UNIQUE,
            round INTEGER NOT NULL,
            bracket TEXT NOT NULL,
            match_index INTEGER NOT NULL,
            team1 TEXT,
            team2 TEXT,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            winner TEXT DEFAULT NULL,
            loser TEXT DEFAULT NULL,
            court INTEGER DEFAULT NULL,
            time TEXT DEFAULT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS super_finals_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_number INTEGER UNIQUE,
            match_id TEXT NOT NULL UNIQUE,
            team1 TEXT,
            team2 TEXT,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            winner TEXT DEFAULT NULL,
            court INTEGER DEFAULT NULL,
            time TEXT DEFAULT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS follower_quali_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_number INTEGER UNIQUE,
            team1 TEXT,
            team2 TEXT,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            winner TEXT DEFAULT NULL,
            court INTEGER DEFAULT NULL,
            time TEXT DEFAULT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS follower_cup_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_number INTEGER UNIQUE,
            round TEXT NOT NULL,
            match_index INTEGER NOT NULL,
            team1 TEXT,
            team2 TEXT,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            winner TEXT DEFAULT NULL,
            court INTEGER DEFAULT NULL,
            time TEXT DEFAULT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS placement_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_number INTEGER UNIQUE,
            placement TEXT NOT NULL,
            team1 TEXT,
            team2 TEXT,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            winner TEXT DEFAULT NULL,
            court INTEGER DEFAULT NULL,
            time TEXT DEFAULT NULL
        )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tournament_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_name TEXT UNIQUE,
        match_duration INTEGER DEFAULT 12,
        break_between_games INTEGER DEFAULT 3,
        start_time TEXT DEFAULT '09:00',
        lunch_break_enabled INTEGER DEFAULT 0,
        lunch_break_start TEXT DEFAULT '12:00',
        lunch_break_end TEXT DEFAULT '13:00',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()
    print(f"✅ Datenbank initialisiert: {db_path}")


def upgrade_database(db_path):
    """Datenbank-Migration für bestehende Datenbanken"""
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("PRAGMA table_info(teams)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'is_ghost' not in columns:
            cursor.execute("ALTER TABLE teams ADD COLUMN is_ghost INTEGER DEFAULT 0")
            print("✅ Spalte 'is_ghost' hinzugefügt")
        
        cursor.execute("PRAGMA table_info(matches)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'match_number' not in columns:
            cursor.execute("ALTER TABLE matches ADD COLUMN match_number INTEGER UNIQUE")
            print("✅ Spalte 'match_number' hinzugefügt")
        
        # Alle anderen Tabellen erstellen (falls nicht vorhanden)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS double_elim_matches_a (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_number INTEGER UNIQUE,
                round INTEGER NOT NULL,
                bracket TEXT NOT NULL,
                match_index INTEGER NOT NULL,
                team1 TEXT, team2 TEXT,
                score1 INTEGER, score2 INTEGER,
                winner TEXT, loser TEXT,
                court INTEGER, time TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS double_elim_matches_b (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_number INTEGER UNIQUE,
                round INTEGER NOT NULL,
                bracket TEXT NOT NULL,
                match_index INTEGER NOT NULL,
                team1 TEXT, team2 TEXT,
                score1 INTEGER, score2 INTEGER,
                winner TEXT, loser TEXT,
                court INTEGER, time TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS super_finals_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_number INTEGER UNIQUE,
                match_id TEXT NOT NULL UNIQUE,
                team1 TEXT, team2 TEXT,
                score1 INTEGER, score2 INTEGER,
                winner TEXT, court INTEGER, time TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS follower_quali_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_number INTEGER UNIQUE,
                team1 TEXT, team2 TEXT,
                score1 INTEGER, score2 INTEGER,
                winner TEXT, court INTEGER, time TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS follower_cup_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_number INTEGER UNIQUE,
                round TEXT NOT NULL,
                match_index INTEGER NOT NULL,
                team1 TEXT, team2 TEXT,
                score1 INTEGER, score2 INTEGER,
                winner TEXT, court INTEGER, time TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS placement_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_number INTEGER UNIQUE,
                placement TEXT NOT NULL,
                team1 TEXT, team2 TEXT,
                score1 INTEGER, score2 INTEGER,
                winner TEXT, court INTEGER, time TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tournament_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_name TEXT UNIQUE,
                match_duration INTEGER DEFAULT 12,
                break_between_games INTEGER DEFAULT 3,
                break_between_rounds INTEGER DEFAULT 5,
                start_time TEXT DEFAULT '09:00',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # MITTAGSPAUSE-SPALTEN HINZUFÜGEN
        cursor.execute("PRAGMA table_info(tournament_config)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'lunch_break_enabled' not in columns:
            cursor.execute("ALTER TABLE tournament_config ADD COLUMN lunch_break_enabled INTEGER DEFAULT 0")
            print("✅ Spalte 'lunch_break_enabled' hinzugefügt")
        
        if 'lunch_break_start' not in columns:
            cursor.execute("ALTER TABLE tournament_config ADD COLUMN lunch_break_start TEXT DEFAULT '12:00'")
            print("✅ Spalte 'lunch_break_start' hinzugefügt")
        
        if 'lunch_break_end' not in columns:
            cursor.execute("ALTER TABLE tournament_config ADD COLUMN lunch_break_end TEXT DEFAULT '13:00'")
            print("✅ Spalte 'lunch_break_end' hinzugefügt")
        
        conn.commit()
        print("✅ Datenbank-Upgrade erfolgreich")
    
    except Exception as e:
        print(f"⚠️ Warnung bei Upgrade: {e}")
        conn.rollback()
    
    finally:
        conn.close()

# ============================================================================
# HILFSFUNKTIONEN
# ============================================================================

def get_qualified_teams_for_bracket(conn, group_start, group_end):
    """
    Holt qualifizierte Teams für ein Bracket.
    Logik: Top 3 aus jeder Gruppe (5 Gruppen = 15 Teams) + bester 4. Platz = 16 Teams.
    Ghost-Teams werden ausgeschlossen.
    """
    cursor = conn.cursor()

    groups = {}
    fourths = []

    for group_num in range(group_start, group_end + 1):
        cursor.execute("""
            SELECT r.team, r.points, r.goal_difference, r.goals_for
            FROM rankings r
            WHERE r.group_number = ?
            AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
            ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC
        """, (group_num,))
        rows = cursor.fetchall()
        top3 = [r['team'] for r in rows[:3]]
        groups[group_num] = top3

        if len(rows) >= 4:
            fourth = rows[3]
            fourths.append({
                'team': fourth['team'],
                'points': fourth['points'],
                'goal_difference': fourth['goal_difference'],
                'goals_for': fourth['goals_for']
            })

    qualified = []
    for group_num in range(group_start, group_end + 1):
        qualified.extend(groups.get(group_num, []))

    # Bester 4. Platz als 16. Team
    if fourths:
        best_fourth = sorted(
            fourths,
            key=lambda x: (-x['points'], -x['goal_difference'], -x['goals_for'])
        )[0]
        qualified.append(best_fourth['team'])

    return qualified[:16]


def process_double_elim_forwarding(conn, match_row, bracket_table, winner_mapping, 
                                   loser_mapping, loser_winner_mapping):
    """Automatische Weiterleitung nach Spielende"""
    cursor = conn.cursor()
    
    round_num = match_row['round']
    bracket_type = match_row['bracket']
    match_index = match_row['match_index']
    winner = match_row['winner']
    loser = match_row['loser']
    
    if not winner or not loser:
        return
    
    if bracket_type == 'Winners':
        key = (round_num, match_index)
        if key in winner_mapping:
            next_round, next_index, slot = winner_mapping[key]
            cursor.execute(f"""
                UPDATE {bracket_table}
                SET {slot} = ?
                WHERE round = ? AND bracket = 'Winners' AND match_index = ?
            """, (winner, next_round, next_index))
        
        if key in loser_mapping:
            loser_round, loser_index, loser_slot = loser_mapping[key]
            cursor.execute(f"""
                UPDATE {bracket_table}
                SET {loser_slot} = ?
                WHERE round = ? AND bracket = 'Losers' AND match_index = ?
            """, (loser, loser_round, loser_index))
    
    elif bracket_type == 'Losers':
        key = (round_num, match_index)
        if key in loser_winner_mapping:
            next_round, next_index, slot = loser_winner_mapping[key]
            cursor.execute(f"""
                UPDATE {bracket_table}
                SET {slot} = ?
                WHERE round = ? AND bracket = 'Losers' AND match_index = ?
            """, (winner, next_round, next_index))
    
    conn.commit()

"""
SPIELNUMMERN-SYSTEM FÜR TURNIER-MANAGEMENT
===========================================

Dieses Modul implementiert die automatische Vergabe von durchlaufenden
Spielnummern (#1-245+) für alle Turnierphasen.

NUMMERIERUNGSSYSTEM:
- Round Robin:           #1 - #150 (10 Gruppen × 5 Runden × 3 Spiele)
- Bracket A Double Elim: #151 - #181 (ca. 31 Spiele)
- Bracket B Double Elim: #182 - #212 (ca. 31 Spiele)
- Super Finals:          #213 - #216 (4 Spiele)
- Follower Cup:          #217 - #245 (ca. 29 Spiele)
- Platzierungsrunde:     #246+ (ca. 14 Spiele)

WICHTIG: Diese Funktionen müssen in generate_matches(), generate_double_elim()
und andere Generierungs-Funktionen integriert werden.
"""

import sqlite3


# ============================================================================
# HILFSFUNKTIONEN FÜR SPIELNUMMERN
# ============================================================================

def get_next_match_number(cursor):
    """
    Ermittelt die nächste verfügbare Spielnummer über alle Tabellen hinweg.
    
    Returns:
        int: Nächste freie Spielnummer
    """
    max_numbers = []
    
    # Round Robin
    cursor.execute("SELECT MAX(match_number) as max_num FROM matches")
    row = cursor.fetchone()
    if row and row['max_num']:
        max_numbers.append(row['max_num'])
    
    # Double Elim A
    cursor.execute("SELECT MAX(match_number) as max_num FROM double_elim_matches_a")
    row = cursor.fetchone()
    if row and row['max_num']:
        max_numbers.append(row['max_num'])
    
    # Double Elim B
    cursor.execute("SELECT MAX(match_number) as max_num FROM double_elim_matches_b")
    row = cursor.fetchone()
    if row and row['max_num']:
        max_numbers.append(row['max_num'])
    
    # Super Finals
    cursor.execute("SELECT MAX(match_number) as max_num FROM super_finals_matches")
    row = cursor.fetchone()
    if row and row['max_num']:
        max_numbers.append(row['max_num'])
    
    # Follower Cup Quali
    cursor.execute("SELECT MAX(match_number) as max_num FROM follower_quali_matches")
    row = cursor.fetchone()
    if row and row['max_num']:
        max_numbers.append(row['max_num'])
    
    # Follower Cup
    cursor.execute("SELECT MAX(match_number) as max_num FROM follower_cup_matches")
    row = cursor.fetchone()
    if row and row['max_num']:
        max_numbers.append(row['max_num'])
    
    # Placement
    cursor.execute("SELECT MAX(match_number) as max_num FROM placement_matches")
    row = cursor.fetchone()
    if row and row['max_num']:
        max_numbers.append(row['max_num'])
    
    if max_numbers:
        return max(max_numbers) + 1
    else:
        return 1


def assign_round_robin_match_numbers(conn):
    """
    Vergibt Spielnummern #1-#150 für Round Robin Matches.
    
    Nummerierung erfolgt nach:
    1. Runde (1-5)
    2. Gruppe (1-10)
    3. Match innerhalb der Gruppe
    
    Args:
        conn: SQLite Connection
    """
    cursor = conn.cursor()
    
    match_number = 1
    
    # Sortierung: Runde → Gruppe → ID
    cursor.execute("""
        SELECT id FROM matches 
        ORDER BY round ASC, group_number ASC, id ASC
    """)
    
    matches = cursor.fetchall()
    
    for match in matches:
        conn.execute("""
            UPDATE matches 
            SET match_number = ?
            WHERE id = ?
        """, (match_number, match['id']))
        match_number += 1
    
    conn.commit()
    print(f"✅ Round Robin: {match_number - 1} Spielnummern vergeben (#1-#{match_number - 1})")
    
    return match_number  # Nächste verfügbare Nummer


def assign_double_elim_match_numbers_a(conn, start_number=151):
    """
    Vergibt Spielnummern für Bracket A Double Elimination.
    Standard: #151-#181
    
    Args:
        conn: SQLite Connection
        start_number: Startnummer (default: 151)
    
    Returns:
        int: Nächste verfügbare Spielnummer
    """
    cursor = conn.cursor()
    
    match_number = start_number
    
    # Winner Bracket: Runde 1-4
    for round_num in range(1, 5):
        cursor.execute("""
            SELECT id FROM double_elim_matches_a 
            WHERE round = ? AND bracket = 'Winners'
            ORDER BY match_index ASC
        """, (round_num,))
        
        for match in cursor.fetchall():
            conn.execute("""
                UPDATE double_elim_matches_a 
                SET match_number = ?
                WHERE id = ?
            """, (match_number, match['id']))
            match_number += 1
    
    # Loser Bracket: Runde 1-6
    for round_num in range(1, 7):
        cursor.execute("""
            SELECT id FROM double_elim_matches_a 
            WHERE round = ? AND bracket = 'Losers'
            ORDER BY match_index ASC
        """, (round_num,))
        
        for match in cursor.fetchall():
            conn.execute("""
                UPDATE double_elim_matches_a 
                SET match_number = ?
                WHERE id = ?
            """, (match_number, match['id']))
            match_number += 1
    
    conn.commit()
    print(f"✅ Bracket A: {match_number - start_number} Spielnummern vergeben (#{start_number}-#{match_number - 1})")
    
    return match_number


def assign_double_elim_match_numbers_b(conn, start_number=182):
    """
    Vergibt Spielnummern für Bracket B Double Elimination.
    Standard: #182-#212
    
    Args:
        conn: SQLite Connection
        start_number: Startnummer (default: 182)
    
    Returns:
        int: Nächste verfügbare Spielnummer
    """
    cursor = conn.cursor()
    
    match_number = start_number
    
    # Winner Bracket: Runde 1-4
    for round_num in range(1, 5):
        cursor.execute("""
            SELECT id FROM double_elim_matches_b 
            WHERE round = ? AND bracket = 'Winners'
            ORDER BY match_index ASC
        """, (round_num,))
        
        for match in cursor.fetchall():
            conn.execute("""
                UPDATE double_elim_matches_b 
                SET match_number = ?
                WHERE id = ?
            """, (match_number, match['id']))
            match_number += 1
    
    # Loser Bracket: Runde 1-6
    for round_num in range(1, 7):
        cursor.execute("""
            SELECT id FROM double_elim_matches_b 
            WHERE round = ? AND bracket = 'Losers'
            ORDER BY match_index ASC
        """, (round_num,))
        
        for match in cursor.fetchall():
            conn.execute("""
                UPDATE double_elim_matches_b 
                SET match_number = ?
                WHERE id = ?
            """, (match_number, match['id']))
            match_number += 1
    
    conn.commit()
    print(f"✅ Bracket B: {match_number - start_number} Spielnummern vergeben (#{start_number}-#{match_number - 1})")
    
    return match_number


def assign_super_finals_match_numbers(conn, start_number=213):
    """
    Vergibt Spielnummern für Super Finals.
    Standard: #213-#216 (4 Spiele)
    
    Args:
        conn: SQLite Connection
        start_number: Startnummer (default: 213)
    
    Returns:
        int: Nächste verfügbare Spielnummer
    """
    cursor = conn.cursor()
    
    match_ids = ['HF1', 'HF2', 'THIRD', 'FINAL']
    match_number = start_number
    
    for match_id in match_ids:
        cursor.execute("""
            UPDATE super_finals_matches 
            SET match_number = ?
            WHERE match_id = ?
        """, (match_number, match_id))
        match_number += 1
    
    conn.commit()
    print(f"✅ Super Finals: {match_number - start_number} Spielnummern vergeben (#{start_number}-#{match_number - 1})")
    
    return match_number


def assign_follower_cup_match_numbers(conn, start_number=217):
    """
    Vergibt Spielnummern für Follower Cup.
    Standard: #217-#245 (ca. 29 Spiele)
    
    Reihenfolge:
    1. Qualifikationsspiele
    2. 1/8-Finale (eighth)
    3. 1/4-Finale (quarter)
    4. Halbfinale (semi)
    5. Spiel um Platz 3 (third)
    6. Finale (final)
    
    Args:
        conn: SQLite Connection
        start_number: Startnummer (default: 217)
    
    Returns:
        int: Nächste verfügbare Spielnummer
    """
    cursor = conn.cursor()
    
    match_number = start_number
    
    # Qualifikationsspiele
    cursor.execute("""
        SELECT id FROM follower_quali_matches 
        ORDER BY id ASC
    """)
    
    for match in cursor.fetchall():
        conn.execute("""
            UPDATE follower_quali_matches 
            SET match_number = ?
            WHERE id = ?
        """, (match_number, match['id']))
        match_number += 1
    
    quali_count = match_number - start_number
    
    # Cup-Runden in korrekter Reihenfolge
    rounds = ['eighth', 'quarter', 'semi', 'third', 'final']
    
    for round_name in rounds:
        cursor.execute("""
            SELECT id FROM follower_cup_matches 
            WHERE round = ?
            ORDER BY match_index ASC
        """, (round_name,))
        
        for match in cursor.fetchall():
            conn.execute("""
                UPDATE follower_cup_matches 
                SET match_number = ?
                WHERE id = ?
            """, (match_number, match['id']))
            match_number += 1
    
    conn.commit()
    print(f"✅ Follower Cup: {match_number - start_number} Spielnummern vergeben (#{start_number}-#{match_number - 1})")
    print(f"   - Qualifikation: {quali_count} Spiele")
    print(f"   - Cup-System: {match_number - start_number - quali_count} Spiele")
    
    return match_number


def assign_placement_match_numbers(conn, start_number=246):
    """
    Vergibt Spielnummern für Platzierungsrunde.
    Standard: #246+ (ca. 14 Spiele)
    
    Args:
        conn: SQLite Connection
        start_number: Startnummer (default: 246)
    
    Returns:
        int: Nächste verfügbare Spielnummer
    """
    cursor = conn.cursor()
    
    match_number = start_number
    
    cursor.execute("""
        SELECT id FROM placement_matches 
        ORDER BY placement ASC, id ASC
    """)
    
    for match in cursor.fetchall():
        conn.execute("""
            UPDATE placement_matches 
            SET match_number = ?
            WHERE id = ?
        """, (match_number, match['id']))
        match_number += 1
    
    conn.commit()
    print(f"✅ Platzierungsrunde: {match_number - start_number} Spielnummern vergeben (#{start_number}-#{match_number - 1})")
    
    return match_number


# ============================================================================
# HAUPTFUNKTION: ALLE SPIELNUMMERN VERGEBEN
# ============================================================================

def assign_all_match_numbers(conn):
    """
    Vergibt Spielnummern für ALLE Turnierphasen in korrekter Reihenfolge.
    
    Diese Funktion sollte aufgerufen werden:
    1. Nach Round Robin Generierung
    2. Nach Double Elimination Generierung
    3. Nach Super Finals Generierung
    4. Nach Follower Cup Generierung
    5. Nach Platzierungsrunden-Generierung
    
    ODER einmal am Ende, wenn alle Matches generiert wurden.
    
    Args:
        conn: SQLite Connection
    
    Returns:
        dict: Statistik über vergebene Nummern
    """
    cursor = conn.cursor()
    
    print("\n" + "=" * 70)
    print("🔢 SPIELNUMMERN-VERGABE STARTET")
    print("=" * 70)
    
    stats = {}
    
    # 1. Round Robin (#1-#150)
    cursor.execute("SELECT COUNT(*) as count FROM matches")
    if cursor.fetchone()['count'] > 0:
        next_num = assign_round_robin_match_numbers(conn)
        stats['round_robin'] = {'start': 1, 'end': next_num - 1, 'count': next_num - 1}
    else:
        next_num = 1
        stats['round_robin'] = {'start': 0, 'end': 0, 'count': 0}
    
    # 2. Bracket A Double Elim (#151-#181)
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches_a")
    if cursor.fetchone()['count'] > 0:
        start = next_num
        next_num = assign_double_elim_match_numbers_a(conn, next_num)
        stats['bracket_a'] = {'start': start, 'end': next_num - 1, 'count': next_num - start}
    else:
        stats['bracket_a'] = {'start': 0, 'end': 0, 'count': 0}
    
    # 3. Bracket B Double Elim (#182-#212)
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches_b")
    if cursor.fetchone()['count'] > 0:
        start = next_num
        next_num = assign_double_elim_match_numbers_b(conn, next_num)
        stats['bracket_b'] = {'start': start, 'end': next_num - 1, 'count': next_num - start}
    else:
        stats['bracket_b'] = {'start': 0, 'end': 0, 'count': 0}
    
    # 4. Super Finals (#213-#216)
    cursor.execute("SELECT COUNT(*) as count FROM super_finals_matches")
    if cursor.fetchone()['count'] > 0:
        start = next_num
        next_num = assign_super_finals_match_numbers(conn, next_num)
        stats['super_finals'] = {'start': start, 'end': next_num - 1, 'count': next_num - start}
    else:
        stats['super_finals'] = {'start': 0, 'end': 0, 'count': 0}
    
    # 5. Follower Cup (#217-#245)
    cursor.execute("SELECT COUNT(*) as count FROM follower_quali_matches")
    quali_count = cursor.fetchone()['count']
    cursor.execute("SELECT COUNT(*) as count FROM follower_cup_matches")
    cup_count = cursor.fetchone()['count']
    
    if quali_count > 0 or cup_count > 0:
        start = next_num
        next_num = assign_follower_cup_match_numbers(conn, next_num)
        stats['follower_cup'] = {'start': start, 'end': next_num - 1, 'count': next_num - start}
    else:
        stats['follower_cup'] = {'start': 0, 'end': 0, 'count': 0}
    
    # 6. Platzierungsrunde (#246+)
    cursor.execute("SELECT COUNT(*) as count FROM placement_matches")
    if cursor.fetchone()['count'] > 0:
        start = next_num
        next_num = assign_placement_match_numbers(conn, next_num)
        stats['placement'] = {'start': start, 'end': next_num - 1, 'count': next_num - start}
    else:
        stats['placement'] = {'start': 0, 'end': 0, 'count': 0}
    
    # Zusammenfassung
    total_matches = sum(phase['count'] for phase in stats.values())
    
    print("\n" + "=" * 70)
    print("📊 ZUSAMMENFASSUNG")
    print("=" * 70)
    
    for phase_name, data in stats.items():
        if data['count'] > 0:
            print(f"   {phase_name.upper()}: #{data['start']}-#{data['end']} ({data['count']} Spiele)")
    
    print(f"\n   GESAMT: {total_matches} Spiele nummeriert")
    print("=" * 70 + "\n")
    
    return stats


# ============================================================================
# RESET-FUNKTION (für Tests)
# ============================================================================

def reset_all_match_numbers(conn):
    """
    Setzt alle Spielnummern zurück (NULL).
    Nützlich für Tests oder Neugenerierung.
    
    Args:
        conn: SQLite Connection
    """
    cursor = conn.cursor()
    
    cursor.execute("UPDATE matches SET match_number = NULL")
    cursor.execute("UPDATE double_elim_matches_a SET match_number = NULL")
    cursor.execute("UPDATE double_elim_matches_b SET match_number = NULL")
    cursor.execute("UPDATE super_finals_matches SET match_number = NULL")
    cursor.execute("UPDATE follower_quali_matches SET match_number = NULL")
    cursor.execute("UPDATE follower_cup_matches SET match_number = NULL")
    cursor.execute("UPDATE placement_matches SET match_number = NULL")
    
    conn.commit()
    print("✅ Alle Spielnummern zurückgesetzt")


# ============================================================================
# INTEGRATION IN BESTEHENDEN CODE
# ============================================================================

"""
INTEGRATION IN app.py:
======================

1. Import am Anfang von app.py hinzufügen:
   
   from match_numbering import assign_all_match_numbers, assign_round_robin_match_numbers

2. In generate_matches() nach conn.commit() einfügen:
   
   assign_round_robin_match_numbers(conn)

3. In generate_double_elim() nach allen Inserts einfügen:
   
   # Nach Bracket A Generierung
   assign_double_elim_match_numbers_a(conn, 151)
   
   # Nach Bracket B Generierung
   assign_double_elim_match_numbers_b(conn, 182)

4. ODER: Einmal am Ende aller Generierungen aufrufen:
   
   assign_all_match_numbers(conn)

5. Für manuelle Neunummerierung eine Route erstellen:
   
   @app.route('/renumber_matches/<game_name>', methods=['POST'])
   def renumber_matches(game_name):
       db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
       conn = get_db_connection(db_path)
       stats = assign_all_match_numbers(conn)
       conn.close()
       return jsonify({"success": True, "stats": stats})
"""


# ============================================================================
# BEISPIEL-VERWENDUNG
# ============================================================================

if __name__ == '__main__':
    print("🔢 SPIELNUMMERN-SYSTEM TEST")
    print("=" * 70)
    print()
    print("Dieses Modul ist für die Integration in app.py gedacht.")
    print("Kopiere die Funktionen in app.py oder importiere sie.")
    print()
    print("Hauptfunktionen:")
    print("  - assign_all_match_numbers(conn)")
    print("  - assign_round_robin_match_numbers(conn)")
    print("  - assign_double_elim_match_numbers_a(conn, start)")
    print("  - assign_double_elim_match_numbers_b(conn, start)")
    print()
    print("Siehe Integrations-Anleitung am Ende der Datei.")
    print("=" * 70)



"""
AUTOMATISCHE ZEITBERECHNUNG FÜR TURNIER-MANAGEMENT
===================================================

Dieses Modul implementiert die automatische Berechnung von Spielzeiten
basierend auf konfigurierbaren Parametern.

ZEITKONFIGURATION (pro Turnier):
- Spieldauer: 8, 10, 12, 15, 20 Minuten (default: 12)
- Pause zwischen Spielen: 2, 3, 4, 5, 10 Minuten (default: 3)
- Pause zwischen Runden: 5, 10, 15, 20 Minuten (default: 5)
- Turnier-Startzeit: z.B. 09:00 Uhr

ZEITBERECHNUNG:
- Parallele Spiele (Round Robin): 3 Spiele pro Gruppe gleichzeitig
- Double Elimination: Variable Anzahl paralleler Felder
- Automatische Pausen zwischen Spielen und Runden

WICHTIG: Diese Funktionen werden in generate_matches() und anderen
Generierungs-Funktionen integriert.
"""

import sqlite3
from datetime import datetime, timedelta


# ============================================================================
# HILFSFUNKTIONEN FÜR ZEITBERECHNUNG
# ============================================================================

def get_tournament_config(conn):
    """
    Holt die Zeitkonfiguration für das Turnier aus der Datenbank.

    Returns:
        dict: Konfiguration mit match_duration, break_between_games, etc.
              Falls nicht vorhanden, werden Defaults verwendet.
    """
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM tournament_config LIMIT 1")
    config = cursor.fetchone()

    if config:
        return {
            'match_duration': config['match_duration'],
            'break_between_games': config['break_between_games'],
            'break_between_rounds': config['break_between_rounds'],
            'start_time': config['start_time']
        }
    else:
        # Defaults falls keine Config vorhanden
        return {
            'match_duration': 12,
            'break_between_games': 3,
            'break_between_rounds': 5,
            'start_time': '09:00'
        }


def parse_time(time_str):
    """
    Konvertiert Zeit-String in datetime.time Objekt.

    Args:
        time_str: Zeit als String (z.B. "09:00", "13:45")

    Returns:
        datetime.time: Zeit-Objekt
    """
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except ValueError:
        # Fallback auf 09:00 wenn ungültiges Format
        return datetime.strptime("09:00", "%H:%M").time()


def format_time(dt):
    """
    Formatiert datetime zu Zeit-String.

    Args:
        dt: datetime Objekt

    Returns:
        str: Zeit als "HH:MM"
    """
    return dt.strftime("%H:%M")


def add_minutes_to_time(time_str, minutes):
    """
    Addiert Minuten zu einer Zeit.

    Args:
        time_str: Startzeit als String (z.B. "09:00")
        minutes: Minuten zum Addieren

    Returns:
        str: Neue Zeit als "HH:MM"
    """
    base = datetime.strptime(time_str, "%H:%M")
    new_time = base + timedelta(minutes=minutes)
    return format_time(new_time)


# ============================================================================
# ROUND ROBIN ZEITBERECHNUNG
# ============================================================================


def check_and_insert_lunch_break(current_time, match_duration, lunch_start, lunch_end):
    """
    Prüft ob ein Match in die Mittagspause fallen würde und verschiebt es danach.
    """
    from datetime import datetime, timedelta

    current_dt = datetime.strptime(current_time, "%H:%M")
    lunch_start_dt = datetime.strptime(lunch_start, "%H:%M")
    lunch_end_dt = datetime.strptime(lunch_end, "%H:%M")

    match_end_dt = current_dt + timedelta(minutes=match_duration)

    # Prüfe ob Match in Mittagspause fällt
    if current_dt < lunch_start_dt and match_end_dt > lunch_start_dt:
        print(f"       ⚠️  Mittagspause! Verschiebe von {current_time} auf {lunch_end}")
        return lunch_end

    if lunch_start_dt <= current_dt < lunch_end_dt:
        print(f"       ⚠️  In Mittagspause! Verschiebe von {current_time} auf {lunch_end}")
        return lunch_end

    return current_time


def calculate_round_robin_times(conn):
    """
    Berechnet Spielzeiten für Round Robin mit PARALLELEN SPIELEN.

    LOGIK:
    - Alle Spiele einer Runde/Bracket spielen PARALLEL
    - 10 Gruppen → 2 Brackets (A: 1-5, B: 6-10)
    - Pro Runde: Erst alle A Gruppen parallel, Pause, dann alle B Gruppen parallel
    """
    cursor = conn.cursor()

    # Config aus DB holen
    cursor.execute("SELECT * FROM tournament_config LIMIT 1")
    config = cursor.fetchone()

    if not config:
        print("❌ Keine Turnier-Konfiguration gefunden!")
        return 0

    match_duration = config["match_duration"]
    pause_between_games = config["break_between_games"]
    try:
        pause_between_rounds = config["break_between_rounds"]
    except (KeyError, TypeError, IndexError):
        pause_between_rounds = pause_between_games
    start_time = config["start_time"]


    # Mittagspause
    try:
        lunch_enabled = config['lunch_break_enabled'] == 1
        lunch_start = config['lunch_break_start']
        lunch_end = config['lunch_break_end']
    except (KeyError, TypeError):
    # Falls Spalten nicht existieren (alte DB)
        lunch_enabled = False
        lunch_start = '12:00'
        lunch_end = '13:00'

    print("\n" + "=" * 70)
    print("⏰ ZEITBERECHNUNG ROUND ROBIN (PARALLELE SPIELE)")
    print("=" * 70)
    print(f"   Spieldauer: {match_duration} Min")
    print(f"   Pause Spiele (A→B): {pause_between_games} Min | Pause Runden (B→A): {pause_between_rounds} Min")
    print(f"   Startzeit: {start_time}")
    if lunch_enabled:
        print(f"   Mittagspause: {lunch_start} - {lunch_end}")
    print("=" * 70)

    current_time = start_time
    updated_count = 0

    # 5 Runden durchgehen
    for round_num in range(1, 6):
        print(f"\n📍 RUNDE {round_num}")
    
        # BRACKET A (Gruppen 1-5) - ALLE PARALLEL
        print(f"   Bracket A (Gruppen 1-5):")
    
        # Mittagspause prüfen
        if lunch_enabled:
            current_time = check_and_insert_lunch_break(
                current_time, match_duration, lunch_start, lunch_end
            )
    
        print(f"      Startzeit: {current_time}")
    
        # Alle Matches von Runde X, Gruppen 1-5 bekommen die GLEICHE Zeit
        cursor.execute("""
            SELECT id FROM matches 
            WHERE round = ? AND group_number BETWEEN 1 AND 5
            ORDER BY group_number
        """, (round_num,))
    
        bracket_a_matches = cursor.fetchall()
    
        for match in bracket_a_matches:
            cursor.execute("UPDATE matches SET time = ? WHERE id = ?", 
                         (current_time, match['id']))
            updated_count += 1
    
        print(f"      → {len(bracket_a_matches)} Matches parallel")
    
        # Zeit vorrücken: Spieldauer + Pause
        # Zeit vorrücken nach Bracket A: Spieldauer + Pause A→B (=break_between_games)
        current_time = add_minutes_to_time(current_time, match_duration + pause_between_games)
    
        # BRACKET B (Gruppen 6-10) - ALLE PARALLEL
        print(f"   Bracket B (Gruppen 6-10):")
    
        # Mittagspause prüfen
        if lunch_enabled:
            current_time = check_and_insert_lunch_break(
                current_time, match_duration, lunch_start, lunch_end
            )
    
        print(f"      Startzeit: {current_time}")
    
        # Alle Matches von Runde X, Gruppen 6-10 bekommen die GLEICHE Zeit
        cursor.execute("""
            SELECT id FROM matches 
            WHERE round = ? AND group_number BETWEEN 6 AND 10
            ORDER BY group_number
        """, (round_num,))
    
        bracket_b_matches = cursor.fetchall()
    
        for match in bracket_b_matches:
            cursor.execute("UPDATE matches SET time = ? WHERE id = ?", 
                         (current_time, match['id']))
            updated_count += 1
    
        print(f"      → {len(bracket_b_matches)} Matches parallel")
    
        # Zeit vorrücken nach Bracket B: Spieldauer + Pause B→nächste Runde A (=break_between_rounds)
        current_time = add_minutes_to_time(current_time, match_duration + pause_between_rounds)

    conn.commit()

    print("\n" + "=" * 70)
    print(f"✅ {updated_count} Matches mit Zeiten versehen")
    print(f"📅 Letzter Timeslot: {current_time}")
    print("=" * 70 + "\n")

    return updated_count


def calculate_round_robin_times_alternative(conn):
    """
    Alternative Zeitberechnung: Matches innerhalb jeder Gruppe nacheinander.

    Logik:
    - Gruppe 1 spielt Match 1, dann Pause, dann Match 2, etc.
    - Alle Gruppen starten gleichzeitig
    - Pro Gruppe: 5 Runden × 3 Matches = 15 Spiele nacheinander

    Diese Methode ist realistischer für kleinere Turniere.

    Args:
        conn: SQLite Connection

    Returns:
        int: Anzahl der Matches mit Zeiten
    """
    cursor = conn.cursor()
    config = get_tournament_config(conn)

    match_duration = config['match_duration']
    break_between_games = config['break_between_games']
    break_between_rounds = config['break_between_rounds']
    start_time = config['start_time']

    print(f"\n⏰ ZEITBERECHNUNG ROUND ROBIN (Alternative Methode)")
    print(f"   Spieldauer: {match_duration} Min")
    print(f"   Pause zwischen Spielen: {break_between_games} Min")
    print(f"   Pause zwischen Runden: {break_between_rounds} Min")
    print(f"   Startzeit: {start_time}")
    print()

    updated_count = 0

    # Pro Gruppe einzeln durchgehen
    for group_num in range(1, 11):
        current_time = start_time
        current_round = None
    
        cursor.execute("""
            SELECT * FROM matches 
            WHERE group_number = ?
            ORDER BY round, id
        """, (group_num,))
    
        matches = cursor.fetchall()
    
        print(f"   Gruppe {group_num}: {len(matches)} Matches")
    
        for match in matches:
            match_id = match['id']
            round_num = match['round']
        
            # Neue Runde beginnt → Rundenpause
            if current_round is not None and current_round != round_num:
                current_time = add_minutes_to_time(current_time, break_between_rounds)
        
            current_round = round_num
        
            # Zeit setzen
            cursor.execute("""
                UPDATE matches 
                SET time = ?
                WHERE id = ?
            """, (current_time, match_id))
        
            updated_count += 1
        
            # Nächster Zeitslot: Spieldauer + Pause
            current_time = add_minutes_to_time(
                current_time,
                match_duration + break_between_games
            )

    conn.commit()

    print(f"\n✅ {updated_count} Matches mit Zeiten versehen")

    return updated_count


# ============================================================================
# DOUBLE ELIMINATION ZEITBERECHNUNG
# ============================================================================

def calculate_double_elim_times(conn, table_name, start_time_str=None):
    """
    Berechnet Spielzeiten für Double Elimination Bracket.

    Logik:
    - Winner Bracket Runde 1: Alle 8 Spiele parallel (verschiedene Felder)
    - Nach Winner R1: Pause, dann Winner R2 (4 Spiele parallel)
    - Loser Bracket: Direkt nach entsprechenden Winner-Spielen

    Args:
        conn: SQLite Connection
        table_name: 'double_elim_matches_a' oder 'double_elim_matches_b'
        start_time_str: Startzeit (optional, sonst nach Round Robin)

    Returns:
        str: Zeit nach letztem Spiel (für nächste Phase)
    """
    cursor = conn.cursor()
    config = get_tournament_config(conn)

    match_duration = config['match_duration']
    break_between_games = config['break_between_games']

    # Startzeit bestimmen
    if start_time_str is None:
        # Nach Round Robin: Letzte Zeit + 30 Min Pause
        cursor.execute("SELECT MAX(time) as last_time FROM matches WHERE time IS NOT NULL")
        last_rr_time = cursor.fetchone()['last_time']
    
        if last_rr_time:
            start_time_str = add_minutes_to_time(last_rr_time, 30)
        else:
            start_time_str = "13:00"  # Fallback

    current_time = start_time_str

    print(f"\n⏰ ZEITBERECHNUNG DOUBLE ELIMINATION ({table_name})")
    print(f"   Startzeit: {current_time}")
    print()

    # Winner Bracket
    for round_num in range(1, 5):
        num_matches = 8 // (2 ** (round_num - 1))
    
        print(f"   Winner Runde {round_num}: {num_matches} Matches um {current_time}")
    
        cursor.execute(f"""
            SELECT id FROM {table_name}
            WHERE round = ? AND bracket = 'Winners'
            ORDER BY match_index
        """, (round_num,))
    
        for match_row in cursor.fetchall():
            cursor.execute(f"""
                UPDATE {table_name}
                SET time = ?
                WHERE id = ?
            """, (current_time, match_row['id']))
    
        # Nächste Runde: Spieldauer + Pause
        current_time = add_minutes_to_time(
            current_time,
            match_duration + break_between_games
        )

    # Loser Bracket (startet während Winner Bracket läuft)
    # Vereinfachung: Loser Bracket 15 Min nach Winner Start
    loser_start = add_minutes_to_time(start_time_str, 15)
    current_time = loser_start

    for round_num in range(1, 7):
        num_matches = max(1, 8 // (2 ** (round_num - 1)))
    
        print(f"   Loser Runde {round_num}: {num_matches} Matches um {current_time}")
    
        cursor.execute(f"""
            SELECT id FROM {table_name}
            WHERE round = ? AND bracket = 'Losers'
            ORDER BY match_index
        """, (round_num,))
    
        for match_row in cursor.fetchall():
            cursor.execute(f"""
                UPDATE {table_name}
                SET time = ?
                WHERE id = ?
            """, (current_time, match_row['id']))
    
        current_time = add_minutes_to_time(
            current_time,
            match_duration + break_between_games
        )

    conn.commit()

    print(f"   → Bracket endet ca. um: {current_time}")

    return current_time


# ============================================================================
# SUPER FINALS ZEITBERECHNUNG
# ============================================================================



def calculate_double_elim_times_and_courts(conn, de_start_time_str=None):
    """Alias — wird von generate_double_elim und recalculate_all_times aufgerufen"""
    return calculate_double_elim_times(conn, 'double_elim_matches', de_start_time_str)

def calculate_super_finals_times(conn, start_time_str=None):
    """
    Berechnet Spielzeiten für Super Finals.

    Reihenfolge:
    1. Halbfinale 1 (HF1)
    2. Halbfinale 2 (HF2) - parallel zu HF1 auf anderem Feld
    3. Pause (30 Min)
    4. Spiel um Platz 3 (THIRD)
    5. Pause (15 Min)
    6. Finale (FINAL)

    Args:
        conn: SQLite Connection
        start_time_str: Startzeit (optional, sonst nach Double Elim)

    Returns:
        str: Zeit nach Finale
    """
    cursor = conn.cursor()
    config = get_tournament_config(conn)

    match_duration = config['match_duration']

    # Startzeit bestimmen
    if start_time_str is None:
        # Nach Double Elimination B
        cursor.execute("""
            SELECT MAX(time) as last_time 
            FROM double_elim_matches_b 
            WHERE time IS NOT NULL
        """)
        last_de_time = cursor.fetchone()['last_time']
    
        if last_de_time:
            start_time_str = add_minutes_to_time(last_de_time, 45)
        else:
            start_time_str = "15:00"  # Fallback

    current_time = start_time_str

    print(f"\n⏰ ZEITBERECHNUNG SUPER FINALS")
    print(f"   Startzeit: {current_time}")
    print()

    # Halbfinale 1 & 2 (parallel)
    print(f"   Halbfinale 1 & 2: {current_time}")
    cursor.execute("""
        UPDATE super_finals_matches 
        SET time = ?
        WHERE match_id IN ('HF1', 'HF2')
    """, (current_time,))

    # Pause nach Halbfinale
    current_time = add_minutes_to_time(current_time, match_duration + 30)

    # Spiel um Platz 3
    print(f"   Spiel um Platz 3: {current_time}")
    cursor.execute("""
        UPDATE super_finals_matches 
        SET time = ?
        WHERE match_id = 'THIRD'
    """, (current_time,))

    # Pause vor Finale
    current_time = add_minutes_to_time(current_time, match_duration + 15)

    # Finale
    print(f"   FINALE: {current_time}")
    cursor.execute("""
        UPDATE super_finals_matches 
        SET time = ?
        WHERE match_id = 'FINAL'
    """, (current_time,))

    # Endzeit
    end_time = add_minutes_to_time(current_time, match_duration)

    conn.commit()

    print(f"   → Turnier endet ca. um: {end_time}\n")

    return end_time


# ============================================================================
# FOLLOWER CUP ZEITBERECHNUNG
# ============================================================================

def calculate_follower_cup_times(conn, start_time_str=None):
    """
    Berechnet Spielzeiten für Follower Cup.

    Läuft parallel zu Double Elimination auf separaten Feldern.

    Args:
        conn: SQLite Connection
        start_time_str: Startzeit (optional, parallel zu Double Elim)

    Returns:
        str: Zeit nach letztem Spiel
    """
    cursor = conn.cursor()
    config = get_tournament_config(conn)

    match_duration = config['match_duration']
    break_between_games = config['break_between_games']

    # Startzeit bestimmen
    if start_time_str is None:
        # Parallel zu Double Elimination, aber auf anderen Feldern
        cursor.execute("""
            SELECT MIN(time) as first_time 
            FROM double_elim_matches_a 
            WHERE time IS NOT NULL
        """)
        de_start = cursor.fetchone()['first_time']
    
        if de_start:
            start_time_str = de_start  # Gleiche Startzeit
        else:
            start_time_str = "13:00"  # Fallback

    current_time = start_time_str

    print(f"\n⏰ ZEITBERECHNUNG FOLLOWER CUP")
    print(f"   Startzeit: {current_time}")
    print()

    # Qualifikationsspiele
    cursor.execute("SELECT COUNT(*) as count FROM follower_quali_matches")
    quali_count = cursor.fetchone()['count']

    if quali_count > 0:
        print(f"   Qualifikation: {quali_count} Matches ab {current_time}")
    
        cursor.execute("SELECT id FROM follower_quali_matches ORDER BY id")
    
        for i, match_row in enumerate(cursor.fetchall()):
            # Alle 4 Spiele parallel (verschiedene Felder)
            if i > 0 and i % 4 == 0:
                current_time = add_minutes_to_time(
                    current_time,
                    match_duration + break_between_games
                )
        
            cursor.execute("""
                UPDATE follower_quali_matches 
                SET time = ?
                WHERE id = ?
            """, (current_time, match_row['id']))
    
        # Pause nach Quali
        current_time = add_minutes_to_time(current_time, 20)

    # Cup-Runden
    rounds = [
        ('eighth', 8, "1/8-Finale"),
        ('quarter', 4, "Viertelfinale"),
        ('semi', 2, "Halbfinale"),
        ('third', 1, "Spiel um Platz 3"),
        ('final', 1, "Finale")
    ]

    for round_name, num_matches, display_name in rounds:
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM follower_cup_matches 
            WHERE round = ?
        """, (round_name,))
    
        count = cursor.fetchone()['count']
    
        if count > 0:
            print(f"   {display_name}: {count} Matches um {current_time}")
        
            cursor.execute("""
                SELECT id FROM follower_cup_matches 
                WHERE round = ?
                ORDER BY match_index
            """, (round_name,))
        
            for match_row in cursor.fetchall():
                cursor.execute("""
                    UPDATE follower_cup_matches 
                    SET time = ?
                    WHERE id = ?
                """, (current_time, match_row['id']))
        
            # Nächste Runde
            if round_name in ['semi', 'third']:
                # Längere Pause vor Finale
                current_time = add_minutes_to_time(
                    current_time,
                    match_duration + 20
                )
            else:
                current_time = add_minutes_to_time(
                    current_time,
                    match_duration + break_between_games
                )

    conn.commit()

    end_time = add_minutes_to_time(current_time, match_duration)
    print(f"   → Follower Cup endet ca. um: {end_time}\n")

    return end_time


# ============================================================================
# HAUPTFUNKTION: ALLE ZEITEN BERECHNEN
# ============================================================================

def calculate_all_match_times(conn):
    """
    Berechnet Spielzeiten für ALLE Turnierphasen.

    Diese Funktion sollte aufgerufen werden:
    1. Nach Round Robin Generierung
    2. Nach Double Elimination Generierung
    3. Nach Super Finals Generierung
    4. Nach Follower Cup Generierung

    ODER einmal am Ende, wenn alle Matches generiert wurden.

    Args:
        conn: SQLite Connection

    Returns:
        dict: Zeitplan-Statistik
    """
    cursor = conn.cursor()

    print("\n" + "=" * 70)
    print("⏰ ZEITBERECHNUNG FÜR ALLE TURNIERPHASEN")
    print("=" * 70)

    stats = {}

    # 1. Round Robin
    cursor.execute("SELECT COUNT(*) as count FROM matches")
    if cursor.fetchone()['count'] > 0:
        calculate_round_robin_times(conn)
    
        cursor.execute("SELECT MIN(time) as start, MAX(time) as end FROM matches WHERE time IS NOT NULL")
        times = cursor.fetchone()
        stats['round_robin'] = {'start': times['start'], 'end': times['end']}

    # 2. Bracket A
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches_a")
    if cursor.fetchone()['count'] > 0:
        end_time_a = calculate_double_elim_times(conn, 'double_elim_matches_a')
    
        cursor.execute("""
            SELECT MIN(time) as start FROM double_elim_matches_a WHERE time IS NOT NULL
        """)
        start_a = cursor.fetchone()['start']
        stats['bracket_a'] = {'start': start_a, 'end': end_time_a}

    # 3. Bracket B (parallel zu A oder nacheinander)
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches_b")
    if cursor.fetchone()['count'] > 0:
        # Optional: Bracket B gleichzeitig mit A (verschiedene Felder)
        # Oder: Bracket B nach A
        end_time_b = calculate_double_elim_times(conn, 'double_elim_matches_b')
    
        cursor.execute("""
            SELECT MIN(time) as start FROM double_elim_matches_b WHERE time IS NOT NULL
        """)
        start_b = cursor.fetchone()['start']
        stats['bracket_b'] = {'start': start_b, 'end': end_time_b}

    # 4. Super Finals
    cursor.execute("SELECT COUNT(*) as count FROM super_finals_matches")
    if cursor.fetchone()['count'] > 0:
        end_time_sf = calculate_super_finals_times(conn)
    
        cursor.execute("""
            SELECT MIN(time) as start FROM super_finals_matches WHERE time IS NOT NULL
        """)
        start_sf = cursor.fetchone()['start']
        stats['super_finals'] = {'start': start_sf, 'end': end_time_sf}

    # 5. Follower Cup (parallel zu Double Elim)
    cursor.execute("""
        SELECT COUNT(*) as count 
        FROM (
            SELECT id FROM follower_quali_matches
            UNION ALL
            SELECT id FROM follower_cup_matches
        )
    """)
    if cursor.fetchone()['count'] > 0:
        end_time_fc = calculate_follower_cup_times(conn)
        stats['follower_cup'] = {'end': end_time_fc}

    # Zusammenfassung
    print("\n" + "=" * 70)
    print("📊 ZEITPLAN-ZUSAMMENFASSUNG")
    print("=" * 70)

    for phase, times in stats.items():
        if 'start' in times and 'end' in times:
            print(f"   {phase.upper()}: {times['start']} - {times['end']}")
        elif 'end' in times:
            print(f"   {phase.upper()}: endet um {times['end']}")

    print("=" * 70 + "\n")

    return stats


# ============================================================================
# RESET-FUNKTION
# ============================================================================

def reset_all_match_times(conn):
    """
    Setzt alle Spielzeiten zurück (NULL).
    Nützlich für Neuberechnung mit geänderten Parametern.

    Args:
        conn: SQLite Connection
    """
    cursor = conn.cursor()

    cursor.execute("UPDATE matches SET time = NULL")
    cursor.execute("UPDATE double_elim_matches_a SET time = NULL")
    cursor.execute("UPDATE double_elim_matches_b SET time = NULL")
    cursor.execute("UPDATE super_finals_matches SET time = NULL")
    cursor.execute("UPDATE follower_quali_matches SET time = NULL")
    cursor.execute("UPDATE follower_cup_matches SET time = NULL")
    cursor.execute("UPDATE placement_matches SET time = NULL")

    conn.commit()
    print("✅ Alle Spielzeiten zurückgesetzt")


# ============================================================================
# INTEGRATION IN BESTEHENDEN CODE
# ============================================================================

# [Integration-Kommentarblock entfernt]






def sort_group_with_head_to_head(conn, group_num):
    """Sortiert eine Gruppe: 1. goals_for DESC  2. goal_difference DESC  3. direkter Vergleich"""
    import functools
    cursor = conn.cursor()
    cursor.execute("""
        SELECT r.*, t.is_ghost
        FROM rankings r
        LEFT JOIN teams t ON r.team = t.name
        WHERE r.group_number = ?
    """, (group_num,))
    teams = list(cursor.fetchall())

    def h2h(team_a, team_b):
        cursor.execute("""
            SELECT score1, score2, team1 FROM matches
            WHERE group_number = ?
            AND ((team1 = ? AND team2 = ?) OR (team1 = ? AND team2 = ?))
            AND score1 IS NOT NULL
        """, (group_num, team_a, team_b, team_b, team_a))
        r = cursor.fetchone()
        if not r: return 0
        if r['team1'] == team_a:
            return 3 if r['score1'] > r['score2'] else 0
        else:
            return 3 if r['score2'] > r['score1'] else 0

    def compare(t1, t2):
        if t1['goals_for'] != t2['goals_for']:
            return -1 if t1['goals_for'] > t2['goals_for'] else 1
        if t1['goal_difference'] != t2['goal_difference']:
            return -1 if t1['goal_difference'] > t2['goal_difference'] else 1
        a = h2h(t1['team'], t2['team'])
        b = h2h(t2['team'], t1['team'])
        if a != b:
            return -1 if a > b else 1
        return 0

    return sorted(teams, key=functools.cmp_to_key(compare))


def get_phase_assignment(conn):
    """
    Berechnet die Phasenzuteilung für alle Teams:
    - DE: P1-3 jeder Gruppe (30) + 2 beste Viertplatzierte (goals_for) = 32
    - FC: 8 restliche Viertplatzierte + Top 8 der 5./6. Platzierten = 16
    - PLZ: letzte 12 der 5./6. Platzierten = 12
    Returns: (de_teams, fc_teams, plz_teams, best2, rest8)
    """
    import functools
    cursor = conn.cursor()

    def sort_grp(g):
        cursor.execute("""
            SELECT r.*, t.is_ghost FROM rankings r
            LEFT JOIN teams t ON r.team = t.name
            WHERE r.group_number = ?
        """, (g,))
        teams = list(cursor.fetchall())
        def cmp(t1, t2):
            if t1['goals_for'] != t2['goals_for']:
                return -1 if t1['goals_for'] > t2['goals_for'] else 1
            if t1['goal_difference'] != t2['goal_difference']:
                return -1 if t1['goal_difference'] > t2['goal_difference'] else 1
            return 0
        return sorted(teams, key=functools.cmp_to_key(cmp))

    de_teams = set()
    fourths = []
    fives_sixes = []

    for g in range(1, 11):
        sg = sort_grp(g)
        real = [t for t in sg if not t['is_ghost']]
        for t in real[:3]:
            de_teams.add(t['team'])
        if len(real) >= 4:
            f = real[3]
            fourths.append({'team': f['team'], 'group': g,
                            'goals_for': f['goals_for'],
                            'goal_difference': f['goal_difference']})
        for t in real[4:6]:
            fives_sixes.append({'team': t['team'],
                                'goals_for': t['goals_for'],
                                'goal_difference': t['goal_difference']})

    # 2 beste Viertplatzierte → DE
    fourths.sort(key=lambda x: (-x['goals_for'], -x['goal_difference']))

    # Manueller Override bei Gleichstand (Wildcard 2)
    try:
        override_row = conn.execute("SELECT wildcard2_team FROM wildcard_override LIMIT 1").fetchone()
    except Exception:
        override_row = None

    if override_row and override_row[0]:
        override_team = override_row[0]
        idx = next((i for i, f in enumerate(fourths) if f['team'] == override_team), None)
        if idx is not None and idx != 1:
            fourths[1], fourths[idx] = fourths[idx], fourths[1]

    best2 = fourths[:2]
    rest8 = fourths[2:]
    for f in best2:
        de_teams.add(f['team'])

    # 5./6. Platzierte sortieren
    fives_sixes.sort(key=lambda x: (-x['goals_for'], -x['goal_difference']))
    fc_from_5_6 = set(t['team'] for t in fives_sixes[:8])
    plz_teams   = set(t['team'] for t in fives_sixes[8:])

    fc_teams = set(f['team'] for f in rest8) | fc_from_5_6

    return de_teams, fc_teams, plz_teams, best2, rest8


def recalculate_rankings_internal(conn):
    """Rankings neu berechnen — stellt sicher dass ALLE Teams einen Eintrag haben"""
    cursor = conn.cursor()

    # ── Sicherheitsnetz: fehlende Rankings-Einträge auffüllen ────────────────
    # Jedes Team in der teams-Tabelle muss einen Eintrag in rankings haben.
    # Fehlt einer (z.B. nach Rename, direktem DB-Edit, oder Bug), wird er erstellt.
    cursor.execute("""
        INSERT INTO rankings (team, group_number, matches_played, wins, draws, losses,
                              goals_for, goals_against, goal_difference, points)
        SELECT t.name, t.group_number, 0, 0, 0, 0, 0, 0, 0, 0
        FROM teams t
        WHERE t.name NOT IN (SELECT team FROM rankings)
    """)

    # ── Doppelte Rankings-Einträge entfernen (behält nur einen pro Team) ─────
    cursor.execute("""
        DELETE FROM rankings WHERE id NOT IN (
            SELECT MIN(id) FROM rankings GROUP BY team
        )
    """)

    # ── Alle Werte auf 0 zurücksetzen ────────────────────────────────────────
    cursor.execute("""
        UPDATE rankings
        SET matches_played=0, wins=0, draws=0, losses=0,
            goals_for=0, goals_against=0, goal_difference=0, points=0
    """)

    # ── Aus gespielte Matches neu berechnen ──────────────────────────────────
    cursor.execute("""
        SELECT * FROM matches
        WHERE score1 IS NOT NULL AND score2 IS NOT NULL
    """)

    for match in cursor.fetchall():
        team1, team2 = match['team1'], match['team2']
        score1, score2 = match['score1'], match['score2']

        if score1 > score2:
            w1,d1,l1,p1 = 1,0,0,3; w2,d2,l2,p2 = 0,0,1,0
        elif score1 < score2:
            w1,d1,l1,p1 = 0,0,1,0; w2,d2,l2,p2 = 1,0,0,3
        else:
            w1,d1,l1,p1 = 0,1,0,1; w2,d2,l2,p2 = 0,1,0,1

        cursor.execute("""
            UPDATE rankings
            SET matches_played=matches_played+1, wins=wins+?, draws=draws+?,
                losses=losses+?, goals_for=goals_for+?, goals_against=goals_against+?,
                goal_difference=goal_difference+(?-?), points=points+?
            WHERE team=?
        """, (w1,d1,l1,score1,score2,score1,score2,p1,team1))

        cursor.execute("""
            UPDATE rankings
            SET matches_played=matches_played+1, wins=wins+?, draws=draws+?,
                losses=losses+?, goals_for=goals_for+?, goals_against=goals_against+?,
                goal_difference=goal_difference+(?-?), points=points+?
            WHERE team=?
        """, (w2,d2,l2,score2,score1,score2,score1,p2,team2))

    conn.commit()


# ============================================================================
# HAUPTROUTEN
# ============================================================================

@app.route('/')
def index():
    """Startseite"""
    return render_template("admin/index.html")


@app.route('/create_new_game', methods=['POST'])
def create_new_game():
    game_name = request.form['game_name']
    
    # Zeitkonfiguration
    match_duration = int(request.form.get('match_duration', 12))
    break_between_games = int(request.form.get('break_between_games', 3))
    start_time = request.form.get('start_time', '09:00')
    
    # NEU: Mittagspause
    lunch_break_enabled = 1 if request.form.get('lunch_break_enabled') == 'on' else 0
    lunch_break_start = request.form.get('lunch_break_start', '12:00')
    lunch_break_end = request.form.get('lunch_break_end', '13:00')
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    initialize_db(db_path)
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Config speichern (NEU: mit Mittagspause)
    cursor.execute("""
        INSERT OR REPLACE INTO tournament_config 
        (game_name, match_duration, break_between_games, start_time,
         lunch_break_enabled, lunch_break_start, lunch_break_end)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (game_name, match_duration, break_between_games, start_time,
          lunch_break_enabled, lunch_break_start, lunch_break_end))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('game_overview', game_name=game_name))

@app.route('/load_game_list')
def load_game_list():
    """Gibt Liste aller Turniere als JSON zurück (für index.html)"""
    tournaments = []
    if os.path.exists(TOURNAMENT_FOLDER):
        for file in sorted(os.listdir(TOURNAMENT_FOLDER)):
            if file.endswith('.db'):
                tournaments.append(file.replace('.db', ''))
    return jsonify({'tournaments': tournaments})


@app.route('/load_game')
def load_game():
    """Seite zum Laden bestehender Turniere"""
    tournaments = []
    if os.path.exists(TOURNAMENT_FOLDER):
        for file in os.listdir(TOURNAMENT_FOLDER):
            if file.endswith('.db'):
                tournaments.append(file.replace('.db', ''))
    
    return render_template("admin/load_game.html", tournaments=tournaments)


@app.route('/load_selected_game', methods=['POST'])
def load_selected_game():
    """Gewähltes Turnier laden"""
    game_name = request.form['tournament']
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    upgrade_database(db_path)
    return redirect(url_for('game_overview', game_name=game_name))





@app.route('/cancel_game/<game_name>', methods=['POST'])
def cancel_game(game_name):
    """Alle Matches löschen, Teams behalten"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM matches")
    cursor.execute("DELETE FROM double_elim_matches_a")
    cursor.execute("DELETE FROM double_elim_matches_b")
    cursor.execute("DELETE FROM super_finals_matches")
    cursor.execute("DELETE FROM follower_quali_matches")
    cursor.execute("DELETE FROM follower_cup_matches")
    cursor.execute("DELETE FROM placement_matches")
    cursor.execute("UPDATE rankings SET matches_played=0, wins=0, draws=0, losses=0, goals_for=0, goals_against=0, goal_difference=0, points=0")

    conn.commit()
    conn.close()

    return redirect(url_for('game_overview', game_name=game_name))


@app.route('/reset_round_robin/<game_name>', methods=['POST'])
def reset_round_robin(game_name):
    """Nur Round Robin Spielplan löschen — Teams und Gruppen bleiben erhalten"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return redirect(url_for('game_overview', game_name=game_name))

    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    # Nur RR-Matches löschen
    cursor.execute("DELETE FROM matches")

    # Rankings zurücksetzen
    cursor.execute("""UPDATE rankings SET
        matches_played=0, wins=0, draws=0, losses=0,
        goals_for=0, goals_against=0, goal_difference=0, points=0""")

    conn.commit()
    conn.close()

    return redirect(url_for('game_overview', game_name=game_name,
                            msg='Spielplan gelöscht — Teams und Gruppen bleiben erhalten.'))




@app.route('/manage_teams/<game_name>')
def manage_teams(game_name):
    """Team-Verwaltungsseite"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM teams ORDER BY group_number, name")
    teams = cursor.fetchall()
    groups = defaultdict(list)
    for team in teams:
        groups[team['group_number']].append(team)
    total_teams = len(teams)
    ghost_count = sum(1 for t in teams if t['is_ghost'])
    missing_teams = max(0, 60 - total_teams)
    flash_msg = request.args.get('msg', '')
    flash_err = request.args.get('err', '')
    conn.close()
    return render_template("admin/manage_teams.html",
                           game_name=game_name,
                           teams=teams,
                           groups=dict(groups),
                           total_teams=total_teams,
                           ghost_count=ghost_count,
                           missing_teams=missing_teams,
                           flash_msg=flash_msg,
                           flash_err=flash_err)


@app.route('/add_team/<game_name>', methods=['POST'])
def add_team(game_name):
    """Team hinzufügen — einzeln oder per CSV"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    # CSV-Import
    csv_file = request.files.get('csv_file')
    if csv_file and csv_file.filename:
        import csv, io as _io
        stream = _io.StringIO(csv_file.stream.read().decode('utf-8-sig'))
        reader = csv.DictReader(stream)
        added = 0; errors = []
        for row in reader:
            name = (row.get('Teamname') or row.get('teamname') or row.get('name') or '').strip()
            grp  = (row.get('Gruppe')   or row.get('gruppe')   or row.get('group') or '0').strip()
            if not name or not grp.isdigit(): continue
            g = int(grp)
            if g < 1 or g > 10: errors.append(f"{name}: Gruppe {g} ungültig"); continue
            cursor.execute("SELECT COUNT(*) as c FROM teams WHERE group_number=?", (g,))
            if cursor.fetchone()['c'] >= 6: errors.append(f"{name}: Gruppe {g} voll"); continue
            cursor.execute("INSERT INTO teams (name,group_number,is_ghost) VALUES (?,?,0)", (name,g))
            cursor.execute("INSERT INTO rankings (team,group_number,matches_played,wins,draws,losses,goals_for,goals_against,goal_difference,points) VALUES (?,?,0,0,0,0,0,0,0,0)", (name,g))
            added += 1
        conn.commit(); conn.close()
        msg = f"{added} Teams importiert."
        if errors: msg += " Fehler: " + "; ".join(errors[:3])
        return redirect(url_for('manage_teams', game_name=game_name, msg=msg))

    # Einzelnes Team
    team_name    = request.form.get('team_name', '').strip()
    group_number = request.form.get('group_number', '0').strip()
    if not team_name or not group_number.isdigit():
        conn.close()
        return redirect(url_for('manage_teams', game_name=game_name, err='Name und Gruppe erforderlich!'))
    g = int(group_number)
    if g < 1 or g > 10:
        conn.close()
        return redirect(url_for('manage_teams', game_name=game_name, err='Gruppe muss 1–10 sein!'))
    cursor.execute("SELECT COUNT(*) as c FROM teams WHERE group_number=?", (g,))
    if cursor.fetchone()['c'] >= 6:
        conn.close()
        return redirect(url_for('manage_teams', game_name=game_name, err=f'Gruppe {g} ist bereits voll (max. 6 Teams)!'))
    cursor.execute("SELECT COUNT(*) as c FROM teams WHERE name=?", (team_name,))
    if cursor.fetchone()['c'] > 0:
        conn.close()
        return redirect(url_for('manage_teams', game_name=game_name, err=f'Team "{team_name}" existiert bereits!'))
    cursor.execute("INSERT INTO teams (name,group_number,is_ghost) VALUES (?,?,0)", (team_name, g))
    cursor.execute("INSERT INTO rankings (team,group_number,matches_played,wins,draws,losses,goals_for,goals_against,goal_difference,points) VALUES (?,?,0,0,0,0,0,0,0,0)", (team_name, g))
    conn.commit(); conn.close()
    return redirect(url_for('manage_teams', game_name=game_name, msg=f'Team "{team_name}" (Gruppe {g}) hinzugefügt!'))


@app.route('/delete_team/<game_name>/<int:team_id>', methods=['POST'])
def delete_team(game_name, team_id):
    """Team löschen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM teams WHERE id=?", (team_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return redirect(url_for('manage_teams', game_name=game_name, err='Team nicht gefunden!'))
    team_name = row['name']
    cursor.execute("DELETE FROM teams WHERE id=?", (team_id,))
    cursor.execute("DELETE FROM rankings WHERE team=?", (team_name,))
    conn.commit(); conn.close()
    return redirect(url_for('manage_teams', game_name=game_name, msg=f'Team "{team_name}" gelöscht.'))


@app.route('/edit_team/<game_name>/<int:team_id>', methods=['POST'])
def edit_team(game_name, team_id):
    """Team umbenennen / Gruppe ändern"""
    new_name  = request.form.get('new_name', '').strip()
    new_group = request.form.get('new_group', '0').strip()
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM teams WHERE id=?", (team_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return redirect(url_for('manage_teams', game_name=game_name, err='Team nicht gefunden!'))
    old_name = row['name']
    g = int(new_group) if new_group.isdigit() else 0
    if g < 1 or g > 10:
        conn.close()
        return redirect(url_for('manage_teams', game_name=game_name, err='Gruppe muss 1–10 sein!'))
    cursor.execute("UPDATE teams SET name=?, group_number=? WHERE id=?", (new_name, g, team_id))
    cursor.execute("UPDATE rankings SET team=?, group_number=? WHERE team=?", (new_name, g, old_name))
    conn.commit(); conn.close()
    return redirect(url_for('manage_teams', game_name=game_name, msg=f'Team "{old_name}" → "{new_name}" aktualisiert.'))


@app.route('/generate_ghost_teams/<game_name>', methods=['POST'])
def generate_ghost_teams(game_name):
    """Ghost-Teams für fehlende Plätze generieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM teams")
    current_count = cursor.fetchone()['count']
    missing_count = 60 - current_count
    if missing_count <= 0:
        conn.close()
        return redirect(url_for('manage_teams', game_name=game_name, err='Bereits 60 Teams vorhanden!'))
    cursor.execute("SELECT group_number, COUNT(*) as count FROM teams GROUP BY group_number")
    group_counts = {row['group_number']: row['count'] for row in cursor.fetchall()}
    ghost_index = 0; teams_created = 0
    for group_num in range(1, 11):
        needed = 6 - group_counts.get(group_num, 0)
        for i in range(needed):
            ghost_name = GHOST_TEAM_NAMES[ghost_index] if ghost_index < len(GHOST_TEAM_NAMES) else f"Ghost {ghost_index+1}"
            cursor.execute("INSERT INTO teams (name,group_number,is_ghost) VALUES (?,?,1)", (ghost_name, group_num))
            cursor.execute("INSERT INTO rankings (team,group_number,matches_played,wins,draws,losses,goals_for,goals_against,goal_difference,points) VALUES (?,?,0,0,0,0,0,0,0,0)", (ghost_name, group_num))
            ghost_index += 1; teams_created += 1
            if teams_created >= missing_count: break
        if teams_created >= missing_count: break
    conn.commit(); conn.close()
    return redirect(url_for('manage_teams', game_name=game_name, msg=f'{teams_created} Ghost-Teams erstellt.'))


@app.route('/csv_template/<game_name>')
def csv_template(game_name):
    """CSV-Vorlage zum Download"""
    import csv, io as _io
    buf = _io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(['Teamname', 'Gruppe'])
    for g in range(1, 11):
        for t in range(1, 7):
            writer.writerow([f'Beispiel G{g}T{t}', g])
    buf.seek(0)
    from flask import Response
    return Response(
        buf.getvalue().encode('utf-8-sig'),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=teams_vorlage_{game_name}.csv'}
    )





@app.route('/generate_matches/<game_name>')
def generate_matches(game_name):
    """Spielplan für Round Robin generieren - MIT AUTOMATISCHER SPIELNUMMERIERUNG"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM matches")
    if cursor.fetchone()['count'] > 0:
        conn.close()
        return render_template("admin/error.html", 
                             error_message="Spiele wurden bereits generiert!")
    
    cursor.execute("SELECT * FROM teams ORDER BY group_number, name")
    all_teams = cursor.fetchall()
    
    groups = defaultdict(list)
    for team in all_teams:
        groups[team['group_number']].append(team['name'])

    # ── Rotierendes Feldsystem ────────────────────────────────────────────────
    # Bracket A: Gruppen 1–5, Bracket B: Gruppen 6–10
    # Jede Gruppe belegt pro Runde 3 Felder (n//2 = 3 Spiele bei 6 Teams)
    # Startfeld der Gruppe in Runde r = ((grp_index * 3) + (r-1) * 3) % 15 + 1
    # Beispiel Bracket A Runde 1: Grp1→F1-3, Grp2→F4-6, Grp3→F7-9, Grp4→F10-12, Grp5→F13-15
    #          Bracket A Runde 2: Grp1→F4-6, Grp2→F7-9, Grp3→F10-12, Grp4→F13-15, Grp5→F1-3
    # Bracket B spielen auf denselben Feldern 1-15, zeitlich versetzt

    FIELDS = 15   # Felder 1–15
    SPIELE_PRO_GRUPPE = 3  # n//2 bei 6 Teams

    def get_start_field(grp_index_in_bracket, round_num):
        """Startfeld für Gruppe in Runde — rotiert um 3 pro Runde"""
        offset = ((grp_index_in_bracket * SPIELE_PRO_GRUPPE) + (round_num - 1) * SPIELE_PRO_GRUPPE) % FIELDS
        return offset + 1  # 1-basiert

    # Gruppen nach Brackets aufteilen
    bracket_a_groups = sorted(g for g in groups.keys() if g <= 5)
    bracket_b_groups = sorted(g for g in groups.keys() if g > 5)

    # Spiele für ein Bracket generieren
    def generate_bracket(bracket_groups):
        # teams_by_group: Kopie für Round-Robin-Rotation
        teams_state = {g: list(groups[g]) for g in bracket_groups}

        for round_num in range(1, 6):  # 5 Runden bei 6 Teams
            for grp_idx, group_num in enumerate(bracket_groups):
                teams = teams_state[group_num]
                n = len(teams)
                if n < 2:
                    continue

                start_field = get_start_field(grp_idx, round_num)

                for i in range(n // 2):
                    team1 = teams[i]
                    team2 = teams[n - 1 - i]
                    field = ((start_field - 1 + i) % FIELDS) + 1  # F, F+1, F+2

                    cursor.execute("""
                        INSERT INTO matches (round, team1, team2, group_number, field)
                        VALUES (?, ?, ?, ?, ?)
                    """, (round_num, team1, team2, group_num, field))

            # Rotation für nächste Runde (alle Gruppen gleichzeitig)
            for group_num in bracket_groups:
                t = teams_state[group_num]
                if len(t) > 2:
                    teams_state[group_num] = [t[0]] + [t[-1]] + t[1:-1]

    generate_bracket(bracket_a_groups)
    generate_bracket(bracket_b_groups)
    
    # Ghost-Teams verlieren automatisch
    cursor.execute("SELECT name FROM teams WHERE is_ghost = 1")
    ghost_teams = [row['name'] for row in cursor.fetchall()]
    
    if ghost_teams:
        cursor.execute("SELECT * FROM matches")
        for match in cursor.fetchall():
            if match['team1'] in ghost_teams:
                cursor.execute("UPDATE matches SET score1 = 0, score2 = 42 WHERE id = ?", (match['id'],))
            elif match['team2'] in ghost_teams:
                cursor.execute("UPDATE matches SET score1 = 42, score2 = 0 WHERE id = ?", (match['id'],))
    
    conn.commit()
    
    # NEU: Spielnummern vergeben
    print("\n🔢 Vergebe Spielnummern...")
    assign_round_robin_match_numbers(conn)

    # NEU: Spielzeiten berechnen
    print("\n⏰ Berechne Spielzeiten...")
    calculate_round_robin_times(conn)

    recalculate_rankings_internal(conn)
    conn.close()
    return redirect(url_for('game_overview', game_name=game_name))


@app.route('/game_overview/<game_name>')
def game_overview(game_name):
    """Turnierübersicht"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM teams")
    team_count = cursor.fetchone()['count']
    cursor.execute("SELECT COUNT(*) as count FROM teams WHERE is_ghost = 1")
    ghost_count = cursor.fetchone()['count']
    cursor.execute("SELECT COUNT(*) as count FROM matches")
    match_count = cursor.fetchone()['count']
    cursor.execute("SELECT COUNT(*) as count FROM matches WHERE score1 IS NOT NULL")
    results_count = cursor.fetchone()['count']
    conn.close()
    return render_template("admin/game_overview.html",
                           game_name=game_name, team_count=team_count,
                           ghost_count=ghost_count, match_count=match_count,
                           results_count=results_count)


@app.route('/match_overview/<game_name>')
def match_overview(game_name):
    """Spielplan Übersicht — Bracket A/B, alle Runden mit Zeit+Feld"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM matches ORDER BY round, group_number, field")
    matches = cursor.fetchall()
    rounds = sorted(set(m['round'] for m in matches))
    conn.close()
    return render_template("admin/match_overview.html",
                           game_name=game_name,
                           matches=matches,
                           rounds=rounds)


@app.route('/enter_results/<game_name>')
def enter_results(game_name):
    """Ergebnisse eintragen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    current_round = request.args.get('round', 1, type=int)
    cursor.execute("SELECT DISTINCT round FROM matches ORDER BY round")
    rounds = [r['round'] for r in cursor.fetchall()]
    cursor.execute("SELECT * FROM matches WHERE round = ? ORDER BY group_number, field",
                   (current_round,))
    matches = cursor.fetchall()
    conn.close()
    return render_template("admin/enter_results.html",
                           game_name=game_name, matches=matches,
                           rounds=rounds, current_round=current_round)


@app.route('/save_result/<game_name>/<int:match_id>', methods=['POST'])
def save_result(game_name, match_id):
    """Einzelnes Ergebnis speichern — antwortet mit JSON für AJAX"""
    score1 = request.form.get('score1', '').strip()
    score2 = request.form.get('score2', '').strip()
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'

    if not score1 or not score2:
        if is_ajax:
            return jsonify({'ok': False, 'error': 'Score fehlt'})
        return redirect(url_for('enter_results', game_name=game_name))
    try:
        s1, s2 = int(score1), int(score2)
        if not (0 <= s1 <= 42 and 0 <= s2 <= 42):
            if is_ajax:
                return jsonify({'ok': False, 'error': 'Punktzahl muss 0–42 sein'})
            return render_template("admin/error.html", error_message="Punktzahl 0–42!")
    except ValueError:
        if is_ajax:
            return jsonify({'ok': False, 'error': 'Ungültige Zahl'})
        return redirect(url_for('enter_results', game_name=game_name))

    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    conn.execute("UPDATE matches SET score1=?, score2=? WHERE id=?", (s1, s2, match_id))
    conn.commit()
    recalculate_rankings_internal(conn)
    conn.close()

    if is_ajax:
        return jsonify({'ok': True, 'match_id': match_id, 'score1': s1, 'score2': s2})
    return redirect(url_for('enter_results', game_name=game_name))


@app.route('/delete_result/<game_name>/<int:match_id>', methods=['POST'])
def delete_result(game_name, match_id):
    """Ergebnis löschen — antwortet mit JSON für AJAX"""
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    conn.execute("UPDATE matches SET score1=NULL, score2=NULL WHERE id=?", (match_id,))
    conn.commit()
    recalculate_rankings_internal(conn)
    conn.close()

    if is_ajax:
        return jsonify({'ok': True, 'match_id': match_id})
    return redirect(url_for('enter_results', game_name=game_name))


@app.route('/recalculate_rankings/<game_name>', methods=['POST'])
def recalculate_rankings(game_name):
    """Rankings manuell neu berechnen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    recalculate_rankings_internal(conn)
    conn.close()
    return redirect(url_for('group_standings', game_name=game_name))


@app.route('/set_wildcard_override/<game_name>', methods=['POST'])
def set_wildcard_override(game_name):
    """Manuelle Auswahl des 2. Wildcard-Teams bei Gleichstand"""
    team = request.form.get('wildcard2_team', '').strip()
    if not team:
        return redirect(url_for('group_standings', game_name=game_name))
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS wildcard_override (id INTEGER PRIMARY KEY, wildcard2_team TEXT NOT NULL)")
        conn.execute("DELETE FROM wildcard_override")
        conn.execute("INSERT INTO wildcard_override (id, wildcard2_team) VALUES (1, ?)", (team,))
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
    return redirect(url_for('group_standings', game_name=game_name))


@app.route('/group_standings/<game_name>')
def group_standings(game_name):
    """Gruppentabellen anzeigen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    conn = get_db_connection(db_path)
    groups = {}
    for group_num in range(1, 11):
        groups[group_num] = sort_group_with_head_to_head(conn, group_num)
    de_teams, fc_teams, plz_teams, best2, rest8 = get_phase_assignment(conn)
    all_fourths = rest8 + best2
    all_fourths.sort(key=lambda x: (-x['goals_for'], -x['goal_difference']))
    best_4th_1 = best2[0] if len(best2) > 0 else None
    best_4th_2 = best2[1] if len(best2) > 1 else None
    wildcard_tie = False
    wildcard_tie_teams = []
    if len(all_fourths) >= 3:
        p2, p3 = all_fourths[1], all_fourths[2]
        if p2['goals_for'] == p3['goals_for'] and p2['goal_difference'] == p3['goal_difference']:
            wildcard_tie = True
            wildcard_tie_teams = [p2, p3]
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS wildcard_override (id INTEGER PRIMARY KEY, wildcard2_team TEXT NOT NULL)")
        override_row = conn.execute("SELECT wildcard2_team FROM wildcard_override LIMIT 1").fetchone()
        current_override = override_row[0] if override_row else None
    except Exception:
        current_override = None
    conn.close()
    return render_template("admin/group_standings.html",
                           game_name=game_name, groups=groups,
                           wildcard_tie=wildcard_tie, wildcard_tie_teams=wildcard_tie_teams,
                           current_override=current_override,
                           best_4th_1=best_4th_1, best_4th_2=best_4th_2,
                           all_fourths=all_fourths, de_teams=de_teams,
                           fc_teams=fc_teams, plz_teams=plz_teams)


@app.route('/generate_double_elim/<game_name>')
def generate_double_elim(game_name):
    """Beide Double Elimination Brackets generieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches_a")
    if cursor.fetchone()['count'] > 0:
        conn.close()
        return render_template("admin/error.html", 
                             error_message="Brackets wurden bereits generiert!")
    
    bracket_a_teams = get_qualified_teams_for_bracket(conn, 1, 5)
    bracket_b_teams = get_qualified_teams_for_bracket(conn, 6, 10)
    
    if len(bracket_a_teams) < 16 or len(bracket_b_teams) < 16:
        conn.close()
        return render_template("admin/error.html", 
                             error_message=f"Nicht genug qualifizierte Teams! A: {len(bracket_a_teams)}, B: {len(bracket_b_teams)}")
    
    # ── KORREKTE 16-TEAM DOUBLE ELIMINATION STRUKTUR ────────────────────────
    # WB: R1=8, R2=4, R3=2, R4=1 (WB Final)
    # LB: R1=8, R2=8, R3=4, R4=4, R5=2, R6=2, R7=1, R8=1 (LB Final)
    # Seeding: P1 vs P16, P2 vs P15, ... P8 vs P9

    # LB-Runden-Struktur: {round: anzahl_spiele}
    LB_ROUNDS = {1:8, 2:8, 3:4, 4:4, 5:2, 6:2, 7:1, 8:1}
    WB_ROUNDS = {1:8, 2:4, 3:2, 4:1}  # R4 = WB Final

    def insert_bracket(table, teams):
        # WB Runde 1 — Seeding: 1 vs 16, 2 vs 15, 3 vs 14, 4 vs 13, ...
        # match_index 0: seed1 vs seed16, 1: seed8 vs seed9, 2: seed4 vs seed13, ...
        # Standard DE Seeding-Schema:
        seeds = list(range(16))  # 0..15 = Platz 1..16
        pairings = [
            (0, 15), (7, 8), (3, 12), (4, 11),
            (1, 14), (6, 9), (2, 13), (5, 10)
        ]
        for i, (s1, s2) in enumerate(pairings):
            cursor.execute(f"""
                INSERT INTO {table}
                (round, bracket, match_index, team1, team2)
                VALUES (1, 'Winners', ?, ?, ?)
            """, (i, teams[s1], teams[s2]))

        # WB Runden 2-4 (leer, werden durch Weiterleitung befüllt)
        for rnd, n in WB_ROUNDS.items():
            if rnd == 1: continue
            for i in range(n):
                cursor.execute(f"""
                    INSERT INTO {table}
                    (round, bracket, match_index, team1, team2)
                    VALUES (?, 'Winners', ?, NULL, NULL)
                """, (rnd, i))

        # LB Runden 1-8 (leer, werden durch Weiterleitung befüllt)
        for rnd, n in LB_ROUNDS.items():
            for i in range(n):
                cursor.execute(f"""
                    INSERT INTO {table}
                    (round, bracket, match_index, team1, team2)
                    VALUES (?, 'Losers', ?, NULL, NULL)
                """, (rnd, i))

    insert_bracket('double_elim_matches_a', bracket_a_teams)
    insert_bracket('double_elim_matches_b', bracket_b_teams)
    
    conn.commit()
    cursor.execute("SELECT MAX(match_number) as max_num FROM matches")
    next_number = (cursor.fetchone()['max_num'] or 0) + 1

    print("\n🔢 Vergebe Spielnummern...")
    next_number = assign_double_elim_match_numbers_a(conn, next_number)
    assign_double_elim_match_numbers_b(conn, next_number)

    # NEU: Spielzeiten
    print("\n⏰ Berechne Spielzeiten...")
    calculate_double_elim_times(conn, 'double_elim_matches_a')
    calculate_double_elim_times(conn, 'double_elim_matches_b')

    conn.close()
    
    return redirect(url_for('double_elim_bracket', game_name=game_name))



@app.route('/renumber_all_matches/<game_name>', methods=['POST'])
def renumber_all_matches(game_name):
    """Manuelle Neunummerierung aller Spiele"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return jsonify({"success": False, "error": "Turnier nicht gefunden!"})
    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("UPDATE matches SET match_number = NULL")
        cursor.execute("UPDATE double_elim_matches_a SET match_number = NULL")
        cursor.execute("UPDATE double_elim_matches_b SET match_number = NULL")
        cursor.execute("UPDATE super_finals_matches SET match_number = NULL")
        cursor.execute("UPDATE follower_quali_matches SET match_number = NULL")
        cursor.execute("UPDATE follower_cup_matches SET match_number = NULL")
        cursor.execute("UPDATE placement_matches SET match_number = NULL")
        conn.commit()
        stats = assign_all_match_numbers(conn)
        conn.close()
        return jsonify({"success": True, "message": "Spielnummern neu vergeben!", "stats": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/double_elim_bracket/<game_name>')
def double_elim_bracket(game_name):
    """Beide Double Elimination Brackets anzeigen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM double_elim_matches_a 
        ORDER BY round, bracket DESC, match_index
    """)
    matches_a = cursor.fetchall()
    
    cursor.execute("""
        SELECT * FROM double_elim_matches_b 
        ORDER BY round, bracket DESC, match_index
    """)
    matches_b = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/double_elim_bracket.html",
                         game_name=game_name,
                         matches_a=matches_a,
                         matches_b=matches_b)


@app.route('/enter_double_elim_results/<game_name>')
def enter_double_elim_results(game_name):
    """Double Elimination Ergebnisse eintragen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 'A' as bracket_id, * FROM double_elim_matches_a 
        WHERE team1 IS NOT NULL AND team2 IS NOT NULL
        ORDER BY round, bracket DESC, match_index
    """)
    matches_a = cursor.fetchall()
    
    cursor.execute("""
        SELECT 'B' as bracket_id, * FROM double_elim_matches_b 
        WHERE team1 IS NOT NULL AND team2 IS NOT NULL
        ORDER BY round, bracket DESC, match_index
    """)
    matches_b = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/enter_double_elim_results.html",
                         game_name=game_name,
                         matches_a=matches_a,
                         matches_b=matches_b)


@app.route('/update_double_elim_result/<game_name>/<int:match_id>', methods=['POST'])
def update_double_elim_result(game_name, match_id):
    """Double Elimination Ergebnis speichern"""
    score1 = int(request.form['score1'])
    score2 = int(request.form['score2'])
    
    if score1 > 42 or score2 > 42 or score1 < 0 or score2 < 0:
        return render_template("admin/error.html", 
                             error_message="Punktzahl muss zwischen 0 und 42 liegen!")
    
    if score1 == score2:
        return render_template("admin/error.html", 
                             error_message="Unentschieden nicht erlaubt!")
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    table = f"double_elim_matches_{bracket_id.lower()}"
    
    cursor.execute(f"SELECT * FROM {table} WHERE id = ?", (match_id,))
    match = cursor.fetchone()
    
    if not match:
        conn.close()
        return redirect(url_for('enter_double_elim_results', game_name=game_name))
    
    winner = match['team1'] if score1 > score2 else match['team2']
    loser = match['team2'] if score1 > score2 else match['team1']
    
    cursor.execute(f"""
        UPDATE {table}
        SET score1 = ?, score2 = ?, winner = ?, loser = ?
        WHERE id = ?
    """, (score1, score2, winner, loser, match_id))
    
    conn.commit()
    
    # Weiterleitung
    if bracket_id == 'A':
        process_double_elim_forwarding(conn, match, table, WINNER_MAPPING_A, 
                                       LOSER_MAPPING_A, LOSER_WINNER_MAPPING_A)
    else:
        process_double_elim_forwarding(conn, match, table, WINNER_MAPPING_B, 
                                       LOSER_MAPPING_B, LOSER_WINNER_MAPPING_B)
    
    conn.close()
    
    return redirect(url_for('enter_double_elim_results', game_name=game_name))


# ============================================================================
# SUPER FINALS
# ============================================================================

@app.route('/generate_super_finals/<game_name>')
def generate_super_finals(game_name):
    """Super Finals aus Bracket A & B generieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM super_finals_matches")
    if cursor.fetchone()['count'] > 0:
        conn.close()
        return render_template("admin/error.html", 
                             error_message="Super Finals wurden bereits generiert!")
    
    # Bracket A Finalisten
    cursor.execute("""
        SELECT winner FROM double_elim_matches_a 
        WHERE round = 4 AND bracket = 'Winners' AND match_index = 0
        AND winner IS NOT NULL
    """)
    row = cursor.fetchone()
    winner_a = row['winner'] if row else None
    
    cursor.execute("""
        SELECT winner FROM double_elim_matches_a 
        WHERE round = 6 AND bracket = 'Losers' 
        AND winner IS NOT NULL
        ORDER BY id DESC LIMIT 1
    """)
    row = cursor.fetchone()
    second_a = row['winner'] if row else None
    
    # Bracket B Finalisten
    cursor.execute("""
        SELECT winner FROM double_elim_matches_b 
        WHERE round = 4 AND bracket = 'Winners' AND match_index = 0
        AND winner IS NOT NULL
    """)
    row = cursor.fetchone()
    winner_b = row['winner'] if row else None
    
    cursor.execute("""
        SELECT winner FROM double_elim_matches_b 
        WHERE round = 6 AND bracket = 'Losers' 
        AND winner IS NOT NULL
        ORDER BY id DESC LIMIT 1
    """)
    row = cursor.fetchone()
    second_b = row['winner'] if row else None
    
    if not all([winner_a, second_a, winner_b, second_b]):
        conn.close()
        return render_template("admin/error.html", 
                             error_message=f"Nicht alle Bracket-Finalisten stehen fest! A1:{winner_a}, A2:{second_a}, B1:{winner_b}, B2:{second_b}")
    
    match_number = 213
    
    # Halbfinale 1: Sieger A vs 2. Platz B
    cursor.execute("""
        INSERT INTO super_finals_matches 
        (match_number, match_id, team1, team2)
        VALUES (?, 'HF1', ?, ?)
    """, (match_number, winner_a, second_b))
    match_number += 1
    
    # Halbfinale 2: Sieger B vs 2. Platz A
    cursor.execute("""
        INSERT INTO super_finals_matches 
        (match_number, match_id, team1, team2)
        VALUES (?, 'HF2', ?, ?)
    """, (match_number, winner_b, second_a))
    match_number += 1
    
    # Finale
    cursor.execute("""
        INSERT INTO super_finals_matches 
        (match_number, match_id, team1, team2)
        VALUES (?, 'FINAL', NULL, NULL)
    """, (match_number,))
    match_number += 1
    
    # Spiel um Platz 3
    cursor.execute("""
        INSERT INTO super_finals_matches 
        (match_number, match_id, team1, team2)
        VALUES (?, 'THIRD', NULL, NULL)
    """, (match_number,))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('super_finals_overview', game_name=game_name))


@app.route('/super_finals_overview/<game_name>')
def super_finals_overview(game_name):
    """Super Finals Übersicht"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM super_finals_matches 
        ORDER BY 
            CASE match_id 
                WHEN 'HF1' THEN 1 
                WHEN 'HF2' THEN 2 
                WHEN 'FINAL' THEN 3 
                WHEN 'THIRD' THEN 4 
            END
    """)
    matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/super_finals_overview.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/enter_super_finals_results/<game_name>')
def enter_super_finals_results(game_name):
    """Super Finals Ergebnisse eintragen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM super_finals_matches 
        WHERE team1 IS NOT NULL AND team2 IS NOT NULL
        ORDER BY 
            CASE match_id 
                WHEN 'HF1' THEN 1 
                WHEN 'HF2' THEN 2 
                WHEN 'FINAL' THEN 3 
                WHEN 'THIRD' THEN 4 
            END
    """)
    matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/enter_super_finals_results.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/save_super_finals_result/<game_name>/<int:match_id>', methods=['POST'])
def save_super_finals_result(game_name, match_id):
    """Super Finals Ergebnis speichern"""
    score1 = int(request.form['score1'])
    score2 = int(request.form['score2'])
    
    if score1 > 42 or score2 > 42 or score1 < 0 or score2 < 0:
        return render_template("admin/error.html", 
                             error_message="Punktzahl muss zwischen 0 und 42 liegen!")
    
    if score1 == score2:
        return render_template("admin/error.html", 
                             error_message="Unentschieden nicht erlaubt!")
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM super_finals_matches WHERE id = ?", (match_id,))
    match = cursor.fetchone()
    
    if not match:
        conn.close()
        return redirect(url_for('enter_super_finals_results', game_name=game_name))
    
    winner = match['team1'] if score1 > score2 else match['team2']
    loser = match['team2'] if score1 > score2 else match['team1']
    
    cursor.execute("""
        UPDATE super_finals_matches
        SET score1 = ?, score2 = ?, winner = ?
        WHERE id = ?
    """, (score1, score2, winner, match_id))
    
    match_id_str = match['match_id']
    
    if match_id_str == 'HF1':
        cursor.execute("UPDATE super_finals_matches SET team1 = ? WHERE match_id = 'FINAL'", (winner,))
        cursor.execute("UPDATE super_finals_matches SET team1 = ? WHERE match_id = 'THIRD'", (loser,))
    elif match_id_str == 'HF2':
        cursor.execute("UPDATE super_finals_matches SET team2 = ? WHERE match_id = 'FINAL'", (winner,))
        cursor.execute("UPDATE super_finals_matches SET team2 = ? WHERE match_id = 'THIRD'", (loser,))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('enter_super_finals_results', game_name=game_name))


@app.route('/reset_super_finals/<game_name>', methods=['POST'])
def reset_super_finals(game_name):
    """Super Finals zurücksetzen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM super_finals_matches")
    conn.commit()
    conn.close()
    
    return redirect(url_for('game_overview', game_name=game_name))


# ============================================================================
# FOLLOWER CUP
# ============================================================================

@app.route('/generate_follower_quali/<game_name>')
def generate_follower_quali(game_name):
    """Follower Cup Qualifikationsrunde generieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM follower_quali_matches")
    if cursor.fetchone()['count'] > 0:
        conn.close()
        return render_template("admin/error.html", 
                             error_message="Follower Quali wurde bereits generiert!")
    
    follower_teams = []
    
    # 4. Platzierte
    cursor.execute("""
        SELECT r.team, r.points, r.goal_difference, r.goals_for, r.group_number
        FROM rankings r
        WHERE (SELECT COUNT(*) FROM rankings r2 
               WHERE r2.group_number = r.group_number 
               AND (r2.points > r.points 
                    OR (r2.points = r.points AND r2.goal_difference > r.goal_difference)
                    OR (r2.points = r.points AND r2.goal_difference = r.goal_difference 
                        AND r2.goals_for > r.goals_for))) = 3
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC
    """)
    all_4th = cursor.fetchall()
    
    for team in all_4th[2:]:
        follower_teams.append((team['team'], team['points'], team['goal_difference']))
    
    # 5. Platzierte
    cursor.execute("""
        SELECT r.team, r.points, r.goal_difference
        FROM rankings r
        WHERE (SELECT COUNT(*) FROM rankings r2 
               WHERE r2.group_number = r.group_number 
               AND (r2.points > r.points 
                    OR (r2.points = r.points AND r2.goal_difference > r.goal_difference)
                    OR (r2.points = r.points AND r2.goal_difference = r.goal_difference 
                        AND r2.goals_for > r.goals_for))) = 4
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.points DESC, r.goal_difference DESC
    """)
    follower_teams.extend([(t['team'], t['points'], t['goal_difference']) for t in cursor.fetchall()])
    
    # 6. Platzierte
    cursor.execute("""
        SELECT r.team, r.points, r.goal_difference
        FROM rankings r
        WHERE (SELECT COUNT(*) FROM rankings r2 
               WHERE r2.group_number = r.group_number 
               AND (r2.points > r.points 
                    OR (r2.points = r.points AND r2.goal_difference > r.goal_difference)
                    OR (r2.points = r.points AND r2.goal_difference = r.goal_difference 
                        AND r2.goals_for > r.goals_for))) = 5
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.points DESC, r.goal_difference DESC
    """)
    follower_teams.extend([(t['team'], t['points'], t['goal_difference']) for t in cursor.fetchall()])
    
    follower_teams.sort(key=lambda x: (x[1], x[2]), reverse=True)
    
    if len(follower_teams) > 20:
        follower_teams = follower_teams[:20]
    
    quali_teams = [t[0] for t in follower_teams[4:]]
    
    match_number = 217
    court = 1
    
    num_quali_teams = len(quali_teams)
    for i in range(num_quali_teams // 2):
        team1 = quali_teams[i]
        team2 = quali_teams[-(i+1)]
        
        cursor.execute("""
            INSERT INTO follower_quali_matches 
            (match_number, team1, team2, court)
            VALUES (?, ?, ?, ?)
        """, (match_number, team1, team2, court))
        
        match_number += 1
        court = (court % 15) + 1
    
    if num_quali_teams % 2 == 1:
        bye_team = quali_teams[num_quali_teams // 2]
        cursor.execute("""
            INSERT INTO follower_quali_matches 
            (match_number, team1, team2, winner, court)
            VALUES (?, ?, 'BYE', ?, ?)
        """, (match_number, bye_team, bye_team, court))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('follower_quali_overview', game_name=game_name))


@app.route('/follower_quali_overview/<game_name>')
def follower_quali_overview(game_name):
    """Follower Cup Qualifikationsübersicht"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM follower_quali_matches ORDER BY match_number")
    quali_matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/follower_quali_overview.html",
                         game_name=game_name,
                         matches=quali_matches)


@app.route('/enter_follower_quali_results/<game_name>')
def enter_follower_quali_results(game_name):
    """Follower Quali Ergebnisse eintragen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM follower_quali_matches 
        WHERE team2 != 'BYE'
        ORDER BY match_number
    """)
    matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/enter_follower_quali_results.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/save_follower_quali_result/<game_name>/<int:match_id>', methods=['POST'])
def save_follower_quali_result(game_name, match_id):
    """Follower Quali Ergebnis speichern"""
    score1 = int(request.form['score1'])
    score2 = int(request.form['score2'])
    
    if score1 > 42 or score2 > 42 or score1 < 0 or score2 < 0:
        return render_template("admin/error.html", 
                             error_message="Punktzahl muss zwischen 0 und 42 liegen!")
    
    if score1 == score2:
        return render_template("admin/error.html", 
                             error_message="Unentschieden nicht erlaubt!")
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM follower_quali_matches WHERE id = ?", (match_id,))
    match = cursor.fetchone()
    
    winner = match['team1'] if score1 > score2 else match['team2']
    
    cursor.execute("""
        UPDATE follower_quali_matches
        SET score1 = ?, score2 = ?, winner = ?
        WHERE id = ?
    """, (score1, score2, winner, match_id))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('enter_follower_quali_results', game_name=game_name))


@app.route('/generate_follower_cup/<game_name>')
def generate_follower_cup(game_name):
    """Follower Cup Hauptturnier generieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM follower_cup_matches")
    if cursor.fetchone()['count'] > 0:
        conn.close()
        return render_template("admin/error.html", 
                             error_message="Follower Cup wurde bereits generiert!")
    
    # Alle 16 FC-Teams direkt via get_phase_assignment — keine Quali nötig
    de_teams, fc_teams, plz_teams, best2, rest8 = get_phase_assignment(conn)

    if len(fc_teams) < 16:
        conn.close()
        return render_template("admin/error.html",
                             error_message=f"Nicht genug FC-Teams! Nur {len(fc_teams)} gefunden. Bitte alle Round Robin Ergebnisse eintragen.")

    # Sortiert nach goals_for (beste zuerst)
    cursor.execute("""
        SELECT r.team, r.goals_for, r.goal_difference
        FROM rankings r
        WHERE r.team IN ({})
        ORDER BY r.goals_for DESC, r.goal_difference DESC
    """.format(','.join('?' for _ in fc_teams)), list(fc_teams))
    all_teams = [row['team'] for row in cursor.fetchall()]
    
    all_teams = all_teams[:16]
    
    match_number = 230
    court = 1
    
    # 1/8-Finale
    for i in range(8):
        cursor.execute("""
            INSERT INTO follower_cup_matches 
            (match_number, round, match_index, team1, team2, court)
            VALUES (?, 'eighth', ?, ?, ?, ?)
        """, (match_number, i, all_teams[i], all_teams[15-i], court))
        match_number += 1
        court = (court % 15) + 1
    
    # 1/4-Finale
    for i in range(4):
        cursor.execute("""
            INSERT INTO follower_cup_matches 
            (match_number, round, match_index, team1, team2, court)
            VALUES (?, 'quarter', ?, NULL, NULL, ?)
        """, (match_number, i, court))
        match_number += 1
        court = (court % 15) + 1
    
    # Halbfinale
    for i in range(2):
        cursor.execute("""
            INSERT INTO follower_cup_matches 
            (match_number, round, match_index, team1, team2, court)
            VALUES (?, 'semi', ?, NULL, NULL, ?)
        """, (match_number, i, court))
        match_number += 1
        court = (court % 15) + 1
    
    # Finale
    cursor.execute("""
        INSERT INTO follower_cup_matches 
        (match_number, round, match_index, team1, team2, court)
        VALUES (?, 'final', 0, NULL, NULL, ?)
    """, (match_number, court))
    match_number += 1
    court = (court % 15) + 1
    
    # Platz 3
    cursor.execute("""
        INSERT INTO follower_cup_matches 
        (match_number, round, match_index, team1, team2, court)
        VALUES (?, 'third', 0, NULL, NULL, ?)
    """, (match_number, court))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('follower_cup_overview', game_name=game_name))


@app.route('/follower_cup_overview/<game_name>')
def follower_cup_overview(game_name):
    """Follower Cup Übersicht"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM follower_cup_matches 
        ORDER BY 
            CASE round 
                WHEN 'eighth' THEN 1 
                WHEN 'quarter' THEN 2 
                WHEN 'semi' THEN 3 
                WHEN 'final' THEN 4 
                WHEN 'third' THEN 5 
            END,
            match_index
    """)
    matches = cursor.fetchall()
    
    conn.close()
    
    matches_eighth = [m for m in matches if m['round'] == 'eighth']
    matches_quarter = [m for m in matches if m['round'] == 'quarter']
    matches_semi = [m for m in matches if m['round'] == 'semi']
    matches_final = [m for m in matches if m['round'] == 'final']
    matches_third = [m for m in matches if m['round'] == 'third']
    
    return render_template("admin/follower_cup_overview.html",
                         game_name=game_name,
                         matches_eighth=matches_eighth,
                         matches_quarter=matches_quarter,
                         matches_semi=matches_semi,
                         matches_final=matches_final,
                         matches_third=matches_third)


@app.route('/enter_follower_cup_results/<game_name>')
def enter_follower_cup_results(game_name):
    """Follower Cup Ergebnisse eintragen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM follower_cup_matches 
        WHERE team1 IS NOT NULL AND team2 IS NOT NULL
        ORDER BY 
            CASE round 
                WHEN 'eighth' THEN 1 
                WHEN 'quarter' THEN 2 
                WHEN 'semi' THEN 3 
                WHEN 'final' THEN 4 
                WHEN 'third' THEN 5 
            END,
            match_index
    """)
    matches = cursor.fetchall()
    
    conn.close()
    
    matches_eighth = [m for m in matches if m['round'] == 'eighth']
    matches_quarter = [m for m in matches if m['round'] == 'quarter']
    matches_semi = [m for m in matches if m['round'] == 'semi']
    matches_final = [m for m in matches if m['round'] == 'final']
    matches_third = [m for m in matches if m['round'] == 'third']
    
    return render_template("admin/enter_follower_cup_results.html",
                         game_name=game_name,
                         matches_eighth=matches_eighth,
                         matches_quarter=matches_quarter,
                         matches_semi=matches_semi,
                         matches_final=matches_final,
                         matches_third=matches_third)


@app.route('/save_follower_cup_result/<game_name>/<int:match_id>', methods=['POST'])
def save_follower_cup_result(game_name, match_id):
    """Follower Cup Ergebnis speichern"""
    score1 = int(request.form['score1'])
    score2 = int(request.form['score2'])
    
    if score1 > 42 or score2 > 42 or score1 < 0 or score2 < 0:
        return render_template("admin/error.html", 
                             error_message="Punktzahl muss zwischen 0 und 42 liegen!")
    
    if score1 == score2:
        return render_template("admin/error.html", 
                             error_message="Unentschieden nicht erlaubt!")
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM follower_cup_matches WHERE id = ?", (match_id,))
    match = cursor.fetchone()
    
    winner = match['team1'] if score1 > score2 else match['team2']
    loser = match['team2'] if score1 > score2 else match['team1']
    
    cursor.execute("""
        UPDATE follower_cup_matches
        SET score1 = ?, score2 = ?, winner = ?
        WHERE id = ?
    """, (score1, score2, winner, match_id))
    
    round_type = match['round']
    match_index = match['match_index']
    
    if round_type == 'eighth':
        next_match_index = match_index // 2
        slot = 'team1' if match_index % 2 == 0 else 'team2'
        cursor.execute(f"""
            UPDATE follower_cup_matches
            SET {slot} = ?
            WHERE round = 'quarter' AND match_index = ?
        """, (winner, next_match_index))
    
    elif round_type == 'quarter':
        next_match_index = match_index // 2
        slot = 'team1' if match_index % 2 == 0 else 'team2'
        cursor.execute(f"""
            UPDATE follower_cup_matches
            SET {slot} = ?
            WHERE round = 'semi' AND match_index = ?
        """, (winner, next_match_index))
    
    elif round_type == 'semi':
        slot_final = 'team1' if match_index == 0 else 'team2'
        slot_third = 'team1' if match_index == 0 else 'team2'
        cursor.execute(f"UPDATE follower_cup_matches SET {slot_final} = ? WHERE round = 'final'", (winner,))
        cursor.execute(f"UPDATE follower_cup_matches SET {slot_third} = ? WHERE round = 'third'", (loser,))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('enter_follower_cup_results', game_name=game_name))


@app.route('/reset_follower_cup/<game_name>', methods=['POST'])
def reset_follower_cup(game_name):
    """Follower Cup zurücksetzen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM follower_quali_matches")
    cursor.execute("DELETE FROM follower_cup_matches")
    conn.commit()
    conn.close()
    
    return redirect(url_for('game_overview', game_name=game_name))


# ============================================================================
# PLATZIERUNGSRUNDE
# ============================================================================

@app.route('/generate_placement_round/<game_name>')
def generate_placement_round(game_name):
    """Platzierungsrunde für Teams 37-60 generieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # PLZ Teams korrekt via get_phase_assignment
    de_teams_plz, fc_teams_plz, plz_teams_set, best2_plz, rest8_plz = get_phase_assignment(conn)

    if len(plz_teams_set) < 1:
        conn.close()
        return render_template("admin/error.html",
                             error_message="Keine Platzierungsrunden-Teams gefunden. Bitte alle Round Robin Ergebnisse eintragen.")

    cursor.execute("""
        SELECT r.team, r.goals_for, r.goal_difference, r.points
        FROM rankings r
        WHERE r.team IN ({})
        ORDER BY r.goals_for DESC, r.goal_difference DESC, r.points DESC
    """.format(','.join('?' for _ in plz_teams_set)), list(plz_teams_set))

    placement_teams = [row['team'] for row in cursor.fetchall()]
    
    match_number = 246
    court = 1
    
    num_teams = len(placement_teams)
    for i in range(num_teams // 2):
        team1 = placement_teams[i]
        team2 = placement_teams[-(i+1)]
        
        cursor.execute("""
            INSERT INTO placement_matches 
            (match_number, placement, team1, team2, court)
            VALUES (?, ?, ?, ?, ?)
        """, (match_number, f"P{37 + i*2}", team1, team2, court))
        
        match_number += 1
        court = (court % 15) + 1
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('placement_round_overview', game_name=game_name))


@app.route('/placement_round_overview/<game_name>')
def placement_round_overview(game_name):
    """Platzierungsrunde Übersicht"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM placement_matches ORDER BY match_number")
    matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/placement_round_overview.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/enter_placement_results/<game_name>')
def enter_placement_results(game_name):
    """Platzierungsrunde Ergebnisse eintragen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM placement_matches ORDER BY match_number")
    matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/enter_placement_results.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/save_placement_result/<game_name>/<int:match_id>', methods=['POST'])
def save_placement_result(game_name, match_id):
    """Platzierungsrunde Ergebnis speichern"""
    score1 = int(request.form['score1'])
    score2 = int(request.form['score2'])
    
    if score1 > 42 or score2 > 42 or score1 < 0 or score2 < 0:
        return render_template("admin/error.html", 
                             error_message="Punktzahl muss zwischen 0 und 42 liegen!")
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM placement_matches WHERE id = ?", (match_id,))
    match = cursor.fetchone()
    
    winner = match['team1'] if score1 > score2 else match['team2']
    
    cursor.execute("""
        UPDATE placement_matches
        SET score1 = ?, score2 = ?, winner = ?
        WHERE id = ?
    """, (score1, score2, winner, match_id))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('enter_placement_results', game_name=game_name))


@app.route('/reset_placement/<game_name>', methods=['POST'])
def reset_placement(game_name):
    """Platzierungsrunde zurücksetzen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM placement_matches")
    conn.commit()
    conn.close()
    
    return redirect(url_for('game_overview', game_name=game_name))


# ============================================================================
# TURNIER-KONFIGURATION
# ============================================================================

@app.route('/tournament_config/<game_name>')
def tournament_config(game_name):
    """Turnierkonfiguration anzeigen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM tournament_config WHERE game_name = ?", (game_name,))
    config = cursor.fetchone()
    
    conn.close()
    
    if not config:
        config = {
            'match_duration': 12,
            'break_between_games': 3,
            'break_between_rounds': 5,
            'start_time': '09:00'
        }
    
    return render_template("admin/tournament_config.html",
                         game_name=game_name,
                         config=config)


@app.route('/update_tournament_config/<game_name>', methods=['POST'])
def update_tournament_config(game_name):
    """Turnierkonfiguration speichern"""
    match_duration = int(request.form['match_duration'])
    break_between_games = int(request.form['break_between_games'])
    start_time = request.form['start_time']
    
    # NEU: Mittagspause
    lunch_break_enabled = 1 if request.form.get('lunch_break_enabled') == 'on' else 0
    lunch_break_start = request.form.get('lunch_break_start', '12:00')
    lunch_break_end = request.form.get('lunch_break_end', '13:00')
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE tournament_config
        SET match_duration = ?,
            break_between_games = ?,
            start_time = ?,
            lunch_break_enabled = ?,
            lunch_break_start = ?,
            lunch_break_end = ?
        WHERE game_name = ?
    """, (match_duration, break_between_games, start_time,
          lunch_break_enabled, lunch_break_start, lunch_break_end,
          game_name))
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('tournament_config', game_name=game_name))


# ============================================================================
# DISPLAY-BEREICH (Beamer/Publikum)
# ============================================================================

@app.route('/display/<game_name>')
def display_home(game_name):
    """Display Startseite"""
    return render_template("display/display_home.html", game_name=game_name)


@app.route('/display/<game_name>/groups')
def display_groups(game_name):
    """Gruppentabellen für Beamer"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    groups = {}
    for group_num in range(1, 11):
        cursor.execute("""
            SELECT r.*, t.is_ghost
            FROM rankings r
            LEFT JOIN teams t ON r.team = t.name
            WHERE r.group_number = ?
            ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC
        """, (group_num,))
        groups[group_num] = cursor.fetchall()
    
    conn.close()
    
    return render_template("display/display_groups.html",
                         game_name=game_name,
                         groups=groups)


@app.route('/display/<game_name>/round_robin')
def display_round_robin(game_name):
    """Round Robin Live Display - wechselt automatisch zu Bracket Standings"""
    return render_template("display/display_round_robin.html", game_name=game_name)


@app.route('/api/display/<game_name>/groups_json')
def api_groups_json(game_name):
    """JSON API: Aktuelle Gruppentabellen mit Phase-Feld fuer Live-Polling"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return jsonify({})
    conn = get_db_connection(db_path)

    # Phasenzuteilung berechnen
    try:
        de_teams, fc_teams, plz_teams, best2, rest8 = get_phase_assignment(conn)
        wc_teams = set(f['team'] for f in best2)
    except Exception:
        de_teams = fc_teams = plz_teams = wc_teams = set()

    def get_phase(team):
        if team in wc_teams:  return 'DE*'
        if team in de_teams:  return 'DE'
        if team in fc_teams:  return 'FC'
        return 'PLZ'

    cursor = conn.cursor()
    groups = {}
    for group_num in range(1, 11):
        cursor.execute("""
            SELECT r.team, r.points, r.goal_difference, r.goals_for,
                   r.matches_played, r.wins, r.losses
            FROM rankings r
            LEFT JOIN teams t ON r.team = t.name
            WHERE r.group_number = ? AND (t.is_ghost IS NULL OR t.is_ghost = 0)
            ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC
        """, (group_num,))
        rows = cursor.fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d['phase'] = get_phase(d['team'])
            result.append(d)
        groups[group_num] = result

    conn.close()
    return jsonify(groups)


@app.route('/api/display/<game_name>/bracket_standings_json')
def api_bracket_standings_json(game_name):
    """JSON API: Bracket Standings fuer Live-Polling"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return jsonify({'A': [], 'B': []})
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    result = {}
    for bracket, table in [('A', 'double_elim_matches_a'), ('B', 'double_elim_matches_b')]:
        cursor.execute(f"""
            SELECT DISTINCT team1 as team FROM {table} WHERE team1 IS NOT NULL AND team1 != ''
            UNION
            SELECT DISTINCT team2 as team FROM {table} WHERE team2 IS NOT NULL AND team2 != ''
        """)
        all_teams = set(row['team'] for row in cursor.fetchall())
        cursor.execute(f"""
            SELECT DISTINCT loser as team FROM {table}
            WHERE bracket = 'Losers' AND loser IS NOT NULL AND loser != ''
        """)
        eliminated = set(row['team'] for row in cursor.fetchall())
        team_status = {}
        for team in all_teams:
            cursor.execute(f"""
                SELECT MAX(round) as max_round, bracket
                FROM {table}
                WHERE (team1 = ? OR team2 = ?) AND winner IS NOT NULL
                GROUP BY bracket
                ORDER BY MAX(round) DESC
                LIMIT 1
            """, (team, team))
            row = cursor.fetchone()
            if team in eliminated:
                status = "Ausgeschieden"
            elif row:
                b = "WB" if row['bracket'] == 'Winners' else "LB"
                status = f"{b} R{row['max_round']}"
            else:
                status = "WB R1"
            team_status[team] = {'team': team, 'status': status, 'eliminated': team in eliminated}
        def sort_key(t):
            if not t['eliminated']:
                return (0, -int(t['status'].split('R')[-1]) if 'R' in t['status'] else 0)
            return (1, 0)
        result[bracket] = sorted(team_status.values(), key=sort_key)
    conn.close()
    return jsonify(result)


@app.route('/display/<game_name>/qualification_tree')
def display_qualification_tree(game_name):
    """Qualifikationsbaum für Beamer"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    groups_a = {}
    for group_num in range(1, 6):
        cursor.execute("""
            SELECT team FROM rankings 
            WHERE group_number = ?
            AND team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
            ORDER BY points DESC, goal_difference DESC, goals_for DESC
            LIMIT 3
        """, (group_num,))
        groups_a[group_num] = [row['team'] for row in cursor.fetchall()]
    
    groups_b = {}
    for group_num in range(6, 11):
        cursor.execute("""
            SELECT team FROM rankings 
            WHERE group_number = ?
            AND team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
            ORDER BY points DESC, goal_difference DESC, goals_for DESC
            LIMIT 3
        """, (group_num,))
        groups_b[group_num] = [row['team'] for row in cursor.fetchall()]
    
    cursor.execute("""
        SELECT r.team, r.group_number
        FROM rankings r
        WHERE (SELECT COUNT(*) FROM rankings r2 
               WHERE r2.group_number = r.group_number 
               AND (r2.points > r.points 
                    OR (r2.points = r.points AND r2.goal_difference > r.goal_difference)
                    OR (r2.points = r.points AND r2.goal_difference = r.goal_difference 
                        AND r2.goals_for > r.goals_for))) = 3
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC
        LIMIT 2
    """)
    best_4th = cursor.fetchall()
    
    conn.close()
    
    return render_template("display/display_qualification_tree.html",
                         game_name=game_name,
                         groups_a=groups_a,
                         groups_b=groups_b,
                         best_4th=best_4th)


@app.route('/display/<game_name>/brackets')
def display_brackets(game_name):
    """Double Elimination Brackets für Beamer"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM double_elim_matches_a 
        ORDER BY round, bracket DESC, match_index
    """)
    matches_a = cursor.fetchall()
    
    cursor.execute("""
        SELECT * FROM double_elim_matches_b 
        ORDER BY round, bracket DESC, match_index
    """)
    matches_b = cursor.fetchall()
    
    conn.close()
    
    return render_template("display/display_brackets.html",
                         game_name=game_name,
                         matches_a=matches_a,
                         matches_b=matches_b)


@app.route('/display/<game_name>/super_finals')
def display_super_finals(game_name):
    """Super Finals für Beamer"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM super_finals_matches 
        ORDER BY 
            CASE match_id 
                WHEN 'HF1' THEN 1 
                WHEN 'HF2' THEN 2 
                WHEN 'FINAL' THEN 3 
                WHEN 'THIRD' THEN 4 
            END
    """)
    matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("display/display_super_finals.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/display/<game_name>/follower_cup')
def display_follower_cup(game_name):
    """Follower Cup für Beamer"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM follower_cup_matches 
        ORDER BY 
            CASE round 
                WHEN 'eighth' THEN 1 
                WHEN 'quarter' THEN 2 
                WHEN 'semi' THEN 3 
                WHEN 'final' THEN 4 
                WHEN 'third' THEN 5 
            END,
            match_index
    """)
    matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("display/display_follower_cup.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/display/<game_name>/slideshow')
def display_slideshow(game_name):
    """Automatischer Slideshow-Durchlauf"""
    return render_template("display/display_slideshow.html",
                         game_name=game_name)


# ============================================================================
# MELDEBLATT PDF EXPORT
# ============================================================================

@app.route('/print_matches/<game_name>')
def print_matches(game_name):
    """Generiert PDF mit Meldeblättern - 2 pro A4-Seite"""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas as rl_canvas

    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT match_number, team1, team2, time, field, group_number, round
        FROM matches
        WHERE match_number IS NOT NULL
        ORDER BY match_number
    """)
    matches = cursor.fetchall()
    conn.close()

    if not matches:
        return render_template("admin/error.html",
                               error_message="Keine Spiele gefunden! Bitte zuerst Spielplan generieren.")

    def draw_match_card(c, x, y, w, h, match):
        pad = 6*mm
        c.setStrokeColor(colors.black)
        c.setLineWidth(1.5)
        c.rect(x, y, w, h)
        header_h = 14*mm
        c.setLineWidth(1)
        c.line(x, y + h - header_h, x + w, y + h - header_h)
        c.setFont("Helvetica-Bold", 20)
        c.setFillColor(colors.black)
        c.drawString(x + pad, y + h - header_h + 3*mm, f"Spiel #{match['match_number']}")
        c.setFont("Helvetica", 10)
        info = f"Gruppe {match['group_number']}  |  Runde {match['round']}"
        c.drawRightString(x + w - pad, y + h - header_h + 4*mm, info)
        mid_y = y + h - header_h - 16*mm
        c.setFont("Helvetica-Bold", 11)
        c.drawString(x + pad, mid_y + 8*mm, "Zeit:")
        c.drawString(x + pad, mid_y, "Feld:")
        c.setFont("Helvetica", 14)
        time_str = match['time'] if match['time'] else '-'
        field_str = str(match['field']) if match['field'] else '-'
        c.drawString(x + 30*mm, mid_y + 8*mm, time_str)
        c.drawString(x + 30*mm, mid_y, field_str)
        teams_y = y + h - header_h - 34*mm
        c.setLineWidth(0.5)
        c.setStrokeColor(colors.grey)
        c.line(x + pad, teams_y + 18*mm, x + w - pad, teams_y + 18*mm)
        c.setStrokeColor(colors.black)
        c.setFont("Helvetica-Bold", 15)
        c.drawString(x + pad, teams_y + 10*mm, match['team1'] or '-')
        c.setFont("Helvetica", 10)
        c.drawString(x + pad, teams_y + 3*mm, "Punkte:")
        c.setLineWidth(0.8)
        c.line(x + 28*mm, teams_y + 3*mm, x + 60*mm, teams_y + 3*mm)
        c.setFont("Helvetica-Bold", 11)
        c.setFillColor(colors.grey)
        c.drawCentredString(x + w/2, teams_y - 2*mm, "vs.")
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 15)
        c.drawString(x + pad, teams_y - 10*mm, match['team2'] or '-')
        c.setFont("Helvetica", 10)
        c.drawString(x + pad, teams_y - 17*mm, "Punkte:")
        c.setLineWidth(0.8)
        c.line(x + 28*mm, teams_y - 17*mm, x + 60*mm, teams_y - 17*mm)
        sig_y = y + 8*mm
        c.setFont("Helvetica", 8)
        c.setFillColor(colors.grey)
        c.drawString(x + pad, sig_y + 4*mm, "Unterschrift Schiedsrichter:")
        c.setLineWidth(0.5)
        c.line(x + 60*mm, sig_y + 4*mm, x + w - pad, sig_y + 4*mm)
        c.setFillColor(colors.black)

    buf = io.BytesIO()
    width, height = A4
    c = rl_canvas.Canvas(buf, pagesize=A4)
    margin = 10*mm
    card_w = width - 2*margin
    card_h = (height - 3*margin) / 2

    for i, match in enumerate(matches):
        pos = i % 2
        if pos == 0 and i > 0:
            c.showPage()
        y_pos = margin if pos == 1 else margin + card_h + margin
        draw_match_card(c, margin, y_pos, card_w, card_h, match)

    c.save()
    buf.seek(0)
    return send_file(
        buf,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f"{game_name}_meldeblätter.pdf"
    )


@app.route('/print_ko_matches/<game_name>')
def print_ko_matches(game_name):
    """Meldeblätter KO-Phase: 2 Karten pro Seite mit fixem Zeitplan"""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas as rl_canvas

    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")

    T1  = "13:20"
    T2  = add_minutes_to_time(T1, 50)
    T3  = add_minutes_to_time(T2, 25)
    T4  = add_minutes_to_time(T3, 25)
    T5  = add_minutes_to_time(T4, 25)
    T6  = add_minutes_to_time(T5, 25)
    T7  = add_minutes_to_time(T6, 25)
    T8  = add_minutes_to_time(T7, 25)
    T9  = add_minutes_to_time(T8, 25)
    T10 = add_minutes_to_time(T9, 25)
    T11 = add_minutes_to_time(T10, 25)

    FIXED_SCHEDULE = [
        *[(151+i, 'Winner Bracket', '1/16-Finale',  T1, i+1)             for i in range(16)],
        *[(167+i, 'Winner Bracket', '1/8-Finale',   T2, [1,2,3,4,13,14,15,16][i]) for i in range(8)],
        *[(175+i, 'Loser Bracket',  'LB R1',         T2, 5+i)             for i in range(8)],
        *[(183+i, 'Winner Bracket', 'Viertelfinale', T4, 9+i)             for i in range(4)],
        *[(187+i, 'Loser Bracket',  'LB R2',         T4, 1+i)             for i in range(8)],
        *[(195+i, 'Loser Bracket',  'LB R3',         T5, 5+i)             for i in range(4)],
        *[(199+i, 'Winner Bracket', 'Halbfinale',    T6, 11+i)            for i in range(2)],
        *[(201+i, 'Loser Bracket',  'LB R4',         T6, 1+i)             for i in range(4)],
        *[(205+i, 'Loser Bracket',  'LB R5',         T7, 3+i)             for i in range(2)],
        (207,     'Winner Bracket', 'WB Final',      T8, 7),
        *[(208+i, 'Loser Bracket',  'LB R6',         T8, 4+i)             for i in range(2)],
        (210,     'Loser Bracket',  'LB R7',         T9,  4),
        (211,     'Loser Bracket',  'LB R8',         T10, 5),
        (212,     'Loser Bracket',  'LB Final',      T11, 5),
        *[(230+i, 'Follower Cup',   '1/8-Finale',    T3, 9+i)             for i in range(8)],
        *[(238+i, 'Follower Cup',   'Viertelfinale', T4, 13+i)            for i in range(4)],
        *[(242+i, 'Follower Cup',   'Halbfinale',    T5, 9+i)             for i in range(2)],
        (244,     'Follower Cup',   'Platz 3',       T6, 7),
        (245,     'Follower Cup',   'Final',         T6, 8),
        *[(246+i, 'Platzierungsrunde', f'P{49+i*2}', T3, 1+i)            for i in range(6)],
    ]
    FIXED_SCHEDULE.sort(key=lambda x: x[0])

    KAT_COLORS = {
        'Winner Bracket':    colors.HexColor('#1a6b3a'),
        'Loser Bracket':     colors.HexColor('#7a3000'),
        'Follower Cup':      colors.HexColor('#7a5200'),
        'Platzierungsrunde': colors.HexColor('#6b1a1a'),
    }

    buf = io.BytesIO()
    W, H = A4
    c = rl_canvas.Canvas(buf, pagesize=A4)
    margin = 10*mm
    card_w = W - 2*margin
    card_h = (H - 3*margin) / 2

    def draw_ko_card(c, x, y, w, h, match):
        pad = 6*mm
        kat = match['kategorie']
        kat_color = KAT_COLORS.get(kat, colors.black)
        bh = 13*mm

        c.setStrokeColor(kat_color)
        c.setLineWidth(2)
        c.rect(x, y, w, h)

        c.setFillColor(kat_color)
        c.rect(x, y+h-bh, w, bh, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont('Helvetica-Bold', 10)
        c.drawString(x+pad, y+h-bh+4*mm, kat.upper())
        c.setFont('Helvetica', 9)
        c.drawRightString(x+w-pad, y+h-bh+4*mm, match['runde'])

        c.setFillColor(colors.black)
        c.setFont('Helvetica-Bold', 30)
        c.drawString(x+pad, y+h-bh-17*mm, f"#{match['match_number']}")

        box_y = y+h-bh-30*mm
        box_h = 12*mm
        c.setFillColor(colors.HexColor('#f0f4fa'))
        c.setStrokeColor(kat_color)
        c.setLineWidth(0.8)
        c.rect(x+pad, box_y, 45*mm, box_h, fill=1, stroke=1)
        c.setFillColor(colors.HexColor('#888888'))
        c.setFont('Helvetica', 7)
        c.drawString(x+pad+2*mm, box_y+8*mm, 'SPIELZEIT')
        c.setFillColor(kat_color)
        c.setFont('Helvetica-Bold', 16)
        c.drawString(x+pad+2*mm, box_y+1.5*mm, str(match['time']))

        c.setFillColor(colors.HexColor('#f0f4fa'))
        c.setStrokeColor(kat_color)
        c.rect(x+pad+50*mm, box_y, 30*mm, box_h, fill=1, stroke=1)
        c.setFillColor(colors.HexColor('#888888'))
        c.setFont('Helvetica', 7)
        c.drawString(x+pad+52*mm, box_y+8*mm, 'FELD')
        c.setFillColor(kat_color)
        c.setFont('Helvetica-Bold', 16)
        c.drawString(x+pad+52*mm, box_y+1.5*mm, str(match['court']))

        sep_y = box_y - 5*mm
        c.setStrokeColor(colors.HexColor('#dddddd'))
        c.setLineWidth(0.5)
        c.line(x+pad, sep_y, x+w-pad, sep_y)

        t1_y = sep_y - 12*mm
        c.setFillColor(kat_color)
        c.setFont('Helvetica-Bold', 8)
        c.drawString(x+pad, t1_y+5*mm, 'TEAM 1')
        c.setStrokeColor(colors.black)
        c.setLineWidth(0.8)
        c.line(x+pad, t1_y, x+w-pad-38*mm, t1_y)
        c.setFillColor(colors.HexColor('#f8f8f8'))
        c.setStrokeColor(colors.HexColor('#aaaaaa'))
        c.setLineWidth(0.5)
        c.rect(x+w-pad-35*mm, t1_y-3.5*mm, 35*mm, 8*mm, fill=1, stroke=1)
        c.setFillColor(colors.HexColor('#999999'))
        c.setFont('Helvetica', 7)
        c.drawCentredString(x+w-pad-17.5*mm, t1_y-0.5*mm, 'Punkte')

        vs_y = t1_y - 8*mm
        c.setFillColor(colors.HexColor('#bbbbbb'))
        c.setFont('Helvetica-BoldOblique', 9)
        c.drawCentredString(x+w/2, vs_y, '— vs. —')

        t2_y = vs_y - 8*mm
        c.setFillColor(kat_color)
        c.setFont('Helvetica-Bold', 8)
        c.drawString(x+pad, t2_y+5*mm, 'TEAM 2')
        c.setStrokeColor(colors.black)
        c.setLineWidth(0.8)
        c.line(x+pad, t2_y, x+w-pad-38*mm, t2_y)
        c.setFillColor(colors.HexColor('#f8f8f8'))
        c.setStrokeColor(colors.HexColor('#aaaaaa'))
        c.setLineWidth(0.5)
        c.rect(x+w-pad-35*mm, t2_y-3.5*mm, 35*mm, 8*mm, fill=1, stroke=1)
        c.setFillColor(colors.HexColor('#999999'))
        c.setFont('Helvetica', 7)
        c.drawCentredString(x+w-pad-17.5*mm, t2_y-0.5*mm, 'Punkte')

        sig_y = y + 6*mm
        c.setFillColor(colors.HexColor('#aaaaaa'))
        c.setFont('Helvetica', 7.5)
        c.drawString(x+pad, sig_y+3*mm, 'Unterschrift Schiedsrichter:')
        c.setStrokeColor(colors.HexColor('#bbbbbb'))
        c.setLineWidth(0.4)
        c.line(x+57*mm, sig_y+3*mm, x+w-pad, sig_y+3*mm)

    matches = [{'match_number': s[0], 'kategorie': s[1], 'runde': s[2],
                'time': s[3], 'court': str(s[4])} for s in FIXED_SCHEDULE]

    for i, match in enumerate(matches):
        pos = i % 2
        if pos == 0 and i > 0:
            c.showPage()
        y_pos = margin if pos == 1 else margin + card_h + margin
        draw_ko_card(c, margin, y_pos, card_w, card_h, match)

    c.save()
    buf.seek(0)
    return send_file(buf, mimetype='application/pdf', as_attachment=True,
                     download_name=f"{game_name}_ko_meldeblätter.pdf")


@app.route('/spielplan_pdf/<game_name>')
def spielplan_pdf(game_name):
    """Spielplan als PDF — 4 Seiten A4 Querformat:
       Seite 1: Bracket A Runden 1-2-3
       Seite 2: Bracket A Runden 4-5
       Seite 3: Bracket B Runden 1-2-3
       Seite 4: Bracket B Runden 4-5
    """
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas as rl_canvas

    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT match_number, team1, team2, time, field, group_number, round
        FROM matches WHERE match_number IS NOT NULL
        ORDER BY group_number, round, field
    """)
    matches = cursor.fetchall()
    conn.close()

    if not matches:
        return render_template("admin/error.html",
                               error_message="Keine Spiele gefunden! Bitte zuerst Spielplan generieren.")

    # data[group][round] = [m1, m2, m3]
    data = {}
    for m in matches:
        g, r = m['group_number'], m['round']
        data.setdefault(g, {}).setdefault(r, []).append(dict(m))

    rounds_all = sorted(set(m['round'] for m in matches))

    # Farben
    C_HDR   = colors.HexColor('#1e3a5f')
    C_GHDR  = colors.HexColor('#2d5a9e')
    C_WHT   = colors.white
    C_EVEN  = colors.HexColor('#eef3ff')
    C_ODD   = colors.HexColor('#f8faff')
    C_BRD   = colors.HexColor('#b0bcd4')
    C_VS    = colors.HexColor('#999999')
    C_NR    = colors.HexColor('#bbbbbb')
    C_TIME  = colors.HexColor('#a8c4e8')
    C_FELD  = colors.HexColor('#e8f0ff')
    C_A     = colors.HexColor('#16a34a')
    C_B     = colors.HexColor('#2563eb')

    def trunc(text, chars):
        if not text: return '–'
        return text if len(text) <= chars else text[:chars-1] + '…'

    def get_time(groups, rnd):
        for g in groups:
            ms = data.get(g, {}).get(rnd, [])
            if ms and ms[0].get('time'):
                return ms[0]['time']
        return '–'

    page_w, page_h = landscape(A4)
    buf = io.BytesIO()
    cv = rl_canvas.Canvas(buf, pagesize=landscape(A4))

    def draw_page(bracket_groups, rounds_on_page, bracket_name, bracket_color, sub_text, page_label):
        margin    = 9 * mm
        title_h   = 13 * mm
        num_r     = len(rounds_on_page)
        num_g     = len(bracket_groups)

        GHDR_W   = 20 * mm   # Gruppen-Spalte links
        RHDR_H   = 13 * mm   # Runden-Zeile oben

        table_top = page_h - margin - title_h
        table_bot = margin + 5 * mm
        avail_h   = table_top - table_bot
        avail_w   = page_w - 2 * margin - GHDR_W

        col_w = avail_w / num_r
        row_h = (avail_h - RHDR_H) / num_g

        # ── Titel ─────────────────────────────────────────────────────────
        cv.setFillColor(C_HDR)
        cv.setFont('Helvetica-Bold', 12)
        cv.drawString(margin, page_h - margin - 7.5*mm,
                      f'Round Robin Spielplan — {bracket_name}  [{page_label}]')
        cv.setFont('Helvetica', 7)
        cv.setFillColor(colors.HexColor('#555555'))
        cv.drawString(margin, page_h - margin - 12*mm, sub_text)

        # Bracket-Farbpunkt
        cv.setFillColor(bracket_color)
        cv.circle(page_w - margin - 4*mm, page_h - margin - 8*mm, 3*mm, fill=1, stroke=0)

        # ── Runden-Header ─────────────────────────────────────────────────
        # Ecke
        cv.setFillColor(C_HDR)
        cv.rect(margin, table_top - RHDR_H, GHDR_W, RHDR_H, fill=1, stroke=0)
        cv.setFillColor(C_TIME); cv.setFont('Helvetica-Bold', 8)
        cv.drawCentredString(margin + GHDR_W/2, table_top - RHDR_H/2 - 1.5*mm, 'GRUPPE')

        for ci, rnd in enumerate(rounds_on_page):
            x = margin + GHDR_W + ci * col_w
            t = get_time(bracket_groups, rnd)

            cv.setFillColor(C_HDR)
            cv.rect(x, table_top - RHDR_H, col_w, RHDR_H, fill=1, stroke=0)
            cv.setFillColor(C_WHT)
            cv.setFont('Helvetica-Bold', 10)
            cv.drawCentredString(x + col_w/2, table_top - RHDR_H + 6.5*mm, f'Runde {rnd}')
            cv.setFillColor(C_TIME)
            cv.setFont('Helvetica', 8.5)
            cv.drawCentredString(x + col_w/2, table_top - RHDR_H + 2*mm, t)

        # ── Gruppen-Zeilen ─────────────────────────────────────────────────
        for gi, grp in enumerate(bracket_groups):
            row_y   = table_top - RHDR_H - (gi + 1) * row_h
            cell_bg = C_EVEN if gi % 2 == 0 else C_ODD

            # Gruppen-Label
            cv.setFillColor(C_GHDR)
            cv.rect(margin, row_y, GHDR_W, row_h, fill=1, stroke=0)
            cv.setFillColor(colors.HexColor('#8ab0d8'))
            cv.setFont('Helvetica', 6.5)
            cv.drawCentredString(margin + GHDR_W/2, row_y + row_h * 0.73, 'GRUPPE')
            cv.setFillColor(C_WHT)
            cv.setFont('Helvetica-Bold', 20)
            cv.drawCentredString(margin + GHDR_W/2, row_y + row_h * 0.28, str(grp))

            # Zellen
            for ci, rnd in enumerate(rounds_on_page):
                x = margin + GHDR_W + ci * col_w
                cv.setFillColor(cell_bg)
                cv.rect(x, row_y, col_w, row_h, fill=1, stroke=0)

                spiele = data.get(grp, {}).get(rnd, [])
                if not spiele:
                    cv.setFillColor(colors.HexColor('#cccccc'))
                    cv.setFont('Helvetica', 8)
                    cv.drawCentredString(x + col_w/2, row_y + row_h/2, '–')
                    continue

                # 3 Spiele pro Zelle gleichmässig aufteilen
                n       = len(spiele)
                pad_x   = 3 * mm
                pad_y   = 2 * mm
                slot_h  = (row_h - 2 * pad_y) / n

                # Max Zeichenlänge für Teamnamen
                max_ch = max(8, int(col_w / 5.2))

                for si, m in enumerate(spiele):
                    slot_y = row_y + pad_y + (n - 1 - si) * slot_h
                    cy     = slot_y + slot_h / 2  # Mitte des Slots

                    # Trennlinie zwischen Spielen
                    if si > 0:
                        cv.setStrokeColor(colors.HexColor('#d0d8ef'))
                        cv.setLineWidth(0.35)
                        cv.line(x + 2*mm, slot_y + slot_h, x + col_w - 2*mm, slot_y + slot_h)

                    # Feld + Nummer — kompakt oben im Slot
                    feld = m.get('field', '?')
                    top_y = cy + slot_h * 0.32

                    # Feld-Badge klein
                    bw, bh = 8.5*mm, 3.5*mm
                    cv.setFillColor(C_HDR)
                    cv.roundRect(x + pad_x, top_y - bh/2, bw, bh, 0.8*mm, fill=1, stroke=0)
                    cv.setFillColor(C_WHT)
                    cv.setFont('Helvetica-Bold', 5)
                    cv.drawCentredString(x + pad_x + bw/2, top_y - 1.2*mm, f'Feld {feld}')

                    # Spielnummer rechts
                    cv.setFillColor(C_NR)
                    cv.setFont('Helvetica', 5)
                    cv.drawRightString(x + col_w - pad_x, top_y - 0.5*mm, f'#{m["match_number"]}')

                    # Team 1
                    t1y = cy - 0.5*mm
                    cv.setFillColor(colors.HexColor('#111111'))
                    cv.setFont('Helvetica-Bold', 7)
                    cv.drawString(x + pad_x, t1y, trunc(m['team1'], max_ch))

                    # vs
                    vsy = cy - 3.8*mm
                    cv.setFillColor(C_VS)
                    cv.setFont('Helvetica-Oblique', 5.5)
                    cv.drawString(x + pad_x, vsy, 'vs.')

                    # Team 2
                    t2y = cy - 7*mm
                    cv.setFillColor(colors.HexColor('#111111'))
                    cv.setFont('Helvetica-Bold', 7)
                    cv.drawString(x + pad_x, t2y, trunc(m['team2'], max_ch))

        # ── Gitter ──────────────────────────────────────────────────────────
        tw = GHDR_W + num_r * col_w
        th = RHDR_H + num_g * row_h
        cv.setStrokeColor(C_BRD); cv.setLineWidth(0.35)

        for gi in range(num_g + 1):
            yy = table_top - RHDR_H - gi * row_h
            cv.line(margin, yy, margin + tw, yy)
        for ci in range(num_r + 1):
            xx = margin + GHDR_W + ci * col_w
            cv.line(xx, table_top - th, xx, table_top)
        cv.line(margin, table_top - th, margin, table_top)

        # Äusserer Rahmen
        cv.setStrokeColor(C_HDR); cv.setLineWidth(1.5)
        cv.rect(margin, table_top - th, tw, th)

        # Gruppen-Trennlinie
        cv.setStrokeColor(C_GHDR); cv.setLineWidth(0.8)
        cv.line(margin + GHDR_W, table_top - th, margin + GHDR_W, table_top)

        # ── Footer ───────────────────────────────────────────────────────────
        cv.setFont('Helvetica', 6.5); cv.setFillColor(colors.HexColor('#888888'))
        cv.drawString(margin, margin / 2 + 0.5*mm, game_name)
        cv.drawRightString(page_w - margin, margin / 2 + 0.5*mm,
                           f'Erstellt: {datetime.now().strftime("%d.%m.%Y %H:%M")}')

    # ── 4 Seiten ──────────────────────────────────────────────────────────────
    # Aufteilen: 3 + 2 Runden (oder anpassen)
    r_split = 3   # erste n Runden auf Seite 1, Rest auf Seite 2

    for bracket_groups, bname, bcolor, bsub in [
        (list(range(1,  6)), 'Bracket A (Gruppen 1–5)',  C_A,
         'Gruppen 1–5  |  Top 3 jeder Gruppe + 2 beste Viertplatzierte → Double Elimination'),
        (list(range(6, 11)), 'Bracket B (Gruppen 6–10)', C_B,
         'Gruppen 6–10  |  Top 3 jeder Gruppe + 2 beste Viertplatzierte → Double Elimination'),
    ]:
        runds_1 = rounds_all[:r_split]
        runds_2 = rounds_all[r_split:]

        draw_page(bracket_groups, runds_1, bname, bcolor, bsub,
                  f'Runden {runds_1[0]}–{runds_1[-1]}')
        cv.showPage()

        if runds_2:
            draw_page(bracket_groups, runds_2, bname, bcolor, bsub,
                      f'Runden {runds_2[0]}–{runds_2[-1]}')
            cv.showPage()

    cv.save()
    buf.seek(0)
    return send_file(
        buf,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f'{game_name}_spielplan.pdf'
    )


# ============================================================================
# EXPORT-FUNKTIONEN
# ============================================================================

@app.route('/export_rankings/<game_name>')
def export_rankings(game_name):
    """Rankings als CSV exportieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM rankings 
        ORDER BY group_number, points DESC, goal_difference DESC
    """)
    rankings = cursor.fetchall()
    
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['Gruppe', 'Team', 'Spiele', 'Siege', 'Unentschieden', 'Niederlagen',
                    'Tore+', 'Tore-', 'Differenz', 'Punkte'])
    
    for rank in rankings:
        writer.writerow([
            rank['group_number'], rank['team'], rank['matches_played'],
            rank['wins'], rank['draws'], rank['losses'],
            rank['goals_for'], rank['goals_against'],
            rank['goal_difference'], rank['points']
        ])
    
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{game_name}_rankings.csv'
    )


@app.route('/export_complete/<game_name>')
def export_complete(game_name):
    """Vollständiger Export aller Turnierdaten"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['TURNIER-STATISTIKEN'])
    writer.writerow(['Turniername:', game_name])
    writer.writerow([])
    
    writer.writerow(['GRUPPENTABELLEN'])
    writer.writerow([])
    
    for group_num in range(1, 11):
        writer.writerow([f'Gruppe {group_num}'])
        writer.writerow(['Platz', 'Team', 'Spiele', 'S', 'U', 'N', 'Tore+', 'Tore-', 'Diff', 'Punkte'])
        
        cursor.execute("""
            SELECT r.*, t.is_ghost
            FROM rankings r
            LEFT JOIN teams t ON r.team = t.name
            WHERE r.group_number = ?
            ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC
        """, (group_num,))
        
        position = 1
        for rank in cursor.fetchall():
            ghost_marker = ' (Ghost)' if rank['is_ghost'] else ''
            writer.writerow([
                position, 
                rank['team'] + ghost_marker,
                rank['matches_played'],
                rank['wins'],
                rank['draws'],
                rank['losses'],
                rank['goals_for'],
                rank['goals_against'],
                rank['goal_difference'],
                rank['points']
            ])
            position += 1
        
        writer.writerow([])
    
    writer.writerow(['BRACKET A - DOUBLE ELIMINATION'])
    writer.writerow(['Spiel#', 'Runde', 'Bracket', 'Team 1', 'Score 1', 'Score 2', 'Team 2', 'Gewinner'])
    
    cursor.execute("SELECT * FROM double_elim_matches_a ORDER BY round, bracket DESC, match_index")
    for match in cursor.fetchall():
        writer.writerow([
            match['match_number'] or '',
            match['round'],
            match['bracket'],
            match['team1'] or 'TBD',
            match['score1'] or '',
            match['score2'] or '',
            match['team2'] or 'TBD',
            match['winner'] or ''
        ])
    
    writer.writerow([])
    
    writer.writerow(['BRACKET B - DOUBLE ELIMINATION'])
    writer.writerow(['Spiel#', 'Runde', 'Bracket', 'Team 1', 'Score 1', 'Score 2', 'Team 2', 'Gewinner'])
    
    cursor.execute("SELECT * FROM double_elim_matches_b ORDER BY round, bracket DESC, match_index")
    for match in cursor.fetchall():
        writer.writerow([
            match['match_number'] or '',
            match['round'],
            match['bracket'],
            match['team1'] or 'TBD',
            match['score1'] or '',
            match['score2'] or '',
            match['team2'] or 'TBD',
            match['winner'] or ''
        ])
    
    conn.close()
    
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{game_name}_complete.csv'
    )


# ============================================================================
# DEBUG-ROUTEN
# ============================================================================

@app.route('/debug_bracket/<game_name>/<bracket_id>')
def debug_bracket(game_name, bracket_id):
    """Debug-Informationen für Bracket"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    table = f"double_elim_matches_{bracket_id.lower()}"
    
    cursor.execute(f"SELECT * FROM {table} ORDER BY round, bracket DESC, match_index")
    matches = cursor.fetchall()
    
    conn.close()
    
    output = f"<h1>Debug Bracket {bracket_id.upper()}</h1>"
    output += "<table border='1'><tr><th>ID</th><th>Match#</th><th>Round</th><th>Bracket</th><th>Index</th><th>Team1</th><th>Score1</th><th>Score2</th><th>Team2</th><th>Winner</th><th>Loser</th></tr>"
    
    for match in matches:
        output += f"<tr>"
        output += f"<td>{match['id']}</td>"
        output += f"<td>{match['match_number'] or 'N/A'}</td>"
        output += f"<td>{match['round']}</td>"
        output += f"<td>{match['bracket']}</td>"
        output += f"<td>{match['match_index']}</td>"
        output += f"<td>{match['team1'] or 'TBD'}</td>"
        output += f"<td>{match['score1'] or '-'}</td>"
        output += f"<td>{match['score2'] or '-'}</td>"
        output += f"<td>{match['team2'] or 'TBD'}</td>"
        output += f"<td>{match['winner'] or '-'}</td>"
        output += f"<td>{match['loser'] or '-'}</td>"
        output += f"</tr>"
    
    output += "</table>"
    
    return output


@app.route('/debug_super_finals/<game_name>')
def debug_super_finals(game_name):
    """Debug-Informationen für Super Finals"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM super_finals_matches ORDER BY id")
    matches = cursor.fetchall()
    
    conn.close()
    
    output = "<h1>Debug Super Finals</h1>"
    output += "<table border='1'><tr><th>ID</th><th>Match#</th><th>MatchID</th><th>Team1</th><th>Score1</th><th>Score2</th><th>Team2</th><th>Winner</th></tr>"
    
    for match in matches:
        output += f"<tr>"
        output += f"<td>{match['id']}</td>"
        output += f"<td>{match['match_number'] or 'N/A'}</td>"
        output += f"<td>{match['match_id']}</td>"
        output += f"<td>{match['team1'] or 'TBD'}</td>"
        output += f"<td>{match['score1'] or '-'}</td>"
        output += f"<td>{match['score2'] or '-'}</td>"
        output += f"<td>{match['team2'] or 'TBD'}</td>"
        output += f"<td>{match['winner'] or '-'}</td>"
        output += f"</tr>"
    
    output += "</table>"
    
    return output


@app.route('/debug_follower_cup/<game_name>')
def debug_follower_cup(game_name):
    """Debug-Informationen für Follower Cup"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    output = "<h1>Debug Follower Cup</h1>"
    
    output += "<h2>Qualifikation</h2>"
    cursor.execute("SELECT * FROM follower_quali_matches ORDER BY match_number")
    quali = cursor.fetchall()
    
    output += "<table border='1'><tr><th>Match#</th><th>Team1</th><th>Score1</th><th>Score2</th><th>Team2</th><th>Winner</th></tr>"
    for match in quali:
        output += f"<tr>"
        output += f"<td>{match['match_number'] or 'N/A'}</td>"
        output += f"<td>{match['team1']}</td>"
        output += f"<td>{match['score1'] or '-'}</td>"
        output += f"<td>{match['score2'] or '-'}</td>"
        output += f"<td>{match['team2']}</td>"
        output += f"<td>{match['winner'] or '-'}</td>"
        output += f"</tr>"
    output += "</table>"
    
    output += "<h2>Cup System</h2>"
    cursor.execute("SELECT * FROM follower_cup_matches ORDER BY match_number")
    cup = cursor.fetchall()
    
    output += "<table border='1'><tr><th>Match#</th><th>Round</th><th>Index</th><th>Team1</th><th>Score1</th><th>Score2</th><th>Team2</th><th>Winner</th></tr>"
    for match in cup:
        output += f"<tr>"
        output += f"<td>{match['match_number'] or 'N/A'}</td>"
        output += f"<td>{match['round']}</td>"
        output += f"<td>{match['match_index']}</td>"
        output += f"<td>{match['team1'] or 'TBD'}</td>"
        output += f"<td>{match['score1'] or '-'}</td>"
        output += f"<td>{match['score2'] or '-'}</td>"
        output += f"<td>{match['team2'] or 'TBD'}</td>"
        output += f"<td>{match['winner'] or '-'}</td>"
        output += f"</tr>"
    output += "</table>"
    
    conn.close()
    
    return output


# ============================================================================
# STATISTIKEN & SCHLUSSRANGLISTE
# ============================================================================

def _get_total_stats(cursor, team):
    """Gesamte Goals + Differenz über ALLE Phasen"""
    gf = gd = 0
    for table, t1f, s1f, t2f, s2f in [
        ('matches','team1','score1','team2','score2'),
        ('double_elim_matches','team1','score1','team2','score2'),
        ('follower_cup_matches','team1','score1','team2','score2'),
        ('placement_matches','team1','score1','team2','score2'),
    ]:
        try:
            cursor.execute(f"SELECT {s1f},{s2f} FROM {table} WHERE {t1f}=? AND {s1f} IS NOT NULL",(team,))
            for r in cursor.fetchall(): gf+=r[0]; gd+=(r[0]-r[1])
            cursor.execute(f"SELECT {s1f},{s2f} FROM {table} WHERE {t2f}=? AND {s1f} IS NOT NULL",(team,))
            for r in cursor.fetchall(): gf+=r[1]; gd+=(r[1]-r[0])
        except Exception: pass
    return gf, gd


def _compute_final_rankings(conn):
    """Vollständige Schlussrangliste P1-P60 — robust gegen fehlende Ergebnisse"""
    import functools
    cursor = conn.cursor()
    rankings = []
    placed = set()

    def add(place, team, phase):
        if team and team not in placed:
            gf, gd = _get_total_stats(cursor, team)
            rankings.append({'place': place, 'team': team, 'phase': phase,
                             'total_goals': gf, 'goal_diff': gd})
            placed.add(team)

    def add_sorted(start_place, teams, phase):
        enriched = []
        for t in teams:
            if t and t not in placed:
                gf, gd = _get_total_stats(cursor, t)
                enriched.append((t, gf, gd))
        enriched.sort(key=lambda x: (-x[1], -x[2]))
        for i, (t, gf, gd) in enumerate(enriched):
            rankings.append({'place': start_place + i, 'team': t, 'phase': phase,
                             'total_goals': gf, 'goal_diff': gd})
            placed.add(t)

    # ── P1-P2: Super Finals ───────────────────────────────────────────────────
    cursor.execute("SELECT * FROM super_finals_matches WHERE match_id='FINAL'")
    sf_final = cursor.fetchone()
    cursor.execute("SELECT * FROM super_finals_matches WHERE match_id='HF1'")
    sf_hf1 = cursor.fetchone()
    cursor.execute("SELECT * FROM super_finals_matches WHERE match_id='THIRD'")
    sf_third = cursor.fetchone()

    if sf_final and sf_final['winner'] and sf_final['team1'] and sf_final['team2']:
        loser = sf_final['team2'] if sf_final['winner'] == sf_final['team1'] else sf_final['team1']
        add(1, sf_final['winner'], 'Turniersieger 🏆')
        add(2, loser, 'Finalist')
    elif sf_hf1 and sf_hf1['winner']:
        add(1, sf_hf1['winner'], 'Turniersieger 🏆')
        loser_hf = sf_hf1['team2'] if sf_hf1['winner'] == sf_hf1['team1'] else sf_hf1['team1']
        if loser_hf: add(2, loser_hf, 'Finalist')
    else:
        # Fallback: WB Sieger + LB Final Sieger
        # WB Final Sieger (WB R4) aus beiden Brackets
        for tbl in ['double_elim_matches_a', 'double_elim_matches_b']:
            cursor.execute(f"SELECT winner FROM {tbl} WHERE bracket='Winners' AND round=4 AND winner IS NOT NULL LIMIT 1")
            r = cursor.fetchone()
            if r: add(1, r['winner'], 'WB Sieger')
            cursor.execute(f"SELECT winner FROM {tbl} WHERE bracket='Losers' AND round=8 AND winner IS NOT NULL LIMIT 1")
            r = cursor.fetchone()
            if r: add(2, r['winner'], 'LB Sieger')

    # ── P3-P4: Platz 3 aus HF1 ───────────────────────────────────────────────
    if sf_third and sf_third['winner'] and sf_third['team1'] and sf_third['team2']:
        loser_third = sf_third['team2'] if sf_third['winner'] == sf_third['team1'] else sf_third['team1']
        add(3, sf_third['winner'], 'Platz 3')
        add(4, loser_third, 'Platz 4')
    elif sf_hf1 and sf_hf1['team1'] and sf_hf1['team2']:
        hf_loser = sf_hf1['team2'] if sf_hf1['winner'] == sf_hf1['team1'] else sf_hf1['team1']
        if hf_loser: add(3, hf_loser, 'HF Verlierer')

    # ── P5-P32: DE Verlierer — LB Runden von hinten ──────────────────────────
    # Korrekte Plätze für 32-Team DE:
    # LB R9 Final Verlierer → P3 (schon oben), Sieger → P2 (schon oben)
    # LB R8 Verlierer (1 Team) → P5
    # LB R7 Verlierer (1 Team) → P6
    # LB R6 Verlierer (2 Teams) → P7-8
    # LB R5 Verlierer (2 Teams) → P9-10
    # LB R4 Verlierer (4 Teams) → P11-14
    # LB R3 Verlierer (4 Teams) → P15-18
    # LB R2 Verlierer (8 Teams) → P19-26
    # LB R1 Verlierer (8 Teams) → P27-34 → aber nur bis P32
    # LB: R8=LB Final Verlierer→P5, R7→P6, R6(2)→P7-8, R5(2)→P9-10,
    #       R4(4)→P11-14, R3(4)→P15-18, R2(8)→P19-26, R1(8)→P27-34
    lb_placement = [(8, 5, 1), (7, 6, 1), (6, 7, 2), (5, 9, 2),
                    (4, 11, 4), (3, 15, 4), (2, 19, 8), (1, 27, 8)]
    for lb_rnd, start, count in lb_placement:
        losers = []
        for tbl in ['double_elim_matches_a', 'double_elim_matches_b']:
            cursor.execute(f"""SELECT loser FROM {tbl}
                WHERE bracket='Losers' AND round=? AND loser IS NOT NULL
                ORDER BY match_index""", (lb_rnd,))
            losers += [r['loser'] for r in cursor.fetchall()]
        add_sorted(start, losers, f'DE P{start}+')

    # WB Verlierer aus früheren Runden (die nicht via LB weitergekommen sind)
    for wb_rnd in range(1, 5):
        for tbl in ['double_elim_matches_a', 'double_elim_matches_b']:
            cursor.execute(f"""SELECT loser FROM {tbl}
                WHERE bracket='Winners' AND round=? AND loser IS NOT NULL
                ORDER BY match_index""", (wb_rnd,))
        losers = [r['loser'] for r in cursor.fetchall()]
        # Nur jene die noch nicht platziert sind (= durch LB ausgeschieden)
        unplaced_losers = [l for l in losers if l not in placed]
        if unplaced_losers:
            next_p = max((r['place'] for r in rankings), default=4) + 1
            add_sorted(next_p, unplaced_losers, f'DE WB R{wb_rnd} Verlierer')

    # ── P33-P48: Follower Cup ─────────────────────────────────────────────────
    cursor.execute("SELECT * FROM follower_cup_matches WHERE round='final' LIMIT 1")
    fc_f = cursor.fetchone()
    if fc_f and fc_f['winner'] and fc_f['team1'] and fc_f['team2']:
        fc_l = fc_f['team2'] if fc_f['winner'] == fc_f['team1'] else fc_f['team1']
        add(33, fc_f['winner'], 'FC Sieger 🥇')
        add(34, fc_l, 'FC Finalist')

    cursor.execute("SELECT * FROM follower_cup_matches WHERE round='third' LIMIT 1")
    fc_3 = cursor.fetchone()
    if fc_3 and fc_3['winner'] and fc_3['team1'] and fc_3['team2']:
        fc_3l = fc_3['team2'] if fc_3['winner'] == fc_3['team1'] else fc_3['team1']
        add(35, fc_3['winner'], 'FC Platz 3')
        add(36, fc_3l, 'FC Platz 4')

    def fc_losers_from_round(rnd):
        cursor.execute(
            "SELECT team1, team2, winner FROM follower_cup_matches WHERE round=? AND winner IS NOT NULL",
            (rnd,))
        return [r['team2'] if r['winner'] == r['team1'] else r['team1'] for r in cursor.fetchall()]

    add_sorted(37, fc_losers_from_round('semi'),    'FC P37+')
    add_sorted(39, fc_losers_from_round('quarter'), 'FC P39+')
    add_sorted(43, fc_losers_from_round('eighth'),  'FC P43+')

    # Restliche FC-Teams (falls Matches noch nicht gespielt)
    cursor.execute("""SELECT DISTINCT team1 as team FROM follower_cup_matches
                      UNION SELECT DISTINCT team2 FROM follower_cup_matches
                      WHERE team2 IS NOT NULL""")
    fc_all = [r['team'] for r in cursor.fetchall() if r['team']]
    unplaced_fc = [t for t in fc_all if t not in placed]
    if unplaced_fc:
        next_p = max((r['place'] for r in rankings if r['place'] <= 48), default=32) + 1
        add_sorted(next_p, unplaced_fc, 'FC (nicht gespielt)')

    # ── P49-P60: Platzierungsrunde ────────────────────────────────────────────
    cursor.execute("SELECT * FROM placement_matches ORDER BY placement")
    for m in cursor.fetchall():
        plz_str = m['placement'] or 'P49'
        plz_num = int(plz_str[1:]) if plz_str.startswith('P') and plz_str[1:].isdigit() else 49
        if m['winner'] and m['team1'] and m['team2']:
            loser_plz = m['team2'] if m['winner'] == m['team1'] else m['team1']
            add(plz_num,     m['winner'],  'PLZ')
            add(plz_num + 1, loser_plz,   'PLZ')
        else:
            if m['team1']: add(plz_num, m['team1'], 'PLZ')
            if m['team2']: add(plz_num + 1, m['team2'], 'PLZ')

    # ── Restliche Teams auffüllen ─────────────────────────────────────────────
    cursor.execute("""SELECT DISTINCT r.team FROM rankings r
        LEFT JOIN teams t ON r.team = t.name
        WHERE (t.is_ghost IS NULL OR t.is_ghost = 0)""")
    all_rr_teams = [r['team'] for r in cursor.fetchall()]
    unplaced = [t for t in all_rr_teams if t not in placed]
    if unplaced:
        next_place = max((r['place'] for r in rankings), default=0) + 1
        add_sorted(next_place, unplaced, 'Round Robin')

    # ── Finale Bereinigung ────────────────────────────────────────────────────
    rankings.sort(key=lambda x: x['place'])

    # Vergib saubere fortlaufende Plätze (ohne Lücken und Duplikate)
    seen_teams = set()
    clean = []
    for r in rankings:
        if r['team'] not in seen_teams:
            seen_teams.add(r['team'])
            clean.append(r)

    # Weise lückenlose Plätze zu
    for i, r in enumerate(clean):
        r['place'] = i + 1

    return clean


def _compute_tournament_facts(conn):
    """Interessante Turnier-Statistiken"""
    cursor = conn.cursor()
    facts = []
    all_matches = []
    for table, phase in [
        ('matches','Round Robin'),
        ('double_elim_matches_a','Double Elimination A'),
        ('double_elim_matches_b','Double Elimination B'),
        ('follower_cup_matches','Follower Cup'),
        ('placement_matches','Platzierungsrunde'),
    ]:
        try:
            cursor.execute(f"SELECT team1,team2,score1,score2 FROM {table} WHERE score1 IS NOT NULL AND score2 IS NOT NULL")
            for r in cursor.fetchall():
                all_matches.append({'team1':r['team1'],'team2':r['team2'],
                                    'score1':r['score1'],'score2':r['score2'],'phase':phase})
        except Exception: pass

    if not all_matches:
        return facts

    total_g = len(all_matches)
    total_pts = sum(m['score1']+m['score2'] for m in all_matches)
    avg = round(total_pts/total_g, 1)
    facts.append({'label':'Total Spiele gespielt','value':str(total_g)})
    facts.append({'label':'Total geworfene Punkte','value':str(total_pts)})
    facts.append({'label':'Ø Punkte pro Spiel','value':str(avg)})

    best = max(all_matches, key=lambda m: abs(m['score1']-m['score2']))
    bw = best['team1'] if best['score1']>best['score2'] else best['team2']
    bl = best['team2'] if best['score1']>best['score2'] else best['team1']
    bws,bls = (best['score1'],best['score2']) if best['score1']>best['score2'] else (best['score2'],best['score1'])
    facts.append({'label':'Höchster Sieg','value':f"{bw} {bws}:{bls} gegen {bl} ({best['phase']})"})

    close = min(all_matches, key=lambda m: abs(m['score1']-m['score2']))
    facts.append({'label':'Engster Match','value':f"{close['team1']} {close['score1']}:{close['score2']} {close['team2']} ({close['phase']})"})

    tg = {}; tw = {}; tgames = {}
    for m in all_matches:
        tg[m['team1']] = tg.get(m['team1'],0)+m['score1']
        tg[m['team2']] = tg.get(m['team2'],0)+m['score2']
        w = m['team1'] if m['score1']>m['score2'] else m['team2']
        tw[w] = tw.get(w,0)+1
        tgames[m['team1']] = tgames.get(m['team1'],0)+1
        tgames[m['team2']] = tgames.get(m['team2'],0)+1

    if tg:
        top_tg = max(tg, key=tg.get)
        facts.append({'label':'Meiste Punkte gesamt','value':f"{top_tg} — {tg[top_tg]} Punkte"})
    if tw:
        top_tw = max(tw, key=tw.get)
        facts.append({'label':'Meiste Siege gesamt','value':f"{top_tw} — {tw[top_tw]} Siege"})
    return facts


@app.route('/final_rankings/<game_name>')
def final_rankings(game_name):
    """Schlussrangliste Webseite"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    conn = get_db_connection(db_path)
    rankings = _compute_final_rankings(conn)
    facts    = _compute_tournament_facts(conn)
    conn.close()
    return render_template("admin/final_rankings.html",
                           game_name=game_name, rankings=rankings, facts=facts)


@app.route('/final_rankings_pdf/<game_name>')
def final_rankings_pdf(game_name):
    """Schlussrangliste als PDF"""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas as rl_canvas

    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")

    conn = get_db_connection(db_path)
    rankings = _compute_final_rankings(conn)
    conn.close()

    buf = io.BytesIO()
    W, H = A4
    c = rl_canvas.Canvas(buf, pagesize=A4)
    mg = 12*mm

    def place_color(place):
        if place == 1: return colors.HexColor('#FFD700')
        if place == 2: return colors.HexColor('#C0C0C0')
        if place == 3: return colors.HexColor('#CD7F32')
        if place <= 32: return colors.HexColor('#1a6b3a')
        if place <= 48: return colors.HexColor('#7a5200')
        return colors.HexColor('#6b1a1a')

    def phase_bg(place):
        if place <= 4:  return colors.HexColor('#fffbe6')
        if place <= 32: return colors.HexColor('#f0f7f0')
        if place <= 48: return colors.HexColor('#fdf5e6')
        return colors.HexColor('#fdf0f0')

    col_place = 12*mm; col_team = 72*mm; col_phase = 52*mm
    col_goals = 18*mm; col_diff = 16*mm; row_h = 7*mm
    header_h = 18*mm; col_hdr_h = 7*mm

    def draw_header(page_num):
        c.setFillColor(colors.HexColor('#1e3a5f'))
        c.rect(mg, H-mg-18*mm, W-2*mg, 18*mm, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont('Helvetica-Bold', 16)
        c.drawString(mg+5*mm, H-mg-12*mm, f'Schlussrangliste  —  {game_name}')
        c.setFont('Helvetica', 8)
        c.drawRightString(W-mg-3*mm, H-mg-12*mm,
                          f'Erstellt: {datetime.now().strftime("%d.%m.%Y %H:%M")}')

    def draw_table_header(y):
        c.setFillColor(colors.HexColor('#2d5a9e'))
        c.rect(mg, y, W-2*mg, col_hdr_h, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont('Helvetica-Bold', 8)
        x = mg + 3*mm
        for lbl, offset in [('Platz',0),('Team',col_place),('Phase',col_place+col_team),
                              ('Punkte',col_place+col_team+col_phase),('Diff',col_place+col_team+col_phase+col_goals)]:
            c.drawString(x+offset, y+2*mm, lbl)

    page = 1
    draw_header(page)
    y = H - mg - header_h - col_hdr_h
    draw_table_header(y)
    y -= row_h

    for r in rankings:
        if y < mg + 15*mm:
            c.showPage(); page += 1
            draw_header(page)
            y = H - mg - header_h - col_hdr_h
            draw_table_header(y)
            y -= row_h

        bg = phase_bg(r['place'])
        clr = place_color(r['place'])
        c.setFillColor(bg)
        c.rect(mg, y, W-2*mg, row_h, fill=1, stroke=0)
        c.setFillColor(clr)
        c.rect(mg, y, col_place, row_h, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont('Helvetica-Bold', 9)
        c.drawCentredString(mg+col_place/2, y+2*mm, str(r['place']))
        c.setFillColor(colors.black)
        c.setFont('Helvetica-Bold' if r['place'] <= 3 else 'Helvetica', 8.5)
        c.drawString(mg+col_place+3*mm, y+2*mm, (r['team'] or '—')[:38])
        c.setFont('Helvetica', 7.5)
        c.setFillColor(colors.HexColor('#555555'))
        c.drawString(mg+col_place+col_team+3*mm, y+2*mm, r['phase'])
        c.setFillColor(colors.black)
        c.setFont('Helvetica', 8)
        c.drawRightString(mg+col_place+col_team+col_phase+col_goals-2*mm, y+2*mm, str(r.get('total_goals','')))
        diff = r.get('goal_diff', 0)
        diff_str = ('+' if diff > 0 else '') + str(diff)
        diff_clr = colors.HexColor('#1a6b3a') if diff > 0 else (colors.HexColor('#7a1a1a') if diff < 0 else colors.black)
        c.setFillColor(diff_clr)
        c.drawRightString(mg+col_place+col_team+col_phase+col_goals+col_diff-2*mm, y+2*mm, diff_str)
        c.setStrokeColor(colors.HexColor('#dddddd'))
        c.setLineWidth(0.3)
        c.line(mg, y, mg+W-2*mg, y)
        y -= row_h

    c.save()
    buf.seek(0)
    return send_file(buf, mimetype='application/pdf', as_attachment=True,
                     download_name=f"{game_name}_schlussrangliste.pdf")


@app.route('/recalculate_all_times/<game_name>', methods=['POST'])
def recalculate_all_times(game_name):
    """Alle Zeiten und Felder neu berechnen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as c FROM double_elim_matches_a WHERE time IS NOT NULL")
    if cursor.fetchone()['c'] > 0:
        calculate_double_elim_times(conn, 'double_elim_matches_a')
        calculate_double_elim_times(conn, 'double_elim_matches_b')
    cursor.execute("SELECT COUNT(*) as c FROM super_finals_matches")
    if cursor.fetchone()['c'] > 0:
        calculate_super_finals_times(conn)
    conn.close()
    return redirect(url_for('game_overview', game_name=game_name))


@app.route('/api/display/<game_name>/final_rankings_json')
def api_display_final_rankings(game_name):
    """JSON API: Schlussrangliste für Display"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return jsonify({})
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    winner = None
    for mid in ['FINAL', 'HF1']:
        cursor.execute("SELECT winner FROM super_finals_matches WHERE match_id=? AND winner IS NOT NULL", (mid,))
        r = cursor.fetchone()
        if r and r['winner']:
            winner = r['winner']
            break
    rankings = _compute_final_rankings(conn)
    conn.close()
    return jsonify({'winner': winner, 'rankings': rankings})


@app.route('/api/de_matches/<game_name>')
def api_de_matches(game_name):
    """JSON API: Alle DE-Matches beider Brackets (A+B) fuer enter_double_elim_results"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return jsonify([])
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    matches = []
    for table in ['double_elim_matches_a', 'double_elim_matches_b']:
        cursor.execute(f"""
            SELECT id, round, bracket, match_index, match_number,
                   team1, team2, score1, score2, winner, loser, court, time
            FROM {table}
            ORDER BY round, bracket DESC, match_index
        """)
        for r in cursor.fetchall():
            d = dict(r)
            d['bracket_table'] = table
            matches.append(d)
    conn.close()
    return jsonify(matches)


@app.route('/api/display/<game_name>/brackets_full_json')
def api_brackets_full_json(game_name):
    """JSON API: Vollständige DE + FC + PLZ Daten für Display"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return jsonify({})
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    wb, lb = [], []
    for tbl in ['double_elim_matches_a', 'double_elim_matches_b']:
        cursor.execute(f"SELECT round,match_index,match_number,team1,team2,score1,score2,winner,loser,court,time FROM {tbl} WHERE bracket='Winners' ORDER BY round,match_index")
        wb += [dict(r) for r in cursor.fetchall()]
        cursor.execute(f"SELECT round,match_index,match_number,team1,team2,score1,score2,winner,loser,court,time FROM {tbl} WHERE bracket='Losers' ORDER BY round,match_index")
        lb += [dict(r) for r in cursor.fetchall()]
    cursor.execute("SELECT round,match_index,match_number,team1,team2,score1,score2,winner,court,time FROM follower_cup_matches ORDER BY CASE round WHEN 'eighth' THEN 1 WHEN 'quarter' THEN 2 WHEN 'semi' THEN 3 WHEN 'final' THEN 4 WHEN 'third' THEN 5 END,match_index")
    fc = [dict(r) for r in cursor.fetchall()]
    cursor.execute("SELECT placement,match_number,team1,team2,score1,score2,winner,court,time FROM placement_matches ORDER BY match_number")
    plz = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return jsonify({'wb':wb,'lb':lb,'fc':fc,'plz':plz})


@app.route('/team_schedules_pdf/<game_name>')
def team_schedules_pdf(game_name):
    """Individuelle Spielpläne für alle Teams — 2 Teams pro A4-Seite"""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas as rl_canvas

    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name, group_number FROM teams WHERE is_ghost=0 ORDER BY group_number, name")
    teams = cursor.fetchall()
    cursor.execute("SELECT match_number, round, team1, team2, field, time, score1, score2 FROM matches ORDER BY match_number")
    all_matches = cursor.fetchall()
    conn.close()

    # Matches pro Team indexieren
    team_matches = {t['name']: [] for t in teams}
    for m in all_matches:
        for side, opp, s1, s2 in [(m['team1'], m['team2'], m['score1'], m['score2']),
                                   (m['team2'], m['team1'], m['score2'], m['score1'])]:
            if side in team_matches:
                team_matches[side].append({
                    'match_number': m['match_number'],
                    'time':  m['time'],
                    'field': m['field'],
                    'opponent': opp,
                    'score1': s1, 'score2': s2
                })
    for name in team_matches:
        team_matches[name].sort(key=lambda x: (x['match_number'] or 0))

    buf = io.BytesIO()
    W, H = A4
    mg   = 10*mm
    HALF = H / 2

    COL_HEADER  = colors.HexColor('#1e3a5f')
    COL_GROUP   = colors.HexColor('#2d5a9e')
    COL_ROW_ODD = colors.HexColor('#f0f4fb')
    COL_WIN     = colors.HexColor('#d4edda')
    COL_LOSS    = colors.HexColor('#f8d7da')

    c = rl_canvas.Canvas(buf, pagesize=A4)

    def draw_team_slot(team, idx, top_y):
        name    = team['name']
        group   = team['group_number']
        matches = team_matches.get(name, [])

        hdr_h = 16*mm; thdr_h = 7*mm; row_h = 8*mm
        col_x = [mg, mg+15*mm, mg+32*mm, mg+50*mm, mg+98*mm]

        c.setFillColor(COL_HEADER)
        c.rect(mg, top_y - hdr_h, W - 2*mg, hdr_h, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont('Helvetica-Bold', 14)
        c.drawString(mg + 4*mm, top_y - 10*mm, name)
        c.setFont('Helvetica', 8)
        c.drawString(mg + 4*mm, top_y - 14*mm, f'Gruppe {group}  —  Round Robin Spielplan')
        c.setFont('Helvetica', 7)
        c.drawRightString(W - mg - 2*mm, top_y - 8*mm, game_name)

        tbl_top = top_y - hdr_h - thdr_h
        c.setFillColor(COL_GROUP)
        c.rect(mg, tbl_top, W - 2*mg, thdr_h, fill=1, stroke=0)
        c.setFillColor(colors.white)
        c.setFont('Helvetica-Bold', 7)
        for lbl, cx in zip(['#', 'Zeit', 'Feld', 'Gegner', 'Resultat'], col_x):
            c.drawString(cx + 1.5*mm, tbl_top + 2*mm, lbl)

        y = tbl_top - row_h
        for mi, m in enumerate(matches):
            has = m['score1'] is not None and m['score2'] is not None
            if has:
                bg = COL_WIN if m['score1'] > m['score2'] else COL_LOSS
            else:
                bg = COL_ROW_ODD if mi % 2 == 0 else colors.white
            c.setFillColor(bg)
            c.rect(mg, y, W - 2*mg, row_h, fill=1, stroke=0)
            c.setStrokeColor(colors.HexColor('#dddddd'))
            c.setLineWidth(0.3)
            c.line(mg, y, W - mg, y)
            c.setFillColor(colors.black)
            c.setFont('Helvetica-Bold', 8)
            c.drawString(col_x[0] + 1.5*mm, y + 2.5*mm, f"#{m['match_number']}")
            c.setFont('Helvetica', 8)
            c.drawString(col_x[1] + 1.5*mm, y + 2.5*mm, m['time'] or '—')
            c.drawString(col_x[2] + 1.5*mm, y + 2.5*mm, f"Feld {m['field']}" if m['field'] else '—')
            c.setFont('Helvetica-Bold', 8)
            c.drawString(col_x[3] + 1.5*mm, y + 2.5*mm, (m['opponent'] or '—')[:30])
            if has:
                res = f"{m['score1']} : {m['score2']}  {'SIEG' if m['score1']>m['score2'] else 'NL'}"
                clr = colors.HexColor('#155724') if m['score1']>m['score2'] else colors.HexColor('#721c24')
                c.setFillColor(clr)
                c.setFont('Helvetica-Bold', 8)
                c.drawString(col_x[4] + 1.5*mm, y + 2.5*mm, res)
                c.setFillColor(colors.black)
            y -= row_h

        # Rahmen
        c.setStrokeColor(COL_HEADER)
        c.setLineWidth(0.8)
        c.rect(mg, y, W - 2*mg, top_y - y, fill=0, stroke=1)

    for i in range(0, len(teams), 2):
        draw_team_slot(teams[i], i, H - mg)
        if i + 1 < len(teams):
            draw_team_slot(teams[i+1], i+1, HALF - 4*mm)
        # Trennlinie
        c.setStrokeColor(colors.HexColor('#888888'))
        c.setLineWidth(0.8)
        c.setDash(4, 4)
        c.line(mg, HALF, W - mg, HALF)
        c.setDash()
        c.setFillColor(colors.HexColor('#888888'))
        c.setFont('Helvetica', 7)
        c.drawCentredString(W / 2, HALF + 1*mm, '✂')
        c.showPage()

    c.save()
    buf.seek(0)
    return send_file(buf, mimetype='application/pdf', as_attachment=True,
                     download_name=f"{game_name}_team_spielplaene.pdf")


# ============================================================================
# ERROR HANDLER
# ============================================================================

@app.errorhandler(404)
def page_not_found(e):
    return render_template("admin/error.html", 
                         error_message="Seite nicht gefunden (404)"), 404


@app.errorhandler(500)
def internal_error(e):
    return render_template("admin/error.html", 
                         error_message=f"Interner Serverfehler (500): {str(e)}"), 500


# ============================================================================
# APP STARTEN
# ============================================================================

def open_browser():
    """Browser automatisch öffnen"""
    webbrowser.open('http://127.0.0.1:5000/')


if __name__ == '__main__':
    print("=" * 70)
    print("🏆 ULTIMATE FRISBEE TURNIER-MANAGEMENT SYSTEM")
    print("=" * 70)
    print(f"✅ Templates-Ordner: {TEMPLATES_FOLDER}")
    print(f"✅ Turnier-Ordner: {TOURNAMENT_FOLDER}")
    print("🌐 Server startet auf: http://127.0.0.1:5000/")
    print()
    print("📋 FEATURES:")
    print("   ✅ Team-Management mit Ghost-Teams")
    print("   ✅ Round Robin mit Zeitplanung")
    print("   ✅ Zwei-Bracket Double Elimination (A & B)")
    print("   ✅ Super Finals (HF + Finale + Platz 3)")
    print("   ✅ Follower Cup (Quali + 1/8 bis Finale)")
    print("   ✅ Platzierungsrunde (Plätze 37-60)")
    print("   ✅ Automatische Weiterleitungen (Mappings)")
    print("   ✅ Display-Bereich für Beamer")
    print("   ✅ CSV-Export komplett")
    print("   ✅ Spielnummern #1-245+")
    print("   ✅ Score-Limitierung (max. 42)")
    print("   ✅ Ranking-Berechnung KORRIGIERT")
    print("   ✅ Debug-Tools für alle Phasen")
    print("=" * 70)
    print()
    print("⚠️  HINWEIS: Templates müssen noch erstellt werden!")
    print("   Siehe: templates/admin/ und templates/display/")
    print()
    print("📚 Dokumentation:")
    print("   - Turnier_System_Dokumentation.txt: Vollständige Spezifikation")
    print("=" * 70)
    
    # Browser nach 1 Sekunde öffnen
    Timer(1, open_browser).start()


if __name__ == '__main__':
    # Flask-Server starten
    app.run(debug=True, use_reloader=False)