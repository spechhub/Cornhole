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

# BRACKET A MAPPINGS (Gruppen 1-5)
WINNER_MAPPING = {
    # WB R1 (16M) → WB R2
    (1, 0):  (2, 0, 'team1'), (1, 1):  (2, 0, 'team2'),
    (1, 2):  (2, 1, 'team1'), (1, 3):  (2, 1, 'team2'),
    (1, 4):  (2, 2, 'team1'), (1, 5):  (2, 2, 'team2'),
    (1, 6):  (2, 3, 'team1'), (1, 7):  (2, 3, 'team2'),
    (1, 8):  (2, 4, 'team1'), (1, 9):  (2, 4, 'team2'),
    (1, 10): (2, 5, 'team1'), (1, 11): (2, 5, 'team2'),
    (1, 12): (2, 6, 'team1'), (1, 13): (2, 6, 'team2'),
    (1, 14): (2, 7, 'team1'), (1, 15): (2, 7, 'team2'),
    # WB R2 (8M) → WB R3
    (2, 0): (3, 0, 'team1'), (2, 1): (3, 0, 'team2'),
    (2, 2): (3, 1, 'team1'), (2, 3): (3, 1, 'team2'),
    (2, 4): (3, 2, 'team1'), (2, 5): (3, 2, 'team2'),
    (2, 6): (3, 3, 'team1'), (2, 7): (3, 3, 'team2'),
    # WB R3 (4M) → WB R4
    (3, 0): (4, 0, 'team1'), (3, 1): (4, 0, 'team2'),
    (3, 2): (4, 1, 'team1'), (3, 3): (4, 1, 'team2'),
    # WB R4 (2M) → WB R5 (Final)
    (4, 0): (5, 0, 'team1'), (4, 1): (5, 0, 'team2'),
}

LOSER_MAPPING = {
    # WB R1 (16V) → LB R1 (8 Spiele, je 2 Verlierer pro Match)
    (1, 0):  (1, 0, 'team1'), (1, 1):  (1, 0, 'team2'),
    (1, 2):  (1, 1, 'team1'), (1, 3):  (1, 1, 'team2'),
    (1, 4):  (1, 2, 'team1'), (1, 5):  (1, 2, 'team2'),
    (1, 6):  (1, 3, 'team1'), (1, 7):  (1, 3, 'team2'),
    (1, 8):  (1, 4, 'team1'), (1, 9):  (1, 4, 'team2'),
    (1, 10): (1, 5, 'team1'), (1, 11): (1, 5, 'team2'),
    (1, 12): (1, 6, 'team1'), (1, 13): (1, 6, 'team2'),
    (1, 14): (1, 7, 'team1'), (1, 15): (1, 7, 'team2'),
    # WB R2 (8V) → LB R3 als team2
    (2, 0): (3, 0, 'team2'), (2, 1): (3, 1, 'team2'),
    (2, 2): (3, 2, 'team2'), (2, 3): (3, 3, 'team2'),
    (2, 4): (3, 4, 'team2'), (2, 5): (3, 5, 'team2'),
    (2, 6): (3, 6, 'team2'), (2, 7): (3, 7, 'team2'),
    # WB R3 (4V) → LB R5 als team2
    (3, 0): (5, 0, 'team2'), (3, 1): (5, 1, 'team2'),
    (3, 2): (5, 2, 'team2'), (3, 3): (5, 3, 'team2'),
    # WB R4 (2V) → LB R7 als team2
    (4, 0): (7, 0, 'team2'), (4, 1): (7, 1, 'team2'),
    # WB Final (1V) → LB Final als team2
    (5, 0): (9, 0, 'team2'),
}

LOSER_WINNER_MAPPING = {
    # LB R1 (8S) → LB R2 als team1
    (1, 0): (2, 0, 'team1'), (1, 1): (2, 1, 'team1'),
    (1, 2): (2, 2, 'team1'), (1, 3): (2, 3, 'team1'),
    (1, 4): (2, 4, 'team1'), (1, 5): (2, 5, 'team1'),
    (1, 6): (2, 6, 'team1'), (1, 7): (2, 7, 'team1'),
    # LB R2 (8S) → LB R3 als team1
    (2, 0): (3, 0, 'team1'), (2, 1): (3, 1, 'team1'),
    (2, 2): (3, 2, 'team1'), (2, 3): (3, 3, 'team1'),
    (2, 4): (3, 4, 'team1'), (2, 5): (3, 5, 'team1'),
    (2, 6): (3, 6, 'team1'), (2, 7): (3, 7, 'team1'),
    # LB R3 (8S) → LB R4 als team1
    (3, 0): (4, 0, 'team1'), (3, 1): (4, 1, 'team1'),
    (3, 2): (4, 2, 'team1'), (3, 3): (4, 3, 'team1'),
    (3, 4): (4, 4, 'team1'), (3, 5): (4, 5, 'team1'),
    (3, 6): (4, 6, 'team1'), (3, 7): (4, 7, 'team1'),
    # LB R4 (8S) → LB R5 als team1
    (4, 0): (5, 0, 'team1'), (4, 1): (5, 1, 'team1'),
    (4, 2): (5, 2, 'team1'), (4, 3): (5, 3, 'team1'),
    (4, 4): (5, 4, 'team1'), (4, 5): (5, 5, 'team1'),
    (4, 6): (5, 6, 'team1'), (4, 7): (5, 7, 'team1'),
    # LB R5 (8S→4M nach WB3 drop) → LB R6 als team1
    (5, 0): (6, 0, 'team1'), (5, 1): (6, 1, 'team1'),
    (5, 2): (6, 2, 'team1'), (5, 3): (6, 3, 'team1'),
    # LB R6 (4S) → LB R7 als team1
    (6, 0): (7, 0, 'team1'), (6, 1): (7, 1, 'team1'),
    (6, 2): (7, 2, 'team1'), (6, 3): (7, 3, 'team1'),
    # LB R7 (4S→2M nach WB4 drop) → LB R8 als team1
    (7, 0): (8, 0, 'team1'), (7, 1): (8, 1, 'team1'),
    # LB R8 (2S) → LB R9 (Final) als team1
    (8, 0): (9, 0, 'team1'),
}

# BRACKET B MAPPINGS (identisch zu A)
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
        CREATE TABLE IF NOT EXISTS double_elim_matches (
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
            CREATE TABLE IF NOT EXISTS double_elim_matches (
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
    
    # Double Elimination
    cursor.execute("SELECT MAX(match_number) as max_num FROM double_elim_matches")
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


def assign_double_elim_match_numbers(conn, start_number=151):
    """Spielnummern fuer Double Elimination (32 Teams, 1 Bracket).
    WB R1-R5 zuerst, dann LB R1-R9."""
    cursor = conn.cursor()
    match_number = start_number
    for round_num in range(1, 6):
        cursor.execute(
            "SELECT id FROM double_elim_matches WHERE round=? AND bracket='Winners' ORDER BY match_index",
            (round_num,))
        for m in cursor.fetchall():
            conn.execute("UPDATE double_elim_matches SET match_number=? WHERE id=?",
                         (match_number, m['id']))
            match_number += 1
    for round_num in range(1, 10):
        cursor.execute(
            "SELECT id FROM double_elim_matches WHERE round=? AND bracket='Losers' ORDER BY match_index",
            (round_num,))
        for m in cursor.fetchall():
            conn.execute("UPDATE double_elim_matches SET match_number=? WHERE id=?",
                         (match_number, m['id']))
            match_number += 1
    conn.commit()
    print(f"✅ DE: {match_number-start_number} Nummern vergeben (#{start_number}-#{match_number-1})")
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
    
    match_ids = ['HF1', 'FINAL', 'THIRD']
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
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches")
    if cursor.fetchone()['count'] > 0:
        start = next_num
        next_num = assign_double_elim_match_numbers(conn, next_num)
        stats['bracket_a'] = {'start': start, 'end': next_num - 1, 'count': next_num - start}
    else:
        stats['bracket_a'] = {'start': 0, 'end': 0, 'count': 0}
    
    # 3. Super Finals (#213-#216)
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
    cursor.execute("UPDATE double_elim_matches SET match_number = NULL")
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
   assign_double_elim_match_numbers(conn, 151)
   
   # Nach Bracket B Generierung
   assign_double_elim_match_numbers(conn, 182)

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
    print("  - assign_double_elim_match_numbers(conn, start)")
    print("  - assign_double_elim_match_numbers(conn, start)")
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

def calculate_double_elim_times_and_courts(conn, de_start_time_str=None):
    """
    Setzt Zeiten UND Felder fuer BEIDE DE-Brackets + FC + Platzierung
    gemaess fixem Zeitplan (Spielplan Excel):

      13:20  WB R1        Felder 1-16  (A:1-8, B:9-16)
      14:10  WB R2        Felder 1-4(A), 13-16(B)
      14:10  LB R1        Felder 5-8(A), 9-12(B)
      14:35  Platzierung  Felder 1-6
      14:35  FC 1/8       Felder 9-16
      15:00  WB VF        Felder 9-10(A), 11-12(B)
      15:00  LB R2        Felder 1-4(A), 5-8(B)
      15:00  FC VF        Felder 13-16
      15:25  LB R3        Felder 5-6(A), 7-8(B)
      15:25  FC HF        Felder 9-10
      15:50  WB HF        Felder 11(A), 12(B)
      15:50  LB R4        Felder 1-2(A), 3-4(B)
      15:50  FC Platz3    Feld 7
      15:50  FC Final     Feld 8
      16:15  LB R5        Felder 3(A), 4(B)
      16:40  WB Final     Felder 7(A), 8(B)
      16:40  LB R6        Felder 4(A), 5(B)
      17:05  LB Final1    Felder 4(A), 5(B)
    """
    cursor = conn.cursor()

    if de_start_time_str is None:
        cursor.execute("SELECT MAX(time) as last_time FROM matches WHERE time IS NOT NULL")
        row = cursor.fetchone()
        last_rr = row['last_time'] if row else None
        de_start_time_str = add_minutes_to_time(last_rr, 15) if last_rr else "13:20"

    print(f"\n⏰ ZEITBERECHNUNG NACHMITTAG – Start: {de_start_time_str}")

    def set_tc(table, ids, time_str, courts):
        for i, mid in enumerate(ids):
            cursor.execute(f"UPDATE {table} SET time=?, court=? WHERE id=?",
                           (time_str, courts[i % len(courts)], mid))

    def get_ids(table, bracket, rnd):
        cursor.execute(f"SELECT id FROM {table} WHERE bracket=? AND round=? ORDER BY match_index",
                       (bracket, rnd))
        return [r['id'] for r in cursor.fetchall()]

    # 13:20 WB R1
    t = de_start_time_str
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',1), t, list(range(1,17)))
    print(f"   WB R1: {t}")

    # 14:10 WB R2 + LB R1
    t2 = add_minutes_to_time(de_start_time_str, 50)
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',2), t2, [1,2,3,4])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',2), t2, [13,14,15,16])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',1),  t2, [5,6,7,8])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',1),  t2, [9,10,11,12])
    print(f"   WB R2 + LB R1: {t2}")

    # 14:35 Platzierung + FC 1/8
    t3 = add_minutes_to_time(t2, 25)
    cursor.execute("SELECT id FROM placement_matches ORDER BY match_number")
    ids_p = [r['id'] for r in cursor.fetchall()]
    if ids_p:
        set_tc('placement_matches', ids_p, t3, [1,2,3,4,5,6])
    cursor.execute("SELECT id FROM follower_cup_matches WHERE round='eighth' ORDER BY match_index")
    ids_fc8 = [r['id'] for r in cursor.fetchall()]
    if ids_fc8:
        set_tc('follower_cup_matches', ids_fc8, t3, [9,10,11,12,13,14,15,16])
    print(f"   Platzierung + FC 1/8: {t3}")

    # 15:00 WB VF + LB R2 + FC VF
    t4 = add_minutes_to_time(t3, 25)
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',3), t4, [9,10])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',3), t4, [11,12])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',2),  t4, [1,2,3,4])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',2),  t4, [5,6,7,8])
    cursor.execute("SELECT id FROM follower_cup_matches WHERE round='quarter' ORDER BY match_index")
    ids_fcq = [r['id'] for r in cursor.fetchall()]
    if ids_fcq:
        set_tc('follower_cup_matches', ids_fcq, t4, [13,14,15,16])
    print(f"   WB VF + LB R2 + FC VF: {t4}")

    # 15:25 LB R3 + FC HF
    t5 = add_minutes_to_time(t4, 25)
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',3), t5, [5,6])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',3), t5, [7,8])
    cursor.execute("SELECT id FROM follower_cup_matches WHERE round='semi' ORDER BY match_index")
    ids_fcs = [r['id'] for r in cursor.fetchall()]
    if ids_fcs:
        set_tc('follower_cup_matches', ids_fcs, t5, [9,10])
    print(f"   LB R3 + FC HF: {t5}")

    # 15:50 WB HF + LB R4 + FC Final + FC Platz3
    t6 = add_minutes_to_time(t5, 25)
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',4), t6, [11])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',4), t6, [12])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',4),  t6, [1,2])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',4),  t6, [3,4])
    cursor.execute("SELECT id FROM follower_cup_matches WHERE round='third'  ORDER BY match_index")
    ids_fc3 = [r['id'] for r in cursor.fetchall()]
    if ids_fc3:
        set_tc('follower_cup_matches', ids_fc3, t6, [7])
    cursor.execute("SELECT id FROM follower_cup_matches WHERE round='final'  ORDER BY match_index")
    ids_fcf = [r['id'] for r in cursor.fetchall()]
    if ids_fcf:
        set_tc('follower_cup_matches', ids_fcf, t6, [8])
    print(f"   WB HF + LB R4 + FC Final: {t6}")

    # 16:15 LB R5
    t7 = add_minutes_to_time(t6, 25)
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',5), t7, [3])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',5), t7, [4])
    print(f"   LB R5: {t7}")

    # 16:40 WB Final + LB R6
    t8 = add_minutes_to_time(t7, 25)
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',5), t8, [7])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Winners',5), t8, [8])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',6),  t8, [4])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',6),  t8, [5])
    print(f"   WB Final + LB R6: {t8}")

    # 17:05 LB Final1
    t9 = add_minutes_to_time(t8, 25)
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',7), t9, [4])
    set_tc('double_elim_matches', get_ids('double_elim_matches','Losers',7), t9, [5])
    print(f"   LB Final1: {t9}")

    conn.commit()
    t_super = add_minutes_to_time(t9, 25)
    print(f"   → Super Finals ab: {t_super}\n")
    return t_super


def calculate_double_elim_times(conn, table_name, start_time_str=None):
    """Legacy-Wrapper: ruft calculate_double_elim_times_and_courts auf."""
    calculate_double_elim_times_and_courts(conn, start_time_str)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(time) as t FROM double_elim_matches WHERE time IS NOT NULL")
    row = cursor.fetchone()
    return row['t'] if row and row['t'] else start_time_str



# ============================================================================
# SUPER FINALS ZEITBERECHNUNG
# ============================================================================

def calculate_super_finals_times(conn, start_time_str=None):
    """
    Super Finals Zeiten laut fixem Zeitplan:
      17:30  Super Final 1  Feld 4
      17:55  Super Final 2A Feld 6
      18:20  Super Final 2B Feld 6
    """
    cursor = conn.cursor()
    if start_time_str is None:
        cursor.execute("SELECT MAX(time) as t FROM double_elim_matches WHERE time IS NOT NULL")
        row = cursor.fetchone()
        last_de = row['t'] if row and row['t'] else None
        start_time_str = add_minutes_to_time(last_de, 25) if last_de else "17:30"

    t_sf1  = start_time_str
    t_sf2a = add_minutes_to_time(t_sf1, 25)
    t_sf2b = add_minutes_to_time(t_sf2a, 25)

    print(f"\n⏰ SUPER FINALS: {t_sf1} / {t_sf2a} / {t_sf2b}")

    for match_id, t, court in [('HF1', t_sf1, 4), ('HF2', t_sf1, 4),
                                ('THIRD', t_sf2a, 6), ('FINAL', t_sf2b, 6)]:
        cursor.execute("UPDATE super_finals_matches SET time=?, court=? WHERE match_id=?",
                       (t, court, match_id))
    conn.commit()
    print(f"   → Turnier endet ca. um: {t_sf2b}\n")
    return t_sf2b


# ============================================================================
# FOLLOWER CUP ZEITBERECHNUNG
# ============================================================================

def calculate_follower_cup_times(conn, start_time_str=None):
    """No-op Wrapper: FC-Zeiten werden via calculate_double_elim_times_and_courts gesetzt."""
    print("ℹ️  FC-Zeiten werden via calculate_double_elim_times_and_courts gesetzt.")
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(time) as t FROM follower_cup_matches WHERE time IS NOT NULL")
    row = cursor.fetchone()
    return row['t'] if row and row['t'] else (start_time_str or "15:50")


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

    # 2. Double Elimination + FC + Platzierung (integrierter Zeitplan)
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches")
    if cursor.fetchone()['count'] > 0:
        calculate_double_elim_times_and_courts(conn)
        cursor.execute("SELECT MIN(time) as start FROM double_elim_matches WHERE time IS NOT NULL")
        start_de = cursor.fetchone()['start']
        cursor.execute("SELECT MAX(time) as t FROM double_elim_matches WHERE time IS NOT NULL")
        end_de = cursor.fetchone()['t']
        stats['double_elim'] = {'start': start_de, 'end': end_de}

    # 3. Super Finals
    cursor.execute("SELECT COUNT(*) as count FROM super_finals_matches")
    if cursor.fetchone()['count'] > 0:
        end_time_sf = calculate_super_finals_times(conn)
        cursor.execute("SELECT MIN(time) as start FROM super_finals_matches WHERE time IS NOT NULL")
        start_sf = cursor.fetchone()['start']
        stats['super_finals'] = {'start': start_sf, 'end': end_time_sf}

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
    cursor.execute("UPDATE double_elim_matches SET time = NULL")
    cursor.execute("UPDATE super_finals_matches SET time = NULL")
    cursor.execute("UPDATE follower_quali_matches SET time = NULL")
    cursor.execute("UPDATE follower_cup_matches SET time = NULL")
    cursor.execute("UPDATE placement_matches SET time = NULL")

    conn.commit()
    print("✅ Alle Spielzeiten zurückgesetzt")


# ============================================================================
# INTEGRATION IN BESTEHENDEN CODE
# ============================================================================

"""
INTEGRATION IN app.py:
======================

1. Import am Anfang:
   from match_times import calculate_all_match_times, calculate_round_robin_times

2. In create_new_game() Route:
   - Zeitkonfiguration aus Formular lesen
   - In tournament_config speichern (bereits implementiert)

3. In generate_matches() nach Spielplan-Generierung:
   
   conn.commit()
   assign_round_robin_match_numbers(conn)
   calculate_round_robin_times(conn)  # NEU
   recalculate_rankings_internal(conn)

4. In generate_double_elim() nach Bracket-Generierung:
   
   conn.commit()
   assign_double_elim_match_numbers(conn, 151)
   calculate_double_elim_times(conn, 'double_elim_matches')  # NEU
   calculate_double_elim_times(conn, 'double_elim_matches')  # NEU

5. Neue Route für manuelle Neuberechnung:
   
   @app.route('/recalculate_times/<game_name>', methods=['POST'])
   def recalculate_times(game_name):
   db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
   conn = get_db_connection(db_path)
   
   reset_all_match_times(conn)
   stats = calculate_all_match_times(conn)
   
   conn.close()
   return jsonify({"success": True, "stats": stats})

6. Route für Konfigurationsänderung:
   
   @app.route('/update_tournament_config/<game_name>', methods=['POST'])
   def update_tournament_config(game_name):
   db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
   conn = get_db_connection(db_path)
   cursor = conn.cursor()
   
   match_duration = int(request.form['match_duration'])
   break_between_games = int(request.form['break_between_games'])
   break_between_rounds = int(request.form['break_between_rounds'])
   start_time = request.form['start_time']
   
   cursor.execute('''
       UPDATE tournament_config 
       SET match_duration = ?, 
           break_between_games = ?, 
           break_between_rounds = ?, 
           start_time = ?
   ''', (match_duration, break_between_games, break_between_rounds, start_time))
   
   conn.commit()
   
   # Zeiten neu berechnen
   reset_all_match_times(conn)
   calculate_all_match_times(conn)
   
   conn.close()
   return redirect(url_for('game_overview', game_name=game_name))
"""






def recalculate_rankings_internal(conn):
    """Rankings neu berechnen - KORRIGIERTE VERSION"""
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE rankings 
        SET matches_played = 0, wins = 0, draws = 0, losses = 0,
            goals_for = 0, goals_against = 0, goal_difference = 0, points = 0
    """)
    
    cursor.execute("""
        SELECT * FROM matches 
        WHERE score1 IS NOT NULL AND score2 IS NOT NULL
    """)
    
    for match in cursor.fetchall():
        team1 = match['team1']
        team2 = match['team2']
        score1 = match['score1']
        score2 = match['score2']
        
        # Team1 Update
        if score1 > score2:
            wins1, draws1, losses1, points1 = 1, 0, 0, 3
        elif score1 < score2:
            wins1, draws1, losses1, points1 = 0, 0, 1, 0
        else:
            wins1, draws1, losses1, points1 = 0, 1, 0, 1
        
        cursor.execute("""
            UPDATE rankings 
            SET matches_played = matches_played + 1,
                wins = wins + ?,
                draws = draws + ?,
                losses = losses + ?,
                goals_for = goals_for + ?,
                goals_against = goals_against + ?,
                goal_difference = goal_difference + (? - ?),
                points = points + ?
            WHERE team = ?
        """, (wins1, draws1, losses1, score1, score2, score1, score2, points1, team1))
        
        # Team2 Update
        if score2 > score1:
            wins2, draws2, losses2, points2 = 1, 0, 0, 3
        elif score2 < score1:
            wins2, draws2, losses2, points2 = 0, 0, 1, 0
        else:
            wins2, draws2, losses2, points2 = 0, 1, 0, 1
        
        cursor.execute("""
            UPDATE rankings 
            SET matches_played = matches_played + 1,
                wins = wins + ?,
                draws = draws + ?,
                losses = losses + ?,
                goals_for = goals_for + ?,
                goals_against = goals_against + ?,
                goal_difference = goal_difference + (? - ?),
                points = points + ?
            WHERE team = ?
        """, (wins2, draws2, losses2, score2, score1, score2, score1, points2, team2))
    
    conn.commit()


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
    cursor.execute("DELETE FROM double_elim_matches")
    cursor.execute("DELETE FROM super_finals_matches")
    cursor.execute("DELETE FROM follower_quali_matches")
    cursor.execute("DELETE FROM follower_cup_matches")
    cursor.execute("DELETE FROM placement_matches")
    cursor.execute("UPDATE rankings SET matches_played=0, wins=0, draws=0, losses=0, goals_for=0, goals_against=0, goal_difference=0, points=0")
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('game_overview', game_name=game_name))


# ============================================================================
# TEAM-MANAGEMENT
# ============================================================================

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
    missing_teams = max(0, 60 - total_teams)
    
    conn.close()
    
    return render_template("admin/manage_teams.html", 
                         game_name=game_name,
                         groups=dict(groups),
                         missing_teams=missing_teams)


@app.route('/add_team/<game_name>', methods=['POST'])
def add_team(game_name):
    """Team hinzufügen"""
    team_name = request.form['team_name']
    group_number = int(request.form['group_number'])
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM teams WHERE group_number = ?", (group_number,))
    count = cursor.fetchone()['count']
    
    if count >= 6:
        conn.close()
        return jsonify({"success": False, "error": "Gruppe ist bereits voll (max. 6 Teams)!"})
    
    cursor.execute("INSERT INTO teams (name, group_number, is_ghost) VALUES (?, ?, 0)", 
                  (team_name, group_number))
    
    cursor.execute("""
        INSERT INTO rankings (team, group_number, matches_played, wins, draws, losses, 
                            goals_for, goals_against, goal_difference, points)
        VALUES (?, ?, 0, 0, 0, 0, 0, 0, 0, 0)
    """, (team_name, group_number))
    
    conn.commit()
    conn.close()
    
    return jsonify({"success": True})


@app.route('/delete_team/<game_name>/<int:team_id>', methods=['POST'])
def delete_team(game_name, team_id):
    """Team löschen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM teams WHERE id = ?", (team_id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        return jsonify({"success": False, "error": "Team nicht gefunden!"})
    
    team_name = row['name']
    
    cursor.execute("DELETE FROM teams WHERE id = ?", (team_id,))
    cursor.execute("DELETE FROM rankings WHERE team = ?", (team_name,))
    
    conn.commit()
    conn.close()
    
    return jsonify({"success": True})


@app.route('/edit_team/<game_name>/<int:team_id>', methods=['POST'])
def edit_team(game_name, team_id):
    """Team bearbeiten"""
    new_name = request.form['new_name']
    new_group = int(request.form['new_group'])
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM teams WHERE id = ?", (team_id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        return jsonify({"success": False, "error": "Team nicht gefunden!"})
    
    old_name = row['name']
    
    cursor.execute("UPDATE teams SET name = ?, group_number = ? WHERE id = ?", 
                  (new_name, new_group, team_id))
    
    cursor.execute("UPDATE rankings SET team = ?, group_number = ? WHERE team = ?", 
                  (new_name, new_group, old_name))
    
    conn.commit()
    conn.close()
    
    return jsonify({"success": True})


@app.route('/generate_ghost_teams/<game_name>', methods=['POST'])
def generate_ghost_teams(game_name):
    """Ghost-Teams generieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM teams")
    current_count = cursor.fetchone()['count']
    
    missing_count = 60 - current_count
    
    if missing_count <= 0:
        conn.close()
        return jsonify({"success": False, "error": "Bereits 60 Teams vorhanden!"})
    
    cursor.execute("""
        SELECT group_number, COUNT(*) as count 
        FROM teams 
        GROUP BY group_number
    """)
    group_counts = {row['group_number']: row['count'] for row in cursor.fetchall()}
    
    ghost_index = 0
    teams_created = 0
    
    for group_num in range(1, 11):
        current_in_group = group_counts.get(group_num, 0)
        needed = 6 - current_in_group
        
        for i in range(needed):
            if ghost_index >= len(GHOST_TEAM_NAMES):
                ghost_name = f"Ghost Team {ghost_index + 1}"
            else:
                ghost_name = GHOST_TEAM_NAMES[ghost_index]
            
            cursor.execute("""
                INSERT INTO teams (name, group_number, is_ghost)
                VALUES (?, ?, 1)
            """, (ghost_name, group_num))
            
            cursor.execute("""
                INSERT INTO rankings (team, group_number, matches_played, wins, draws, losses,
                                    goals_for, goals_against, goal_difference, points)
                VALUES (?, ?, 0, 0, 0, 0, 0, 0, 0, 0)
            """, (ghost_name, group_num))
            
            ghost_index += 1
            teams_created += 1
            
            if teams_created >= missing_count:
                break
        
        if teams_created >= missing_count:
            break
    
    conn.commit()
    conn.close()
    
    return jsonify({"success": True, "generated": teams_created})


# ============================================================================
# ROUND ROBIN PHASE
# ============================================================================


"""
INTEGRATION-PATCHES FÜR APP.PY
================================

Diese Datei enthält alle Code-Änderungen, die du in app.py vornehmen musst,
um die automatische Spielnummern-Vergabe zu integrieren.

SCHRITT 1: FUNKTIONEN KOPIEREN
===============================
Kopiere alle Funktionen aus match_numbering.py (Zeilen 15-385) und füge sie
NACH den Mapping-Definitionen und VOR den Flask-Routen ein.

SCHRITT 2: CODE-PATCHES ANWENDEN
=================================
Ersetze die bestehenden Funktionen mit den folgenden Versionen:
"""




# ============================================================================
# PATCH 3: NEUE ROUTE - MANUELLE NEUNUMMERIERUNG
# ============================================================================


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
    
    # WICHTIG: match_number wird NICHT hier vergeben, sondern später!
    # Bracket A (Gruppen 1-5) und Bracket B (Gruppen 6-10) spielen zeitlich
    # VERSETZT auf denselben Feldern 1-15 → separate field_counter pro Bracket
    field_counter_a = 1  # Felder für Gruppen 1-5
    field_counter_b = 1  # Felder für Gruppen 6-10

    for group_num in sorted(groups.keys()):
        teams = groups[group_num]
        
        if len(teams) < 2:
            continue
        
        n = len(teams)
        rounds = n - 1 if n % 2 == 0 else n

        # Welches Bracket? A = Gruppen 1-5, B = Gruppen 6-10
        is_bracket_b = (group_num >= 6)
        
        for round_num in range(1, rounds + 1):
            for i in range(n // 2):
                team1_idx = i
                team2_idx = n - 1 - i
                
                if team1_idx < len(teams) and team2_idx < len(teams):
                    team1 = teams[team1_idx]
                    team2 = teams[team2_idx]

                    if is_bracket_b:
                        field = field_counter_b
                        field_counter_b = (field_counter_b % 15) + 1
                    else:
                        field = field_counter_a
                        field_counter_a = (field_counter_a % 15) + 1
                    
                    # KEINE match_number hier! Wird später vergeben
                    cursor.execute("""
                        INSERT INTO matches (round, team1, team2, group_number, field)
                        VALUES (?, ?, ?, ?, ?)
                    """, (round_num, team1, team2, group_num, field))
            
            if n > 2:
                teams = [teams[0]] + [teams[-1]] + teams[1:-1]
    
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
@app.route('/renumber_all_matches/<game_name>', methods=['POST'])
def renumber_all_matches(game_name):
    """
    Manuelle Neunummerierung aller Spiele.
    Nützlich nach manuellen Änderungen oder Fehlern.
    """
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    if not os.path.exists(db_path):
        return jsonify({"success": False, "error": "Turnier nicht gefunden!"})
    
    try:
        conn = get_db_connection(db_path)
        
        # Zuerst alle Nummern zurücksetzen
        cursor = conn.cursor()
        cursor.execute("UPDATE matches SET match_number = NULL")
        cursor.execute("UPDATE double_elim_matches SET match_number = NULL")
        cursor.execute("UPDATE super_finals_matches SET match_number = NULL")
        cursor.execute("UPDATE follower_quali_matches SET match_number = NULL")
        cursor.execute("UPDATE follower_cup_matches SET match_number = NULL")
        cursor.execute("UPDATE placement_matches SET match_number = NULL")
        conn.commit()
        
        # Neu vergeben
        stats = assign_all_match_numbers(conn)
        
        conn.close()
        
        return jsonify({
            "success": True, 
            "message": "Alle Spielnummern erfolgreich neu vergeben!",
            "stats": stats
        })
    
    except Exception as e:
        return jsonify({
            "success": False, 
            "error": f"Fehler bei Neunummerierung: {str(e)}"
        })


@app.route('/game_overview/<game_name>')
def game_overview(game_name):
    """Turnierübersicht - MIT SPIELNUMMERN-STATISTIK"""
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
    
    # NEU: Spielnummern-Status
    cursor.execute("SELECT COUNT(*) as count FROM matches WHERE match_number IS NOT NULL")
    numbered_rr = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches WHERE match_number IS NOT NULL")
    numbered_a = cursor.fetchone()['count']
    numbered_b = numbered_a  # Ein Bracket
    
    cursor.execute("SELECT MAX(match_number) as max_num FROM matches")
    max_rr = cursor.fetchone()['max_num'] or 0
    
    cursor.execute("""
        SELECT MAX(match_number) as max_num
        FROM (
            SELECT match_number FROM matches
            UNION ALL SELECT match_number FROM double_elim_matches
        )
    """)
    max_overall = cursor.fetchone()['max_num'] or 0
    
    conn.close()
    
    return render_template("admin/game_overview.html", 
                         game_name=game_name,
                         team_count=team_count,
                         ghost_count=ghost_count,
                         match_count=match_count,
                         results_count=results_count,
                         numbered_rr=numbered_rr,
                         numbered_a=numbered_a,
                         numbered_b=numbered_b,
                         max_rr=max_rr,
                         max_overall=max_overall)


# ============================================================================
# INSTALLATIONS-ANLEITUNG
# ============================================================================

"""
SO INTEGRIERST DU DEN CODE IN APP.PY:
======================================

SCHRITT 1: FUNKTIONEN HINZUFÜGEN
---------------------------------
1. Öffne app.py
2. Suche die Zeile: # ============================================================================
                     # HILFSFUNKTIONEN
3. NACH dieser Sektion und VOR den Flask-Routen (@app.route) füge ein:
   
   # ============================================================================
   # SPIELNUMMERN-SYSTEM
   # ============================================================================
   
   [HIER ALLE FUNKTIONEN AUS match_numbering.py EINFÜGEN]
   

SCHRITT 2: GENERATE_MATCHES() ERSETZEN
---------------------------------------
1. Suche in app.py nach: def generate_matches(game_name):
2. Ersetze die KOMPLETTE Funktion mit generate_matches_WITH_NUMBERING()
   (aus diesem File)
3. Benenne sie zurück in generate_matches()


SCHRITT 3: GENERATE_DOUBLE_ELIM() ERSETZEN
-------------------------------------------
1. Suche in app.py nach: def generate_double_elim(game_name):
2. Ersetze die KOMPLETTE Funktion mit generate_double_elim_WITH_NUMBERING()
3. Benenne sie zurück in generate_double_elim()


SCHRITT 4: NEUE ROUTE HINZUFÜGEN
---------------------------------
1. Suche eine passende Stelle (z.B. nach generate_double_elim)
2. Füge die neue Route ein:
   
   @app.route('/renumber_all_matches/<game_name>', methods=['POST'])
   def renumber_all_matches(game_name):
       ...


SCHRITT 5: GAME_OVERVIEW() ERWEITERN (OPTIONAL)
------------------------------------------------
1. Suche: def game_overview(game_name):
2. Ersetze mit game_overview_WITH_STATS()
3. Benenne zurück in game_overview()
4. Passe game_overview.html an (zeige Statistik)


SCHRITT 6: TESTEN
-----------------
1. Starte die App neu
2. Erstelle ein neues Turnier
3. Füge Teams hinzu
4. Generiere Spielplan
5. Prüfe, ob Spielnummern #1-#150 vergeben wurden
6. Generiere Double Elimination
7. Prüfe, ob Nummern #151+ vergeben wurden


SCHRITT 7: TEMPLATE ANPASSEN (game_overview.html)
--------------------------------------------------
Füge in game_overview.html nach den Team-Statistiken ein:

<div class="card mt-3">
    <div class="card-header">
        🔢 Spielnummern-Status
    </div>
    <div class="card-body">
        <p><strong>Round Robin:</strong> {{ numbered_rr }} / {{ match_count }} Spiele nummeriert 
           {% if numbered_rr > 0 %}(bis #{{ max_rr }}){% endif %}</p>
        
        <p><strong>Bracket A:</strong> {{ numbered_a }} Spiele nummeriert</p>
        
        <p><strong>Bracket B:</strong> {{ numbered_b }} Spiele nummeriert</p>
        
        <p><strong>Höchste Spielnummer:</strong> #{{ max_overall }}</p>
        
        {% if numbered_rr < match_count %}
        <form method="POST" action="{{ url_for('renumber_all_matches', game_name=game_name) }}">
            <button type="submit" class="btn btn-warning">
                🔄 Alle Spielnummern neu vergeben
            </button>
        </form>
        {% endif %}
    </div>
</div>


FERTIG! 🎉
----------
Nach diesen Schritten hast du:
✅ Automatische Spielnummern-Vergabe #1-#245+
✅ Durchlaufende Nummerierung über alle Phasen
✅ Manuelle Neunummerierungs-Funktion
✅ Status-Anzeige in der Übersicht
"""



@app.route('/match_overview/<game_name>')
def match_overview(game_name):
    """Übersicht aller Gruppenspiele"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM matches 
        ORDER BY group_number, round, field
    """)
    matches = cursor.fetchall()
    
    bracket_a = [m for m in matches if m['group_number'] <= 5]
    bracket_b = [m for m in matches if m['group_number'] > 5]
    
    conn.close()
    
    return render_template("admin/match_overview.html",
                         game_name=game_name,
                         bracket_a=bracket_a,
                         bracket_b=bracket_b)


@app.route('/enter_results/<game_name>')
def enter_results(game_name):
    """Ergebnisse eintragen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM matches 
        ORDER BY round, group_number, field
    """)
    matches = cursor.fetchall()
    
    conn.close()
    
    return render_template("admin/enter_results.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/save_result/<game_name>/<int:match_id>', methods=['POST'])
def save_result(game_name, match_id):
    """Einzelnes Ergebnis speichern"""
    score1 = request.form.get('score1')
    score2 = request.form.get('score2')
    
    if not score1 or not score2:
        return redirect(url_for('enter_results', game_name=game_name))
    
    try:
        score1 = int(score1)
        score2 = int(score2)
        
        if score1 > 42 or score2 > 42 or score1 < 0 or score2 < 0:
            return render_template("admin/error.html", 
                                 error_message="Punktzahl muss zwischen 0 und 42 liegen!")
    except ValueError:
        return redirect(url_for('enter_results', game_name=game_name))
    
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE matches 
        SET score1 = ?, score2 = ?
        WHERE id = ?
    """, (score1, score2, match_id))
    
    conn.commit()
    recalculate_rankings_internal(conn)
    conn.close()
    
    return redirect(url_for('enter_results', game_name=game_name))


@app.route('/delete_result/<game_name>/<int:match_id>', methods=['POST'])
def delete_result(game_name, match_id):
    """Ergebnis löschen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE matches 
        SET score1 = NULL, score2 = NULL
        WHERE id = ?
    """, (match_id,))
    
    conn.commit()
    recalculate_rankings_internal(conn)
    conn.close()
    
    return redirect(url_for('enter_results', game_name=game_name))


@app.route('/recalculate_rankings/<game_name>', methods=['POST'])
def recalculate_rankings(game_name):
    """Rankings manuell neu berechnen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    recalculate_rankings_internal(conn)
    conn.close()
    
    return redirect(url_for('group_standings', game_name=game_name))


@app.route('/group_standings/<game_name>')
def group_standings(game_name):
    """Gruppentabellen anzeigen"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    if not os.path.exists(db_path):
        return render_template("admin/error.html", error_message="Turnier nicht gefunden!")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    groups = {}
    for group_num in range(1, 11):
        groups[group_num] = sort_group_with_head_to_head(conn, group_num)
    
    # Beste 4. Platzierte Bracket A - KORRIGIERT
    cursor.execute("""
        WITH ranked_teams AS (
            SELECT r.team, r.points, r.goal_difference, r.goals_for, r.group_number,
                   ROW_NUMBER() OVER (PARTITION BY r.group_number ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC) as position
            FROM rankings r
            WHERE r.group_number BETWEEN 1 AND 5
            AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        )
        SELECT team, points, goal_difference, goals_for, group_number, position
        FROM ranked_teams
        WHERE position = 4
        ORDER BY points DESC, goal_difference DESC, goals_for DESC
        LIMIT 1
    """)
    best_4th_a = cursor.fetchone()
    
    # Beste 4. Platzierte Bracket B - KORRIGIERT
    cursor.execute("""
        WITH ranked_teams AS (
            SELECT r.team, r.points, r.goal_difference, r.goals_for, r.group_number,
                   ROW_NUMBER() OVER (PARTITION BY r.group_number ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC) as position
            FROM rankings r
            WHERE r.group_number BETWEEN 6 AND 10
            AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        )
        SELECT team, points, goal_difference, goals_for, group_number, position
        FROM ranked_teams
        WHERE position = 4
        ORDER BY points DESC, goal_difference DESC, goals_for DESC
        LIMIT 1
    """)
    best_4th_b = cursor.fetchone()
    
    conn.close()
    
    return render_template("admin/group_standings.html",
                         game_name=game_name,
                         groups=groups,
                         best_4th_a=best_4th_a,
                         best_4th_b=best_4th_b)


@app.route('/generate_double_elim/<game_name>')
def generate_double_elim(game_name):
    """Double Elimination: 1 Bracket, 32 Teams.
    Top 3 aus allen 10 Gruppen (30) + 2 beste 4. global = 32. Seeding: 1vs32...16vs17."""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches")
    if cursor.fetchone()["count"] > 0:
        conn.close()
        return render_template("admin/error.html", error_message="Double Elimination wurde bereits generiert!")

    # Top 3 aus allen 10 Gruppen = 30 Teams
    teams = []
    for g in range(1, 11):
        sorted_g = sort_group_with_head_to_head(conn, g)
        real = [t["team"] for t in sorted_g if not t["is_ghost"]]
        teams.extend(real[:3])

    # 2 beste 4. Plaetze global = 32. und 33. Team (aber nur 2 brauchen wir)
    all_fourths = []
    for g in range(1, 11):
        sorted_g = sort_group_with_head_to_head(conn, g)
        real = [t for t in sorted_g if not t["is_ghost"]]
        if len(real) >= 4:
            f = real[3]
            all_fourths.append({"team": f["team"], "goals_for": f["goals_for"],
                                "goal_difference": f["goal_difference"]})
    all_fourths.sort(key=lambda x: (-x["goals_for"], -x["goal_difference"]))
    for f in all_fourths[:2]:
        if f["team"] not in teams:
            teams.append(f["team"])

    teams = teams[:32]
    if len(teams) < 32:
        conn.close()
        return render_template("admin/error.html",
                             error_message=f"Nicht genug Teams: {len(teams)} statt 32")

    # WB R1: 16 Matches, Seeding 1vs32, 2vs31 ...
    for i in range(16):
        cursor.execute(
            "INSERT INTO double_elim_matches (round,bracket,match_index,team1,team2) VALUES (1,'Winners',?,?,?)",
            (i, teams[i], teams[31-i]))

    # WB R2-R5
    for rnd, nm in [(2,8),(3,4),(4,2),(5,1)]:
        for i in range(nm):
            cursor.execute(
                "INSERT INTO double_elim_matches (round,bracket,match_index,team1,team2) VALUES (?,'Winners',?,NULL,NULL)",
                (rnd, i))

    # LB R1-R9
    for rnd, nm in [(1,8),(2,8),(3,8),(4,8),(5,4),(6,4),(7,4),(8,2),(9,1)]:
        for i in range(nm):
            cursor.execute(
                "INSERT INTO double_elim_matches (round,bracket,match_index,team1,team2) VALUES (?,'Losers',?,NULL,NULL)",
                (rnd, i))

    conn.commit()
    cursor.execute("SELECT MAX(match_number) as max_num FROM matches")
    next_number = (cursor.fetchone()["max_num"] or 0) + 1
    assign_double_elim_match_numbers(conn, next_number)
    calculate_double_elim_times_and_courts(conn)
    conn.close()
    return redirect(url_for("double_elim_bracket", game_name=game_name))


@app.route('/double_elim_bracket/<game_name>')
def double_elim_bracket(game_name):
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM double_elim_matches ORDER BY round, bracket DESC, match_index")
    matches = cursor.fetchall()
    conn.close()
    return render_template("admin/double_elim_bracket.html",
                         game_name=game_name, matches=matches)


@app.route('/enter_double_elim_results/<game_name>')
def enter_double_elim_results(game_name):
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM double_elim_matches
        WHERE team1 IS NOT NULL AND team2 IS NOT NULL
        ORDER BY round, bracket DESC, match_index
    """)
    matches = cursor.fetchall()
    conn.close()
    return render_template("admin/enter_double_elim_results.html",
                         game_name=game_name, matches=matches)


@app.route('/update_double_elim_result/<game_name>/<int:match_id>', methods=['POST'])
def update_double_elim_result(game_name, match_id):
    score1 = int(request.form["score1"])
    score2 = int(request.form["score2"])
    if score1 > 42 or score2 > 42 or score1 < 0 or score2 < 0:
        return render_template("admin/error.html", error_message="Punktzahl 0-42!")
    if score1 == score2:
        return render_template("admin/error.html", error_message="Kein Unentschieden!")
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM double_elim_matches WHERE id=?", (match_id,))
    match = cursor.fetchone()
    if not match:
        conn.close()
        return redirect(url_for("enter_double_elim_results", game_name=game_name))
    winner = match["team1"] if score1 > score2 else match["team2"]
    loser  = match["team2"] if score1 > score2 else match["team1"]
    cursor.execute("UPDATE double_elim_matches SET score1=?,score2=?,winner=?,loser=? WHERE id=?",
                   (score1, score2, winner, loser, match_id))
    conn.commit()
    process_double_elim_forwarding(conn, match, "double_elim_matches",
                                   WINNER_MAPPING, LOSER_MAPPING, LOSER_WINNER_MAPPING)
    conn.close()
    return redirect(url_for("enter_double_elim_results", game_name=game_name))


# ============================================================================
# SUPER FINALS
# ============================================================================

@app.route('/generate_super_finals/<game_name>')
def generate_super_finals(game_name):
    """Super Finals aus dem DE-Bracket: WB-Final-Sieger vs. LB-Final-Sieger."""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM super_finals_matches")
    if cursor.fetchone()["count"] > 0:
        conn.close()
        return render_template("admin/error.html", error_message="Super Finals bereits generiert!")

    # WB Final Sieger (Runde 5, Winners)
    cursor.execute("SELECT winner FROM double_elim_matches WHERE round=5 AND bracket='Winners' AND winner IS NOT NULL LIMIT 1")
    row = cursor.fetchone()
    wb_winner = row["winner"] if row else None

    # LB Final Sieger (Runde 9, Losers)
    cursor.execute("SELECT winner FROM double_elim_matches WHERE round=9 AND bracket='Losers' AND winner IS NOT NULL LIMIT 1")
    row = cursor.fetchone()
    lb_winner = row["winner"] if row else None

    if not wb_winner or not lb_winner:
        conn.close()
        return render_template("admin/error.html",
                             error_message=f"Finalisten fehlen: WB={wb_winner}, LB={lb_winner}")

    # HF1: WB-Sieger vs. LB-Sieger
    cursor.execute("INSERT INTO super_finals_matches (match_number,match_id,team1,team2) VALUES (?,'HF1',?,?)",
                   (213, wb_winner, lb_winner))
    # FINAL + THIRD (Teams kommen nach HF1)
    cursor.execute("INSERT INTO super_finals_matches (match_number,match_id,team1,team2) VALUES (?,'FINAL',NULL,NULL)", (214,))
    cursor.execute("INSERT INTO super_finals_matches (match_number,match_id,team1,team2) VALUES (?,'THIRD',NULL,NULL)", (215,))

    conn.commit()
    conn.close()
    return redirect(url_for("super_finals_overview", game_name=game_name))


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
        cursor.execute("UPDATE super_finals_matches SET team1=? WHERE match_id='FINAL'", (winner,))
        cursor.execute("UPDATE super_finals_matches SET team2=? WHERE match_id='FINAL'", (loser,))
        cursor.execute("UPDATE super_finals_matches SET team1=? WHERE match_id='THIRD'", (loser,))
        cursor.execute("UPDATE super_finals_matches SET team2=? WHERE match_id='THIRD'", (winner,))
    
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
    
    # Die 28 Teams die NICHT im Double Elimination sind,
    # sortiert nach RR-Punkten → Top 16 → Follower Cup (Plätze 33–48)
    cursor.execute("""
        SELECT r.team, r.points, r.goal_difference, r.goals_for
        FROM rankings r
        WHERE r.team NOT IN (
            SELECT team1 FROM double_elim_matches WHERE round = 1
            UNION
            SELECT team2 FROM double_elim_matches WHERE round = 1
            UNION
            SELECT team1 FROM double_elim_matches WHERE round = 1
            UNION
            SELECT team2 FROM double_elim_matches WHERE round = 1
        )
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC
    """)
    remaining_teams = cursor.fetchall()
    all_teams = [row['team'] for row in remaining_teams[:16]]

    if len(all_teams) < 16:
        conn.close()
        return render_template("admin/error.html",
                             error_message=f"Nicht genug Teams! Nur {len(all_teams)} statt 16 (Double Elimination noch nicht generiert?)")
    
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
    
    cursor.execute("""
        SELECT r.team, r.points, r.goal_difference, r.goals_for
        FROM rankings r
        WHERE r.team NOT IN (
            SELECT team1 FROM double_elim_matches WHERE round = 1
            UNION
            SELECT team2 FROM double_elim_matches WHERE round = 1
            UNION
            SELECT team1 FROM double_elim_matches WHERE round = 1
            UNION
            SELECT team2 FROM double_elim_matches WHERE round = 1
        )
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.points DESC, r.goal_difference DESC, r.goals_for DESC
    """)
    remaining = cursor.fetchall()
    # Letzten 12 der 28 Nicht-DE-Teams → Platzierungsrunde (Plätze 49–60)
    placement_teams = [row['team'] for row in remaining[16:]]
    
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
        """, (match_number, f"P{49 + i*2}", team1, team2, court))
        
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
        groups[group_num] = sort_group_with_head_to_head(conn, group_num)
    
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
    """JSON API: Aktuelle Gruppentabellen fuer Live-Polling"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    if not os.path.exists(db_path):
        return jsonify({})
    conn = get_db_connection(db_path)
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
        groups[group_num] = [dict(row) for row in rows]
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
    for bracket, table in [('A', 'double_elim_matches'), ('B', 'double_elim_matches')]:
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
        SELECT * FROM double_elim_matches 
        ORDER BY round, bracket DESC, match_index
    """)
    matches_a = cursor.fetchall()
    
    cursor.execute("""
        SELECT * FROM double_elim_matches 
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


@app.route('/spielplan_pdf/<game_name>')
def spielplan_pdf(game_name):
    """Spielplan-Übersicht als PDF (Querformat, 8 Felder pro Seite)"""
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
        FROM matches WHERE match_number IS NOT NULL ORDER BY round, field
    """)
    matches = cursor.fetchall()
    conn.close()

    if not matches:
        return render_template("admin/error.html",
                               error_message="Keine Spiele gefunden! Bitte zuerst Spielplan generieren.")

    bracket_a, bracket_b = {}, {}
    all_rounds   = sorted(set(m['round'] for m in matches))
    all_fields_a = sorted(set(m['field'] for m in matches if m['group_number'] <= 5))
    all_fields_b = sorted(set(m['field'] for m in matches if m['group_number'] >= 6))

    for m in matches:
        f, r = m['field'], m['round']
        if m['group_number'] <= 5:
            bracket_a.setdefault(f, {})[r] = dict(m)
        else:
            bracket_b.setdefault(f, {})[r] = dict(m)

    page_w, page_h = landscape(A4)
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=landscape(A4))

    COL_BG  = colors.HexColor("#1e3a5f")
    ROW_BG  = colors.HexColor("#2d5a9e")
    WHITE   = colors.white
    EVEN_BG = colors.HexColor("#eef3ff")
    ODD_BG  = colors.white
    BORDER  = colors.HexColor("#b0bcd4")
    TITLE_C = colors.HexColor("#1e3a5f")
    GRP_C   = colors.HexColor("#2d5a9e")
    VS_C    = colors.HexColor("#aaaaaa")
    NR_C    = colors.HexColor("#cccccc")

    def trunc(text, n):
        if not text: return "-"
        return text if len(text) <= n else text[:n-1] + "\u2026"

    def draw_page(bracket_data, fields, rounds, bracket_name, sub, page_info=""):
        margin    = 10 * mm
        title_h   = 15 * mm
        num_r     = len(rounds)
        num_f     = len(fields)
        row_hdr_w = 18 * mm
        col_hdr_h = 15 * mm
        table_top = page_h - margin - title_h
        table_bot = margin + 5 * mm
        avail_h   = table_top - table_bot
        col_w     = (page_w - 2*margin - row_hdr_w) / num_r
        row_h     = (avail_h - col_hdr_h) / num_f

        # Titel
        c.setFillColor(TITLE_C)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(margin, page_h - margin - 9*mm,
                     f"Round Robin Spielplan \u2014 {bracket_name}  {page_info}")
        c.setFont("Helvetica", 8)
        c.setFillColor(colors.HexColor("#555555"))
        c.drawString(margin, page_h - margin - 14*mm, sub)

        # Spalten-Header
        c.setFillColor(COL_BG)
        c.rect(margin, table_top - col_hdr_h, row_hdr_w, col_hdr_h, fill=1, stroke=0)
        for ri, rnd in enumerate(rounds):
            x = margin + row_hdr_w + ri * col_w
            c.setFillColor(COL_BG)
            c.rect(x, table_top - col_hdr_h, col_w, col_hdr_h, fill=1, stroke=0)
            c.setFillColor(WHITE)
            c.setFont("Helvetica-Bold", 10)
            c.drawCentredString(x + col_w/2, table_top - col_hdr_h + 8*mm, f"Runde {rnd}")
            rnd_time = "\u2013"
            for f in fields:
                if f in bracket_data and rnd in bracket_data[f]:
                    rnd_time = bracket_data[f][rnd].get('time') or "\u2013"
                    break
            c.setFont("Helvetica", 9)
            c.setFillColor(colors.HexColor("#a8c4e8"))
            c.drawCentredString(x + col_w/2, table_top - col_hdr_h + 3*mm, rnd_time)

        # Zeilen
        for fi, field in enumerate(fields):
            row_y   = table_top - col_hdr_h - (fi+1)*row_h
            cell_bg = EVEN_BG if fi % 2 == 0 else ODD_BG

            # Feld-Label
            c.setFillColor(ROW_BG)
            c.rect(margin, row_y, row_hdr_w, row_h, fill=1, stroke=0)
            c.setFillColor(WHITE)
            c.setFont("Helvetica", 7)
            c.drawCentredString(margin + row_hdr_w/2, row_y + row_h*0.65, "Feld")
            c.setFont("Helvetica-Bold", 14)
            c.drawCentredString(margin + row_hdr_w/2, row_y + row_h*0.28, str(field))

            for ri, rnd in enumerate(rounds):
                x = margin + row_hdr_w + ri * col_w
                c.setFillColor(cell_bg)
                c.rect(x, row_y, col_w, row_h, fill=1, stroke=0)

                m = bracket_data.get(field, {}).get(rnd)
                if not m:
                    c.setFillColor(colors.HexColor("#cccccc"))
                    c.setFont("Helvetica", 8)
                    c.drawCentredString(x + col_w/2, row_y + row_h/2, "\u2013")
                    continue

                pad        = 3 * mm
                bottom_pad = 3 * mm
                line_h     = 4.5 * mm

                team2_y  = row_y + bottom_pad
                vs_y     = team2_y + line_h
                team1_y  = vs_y + line_h * 0.9
                header_y = row_y + row_h - 4*mm

                # Header: Gruppe + Spielnummer
                c.setFillColor(GRP_C)
                c.setFont("Helvetica-Bold", 7.5)
                c.drawString(x + pad, header_y, f"Grp {m['group_number']}")
                c.setFillColor(NR_C)
                c.setFont("Helvetica", 6.5)
                c.drawRightString(x + col_w - pad, header_y, f"#{m['match_number']}")

                # Trennlinie
                c.setStrokeColor(colors.HexColor("#dddddd"))
                c.setLineWidth(0.3)
                c.line(x + pad, header_y - 1.5*mm, x + col_w - pad, header_y - 1.5*mm)

                # Team 1
                c.setFillColor(colors.black)
                c.setFont("Helvetica-Bold", 8.5)
                c.drawString(x + pad, team1_y, trunc(m['team1'], 23))

                # vs.
                c.setFillColor(VS_C)
                c.setFont("Helvetica-Oblique", 7)
                c.drawString(x + pad, vs_y, "vs.")

                # Team 2
                c.setFillColor(colors.black)
                c.setFont("Helvetica-Bold", 8.5)
                c.drawString(x + pad, team2_y, trunc(m['team2'], 23))

        # Gitter
        total_w = row_hdr_w + num_r * col_w
        total_h = col_hdr_h + num_f * row_h
        c.setStrokeColor(BORDER)
        c.setLineWidth(0.4)
        for fi in range(num_f + 1):
            y_l = table_top - col_hdr_h - fi * row_h
            c.line(margin, y_l, margin + total_w, y_l)
        for ri in range(num_r + 1):
            x_l = margin + row_hdr_w + ri * col_w
            c.line(x_l, table_top - total_h, x_l, table_top)
        c.line(margin, table_top - total_h, margin, table_top)
        c.line(margin, table_top, margin + total_w, table_top)
        c.setStrokeColor(TITLE_C)
        c.setLineWidth(1.5)
        c.rect(margin, table_top - total_h, total_w, total_h)

        # Footer
        c.setFont("Helvetica", 7)
        c.setFillColor(colors.HexColor("#888888"))
        c.drawString(margin, margin/2 + 1*mm, game_name)
        c.drawRightString(page_w - margin, margin/2 + 1*mm,
                          f"Erstellt: {datetime.now().strftime('%d.%m.%Y %H:%M')}")

    FIELDS_PER_PAGE = 8
    for bracket_data, fields, bname in [
        (bracket_a, sorted(all_fields_a), "Bracket A (Gruppen 1\u20135)"),
        (bracket_b, sorted(all_fields_b), "Bracket B (Gruppen 6\u201310)"),
    ]:
        sub_a = "Gruppen 1, 2, 3, 4, 5  |  Top 3 je Gruppe + beste 4. qualifizieren sich f\u00fcr Double Elimination"
        sub_b = "Gruppen 6, 7, 8, 9, 10  |  Top 3 je Gruppe + beste 4. qualifizieren sich f\u00fcr Double Elimination"
        sub = sub_a if "1" in bname else sub_b
        total_pages = (len(fields) + FIELDS_PER_PAGE - 1) // FIELDS_PER_PAGE
        for i in range(0, len(fields), FIELDS_PER_PAGE):
            chunk  = fields[i:i+FIELDS_PER_PAGE]
            page_n = i // FIELDS_PER_PAGE + 1
            draw_page(bracket_data, chunk, all_rounds, bname, sub,
                      f"(Seite {page_n}/{total_pages})")
            c.showPage()

    c.save()
    buf.seek(0)
    return send_file(
        buf,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f"{game_name}_spielplan.pdf"
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
    
    cursor.execute("SELECT * FROM double_elim_matches ORDER BY round, bracket DESC, match_index")
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
    
    cursor.execute("SELECT * FROM double_elim_matches ORDER BY round, bracket DESC, match_index")
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
    
    table = "double_elim_matches"  # Einzige DE-Tabelle
    
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
# STATISTIKEN
# ============================================================================

@app.route('/final_rankings/<game_name>')
def final_rankings(game_name):
    """Finale Gesamtplatzierung"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    rankings = []
    
    # Platz 1-4: Super Finals
    cursor.execute("SELECT winner FROM super_finals_matches WHERE match_id = 'FINAL'")
    row = cursor.fetchone()
    if row and row['winner']:
        rankings.append({'place': 1, 'team': row['winner'], 'phase': 'Super Finals Sieger'})
    
    cursor.execute("""
        SELECT team1, team2, winner FROM super_finals_matches WHERE match_id = 'FINAL'
    """)
    row = cursor.fetchone()
    if row and row['winner']:
        loser = row['team2'] if row['winner'] == row['team1'] else row['team1']
        rankings.append({'place': 2, 'team': loser, 'phase': 'Super Finals Finalist'})
    
    cursor.execute("SELECT winner FROM super_finals_matches WHERE match_id = 'THIRD'")
    row = cursor.fetchone()
    if row and row['winner']:
        rankings.append({'place': 3, 'team': row['winner'], 'phase': 'Spiel um Platz 3'})
    
    cursor.execute("""
        SELECT team1, team2, winner FROM super_finals_matches WHERE match_id = 'THIRD'
    """)
    row = cursor.fetchone()
    if row and row['winner']:
        loser = row['team2'] if row['winner'] == row['team1'] else row['team1']
        rankings.append({'place': 4, 'team': loser, 'phase': 'Spiel um Platz 3'})
    
    conn.close()
    
    return render_template("admin/final_rankings.html",
                         game_name=game_name,
                         rankings=rankings)


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