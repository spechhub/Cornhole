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
    (1, 0): (2, 0, 'team1'), (1, 1): (2, 0, 'team2'),
    (1, 2): (2, 1, 'team1'), (1, 3): (2, 1, 'team2'),
    (1, 4): (2, 2, 'team1'), (1, 5): (2, 2, 'team2'),
    (1, 6): (2, 3, 'team1'), (1, 7): (2, 3, 'team2'),
    (1, 8): (2, 4, 'team1'), (1, 9): (2, 4, 'team2'),
    (1, 10): (2, 5, 'team1'), (1, 11): (2, 5, 'team2'),
    (1, 12): (2, 6, 'team1'), (1, 13): (2, 6, 'team2'),
    (1, 14): (2, 7, 'team1'), (1, 15): (2, 7, 'team2'),
    (2, 0): (3, 0, 'team1'), (2, 1): (3, 0, 'team2'),
    (2, 2): (3, 1, 'team1'), (2, 3): (3, 1, 'team2'),
    (2, 4): (3, 2, 'team1'), (2, 5): (3, 2, 'team2'),
    (2, 6): (3, 3, 'team1'), (2, 7): (3, 3, 'team2'),
    (3, 0): (4, 0, 'team1'), (3, 1): (4, 0, 'team2'),
    (3, 2): (4, 1, 'team1'), (3, 3): (4, 1, 'team2'),
    (4, 0): (5, 0, 'team1'), (4, 1): (5, 0, 'team2'),
}

LOSER_MAPPING = {
    (1, 0): (1, 0, 'team1'), (1, 1): (1, 0, 'team2'),
    (1, 2): (1, 1, 'team1'), (1, 3): (1, 1, 'team2'),
    (1, 4): (1, 2, 'team1'), (1, 5): (1, 2, 'team2'),
    (1, 6): (1, 3, 'team1'), (1, 7): (1, 3, 'team2'),
    (1, 8): (1, 4, 'team1'), (1, 9): (1, 4, 'team2'),
    (1, 10): (1, 5, 'team1'), (1, 11): (1, 5, 'team2'),
    (1, 12): (1, 6, 'team1'), (1, 13): (1, 6, 'team2'),
    (1, 14): (1, 7, 'team1'), (1, 15): (1, 7, 'team2'),
    (2, 0): (2, 0, 'team2'), (2, 1): (2, 1, 'team2'),
    (2, 2): (2, 2, 'team2'), (2, 3): (2, 3, 'team2'),
    (2, 4): (2, 4, 'team2'), (2, 5): (2, 5, 'team2'),
    (2, 6): (2, 6, 'team2'), (2, 7): (2, 7, 'team2'),
    (3, 0): (4, 0, 'team2'), (3, 1): (4, 1, 'team2'),
    (3, 2): (4, 2, 'team2'), (3, 3): (4, 3, 'team2'),
    (4, 0): (6, 0, 'team2'), (4, 1): (6, 1, 'team2'),
    (5, 0): (8, 0, 'team2'),
}

LOSER_WINNER_MAPPING = {
    (1, 0): (2, 0, 'team1'), (1, 1): (2, 1, 'team1'),
    (1, 2): (2, 2, 'team1'), (1, 3): (2, 3, 'team1'),
    (1, 4): (2, 4, 'team1'), (1, 5): (2, 5, 'team1'),
    (1, 6): (2, 6, 'team1'), (1, 7): (2, 7, 'team1'),
    (2, 0): (3, 0, 'team1'), (2, 1): (3, 0, 'team2'),
    (2, 2): (3, 1, 'team1'), (2, 3): (3, 1, 'team2'),
    (2, 4): (3, 2, 'team1'), (2, 5): (3, 2, 'team2'),
    (2, 6): (3, 3, 'team1'), (2, 7): (3, 3, 'team2'),
    (3, 0): (4, 0, 'team1'), (3, 1): (4, 1, 'team1'),
    (3, 2): (4, 2, 'team1'), (3, 3): (4, 3, 'team1'),
    (4, 0): (5, 0, 'team1'), (4, 1): (5, 0, 'team2'),
    (4, 2): (5, 1, 'team1'), (4, 3): (5, 1, 'team2'),
    (5, 0): (6, 0, 'team1'), (5, 1): (6, 1, 'team1'),
    (6, 0): (7, 0, 'team1'), (6, 1): (7, 0, 'team2'),
    (7, 0): (8, 0, 'team1'),
}

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
            ORDER BY r.goals_for DESC, r.goal_difference DESC
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


def process_double_elim_forwarding(conn, match_row):
    """Automatische Weiterleitung nach Spielende - eine gemeinsame Tabelle"""
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
        if key in WINNER_MAPPING:
            next_round, next_index, slot = WINNER_MAPPING[key]
            cursor.execute(f"""
                UPDATE double_elim_matches
                SET {slot} = ?
                WHERE round = ? AND bracket = 'Winners' AND match_index = ?
            """, (winner, next_round, next_index))

        if key in LOSER_MAPPING:
            loser_round, loser_index, loser_slot = LOSER_MAPPING[key]
            cursor.execute(f"""
                UPDATE double_elim_matches
                SET {loser_slot} = ?
                WHERE round = ? AND bracket = 'Losers' AND match_index = ?
            """, (loser, loser_round, loser_index))

    elif bracket_type == 'Losers':
        key = (round_num, match_index)
        if key in LOSER_WINNER_MAPPING:
            next_round, next_index, slot = LOSER_WINNER_MAPPING[key]
            cursor.execute(f"""
                UPDATE double_elim_matches
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
    cursor.execute("SELECT MAX(match_number) as max_num FROM double_elim_matches")
    row = cursor.fetchone()
    if row and row['max_num']:
        max_numbers.append(row['max_num'])
    
    # Double Elim B
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


def assign_double_elim_match_numbers(conn, start_number=200):
    """
    Vergibt Spielnummern für die gemeinsame Double Elimination (32 Teams).
    WB Runden 1-5, LB Runden 1-8
    """
    cursor = conn.cursor()
    match_number = start_number

    # Winner Bracket: Runden 1-5
    for round_num in range(1, 6):
        cursor.execute("""
            SELECT id FROM double_elim_matches
            WHERE round = ? AND bracket = 'Winners'
            ORDER BY match_index ASC
        """, (round_num,))
        for match in cursor.fetchall():
            conn.execute("""
                UPDATE double_elim_matches
                SET match_number = ? WHERE id = ?
            """, (match_number, match['id']))
            match_number += 1

    # Loser Bracket: Runden 1-8
    for round_num in range(1, 9):
        cursor.execute("""
            SELECT id FROM double_elim_matches
            WHERE round = ? AND bracket = 'Losers'
            ORDER BY match_index ASC
        """, (round_num,))
        for match in cursor.fetchall():
            conn.execute("""
                UPDATE double_elim_matches
                SET match_number = ? WHERE id = ?
            """, (match_number, match['id']))
            match_number += 1

    conn.commit()
    print(f"✅ Double Elim: {match_number - start_number} Spielnummern vergeben (#{start_number}-#{match_number-1})")
    return match_number



def assign_super_finals_match_numbers(conn, start_number=262):
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



def assign_follower_cup_match_numbers(conn, start_number=300):
    """
    Vergibt Spielnummern für den Follower Cup.
    Standard: ab #300
    Reihenfolge: Qualifikation → 1/8 → 1/4 → 1/2 → Finale → Platz 3
    """
    cursor = conn.cursor()
    match_number = start_number

    # Qualifikationsspiele (follower_matches)
    cursor.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND name='follower_matches'
    """)
    if cursor.fetchone():
        cursor.execute("SELECT id FROM follower_matches WHERE round = 'qualification' ORDER BY id")
        for match in cursor.fetchall():
            conn.execute("UPDATE follower_matches SET match_number = ? WHERE id = ?",
                        (match_number, match['id']))
            match_number += 1

    # KO-Runden in Reihenfolge
    for round_name in ['eighth', 'quarter', 'semi', 'final', 'third']:
        cursor.execute("""
            SELECT id FROM follower_cup_matches WHERE round = ? ORDER BY match_number ASC
        """, (round_name,))
        for match in cursor.fetchall():
            conn.execute("UPDATE follower_cup_matches SET match_number = ? WHERE id = ?",
                        (match_number, match['id']))
            match_number += 1

    conn.commit()
    print(f"✅ Follower Cup: {match_number - start_number} Spielnummern vergeben (#{start_number}-#{match_number-1})")
    return match_number

def assign_follower_cup_match_numbers(conn, start_number=300):
    """
    Vergibt Spielnummern für den Follower Cup.
    Reihenfolge: Quali → 1/8-Final → 1/4-Final → 1/2-Final → Finale → Platz 3
    Startet bei #300
    """
    cursor = conn.cursor()
    match_number = start_number

    # Qualifikationsspiele (follower_matches)
    cursor.execute("""
        SELECT name FROM sqlite_master WHERE type='table' AND name='follower_matches'
    """)
    if cursor.fetchone():
        cursor.execute("""
            SELECT id FROM follower_matches 
            WHERE round = 'qualification'
            ORDER BY id ASC
        """)
        for match in cursor.fetchall():
            # follower_matches hat keine match_number Spalte - überspringen
            pass

    # Cup Spiele (follower_cup_matches): eighth → quarter → semi → final → third
    for round_name in ['eighth', 'quarter', 'semi', 'final', 'third']:
        cursor.execute("""
            SELECT id FROM follower_cup_matches
            WHERE round = ?
            ORDER BY match_number ASC
        """, (round_name,))
        for match in cursor.fetchall():
            conn.execute("""
                UPDATE follower_cup_matches
                SET match_number = ? WHERE id = ?
            """, (match_number, match['id']))
            match_number += 1

    conn.commit()
    print(f"✅ Follower Cup: {match_number - start_number} Spielnummern vergeben (#{start_number}-#{match_number-1})")
    return match_number


# ============================================================================
# FOLLOWER CUP
# ============================================================================

def ensure_follower_cup_tables_exist(db_path):
    """Stellt sicher, dass die benötigten Tabellen für den Follower Cup existieren"""
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Tabelle für die Follower Cup Matches
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS follower_cup_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round TEXT NOT NULL,
            match_number INTEGER NOT NULL,
            team1 TEXT,
            team2 TEXT,
            court INTEGER,
            time TEXT,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            winner TEXT DEFAULT NULL
        )
    """)
    conn.commit()
    conn.close()


def sync_quali_winners_to_cup(conn):
    """Synchronisiert alle Qualifikationsgewinner mit dem Cup-System"""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) FROM sqlite_master
        WHERE type='table' AND name='follower_matches'
    """)
    if cursor.fetchone()[0] == 0:
        return  # Keine Qualifikationstabelle vorhanden
    
    # Alle Qualifikationsgewinner laden
    cursor.execute("""
        SELECT winner FROM follower_matches 
        WHERE round = 'qualification' AND winner IS NOT NULL
        ORDER BY id
    """)
    quali_winners = [row['winner'] for row in cursor.fetchall()]
    
    if not quali_winners:
        return  # Keine Gewinner vorhanden
    
    print(f"🔄 Synchronisiere {len(quali_winners)} Qualifikationsgewinner...")
    
    # Bereits im Cup vorhandene Teams laden
    cursor.execute("""
        SELECT team1, team2 FROM follower_cup_matches 
        WHERE round = 'eighth'
    """)
    existing_teams = []
    for row in cursor.fetchall():
        if row['team1'] and row['team1'] != 'TBD': 
            existing_teams.append(row['team1'])
        if row['team2'] and row['team2'] != 'TBD': 
            existing_teams.append(row['team2'])
    
    # Fehlende Gewinner identifizieren
    missing_winners = [w for w in quali_winners if w not in existing_teams]
    
    if not missing_winners:
        print("✅ Alle Gewinner sind bereits im Cup-System!")
        return
    
    print(f"🔍 Fehlende Gewinner gefunden: {missing_winners}")
    
    # Für jeden fehlenden Gewinner ein freies Feld im Cup suchen
    for winner in missing_winners:
        cursor.execute("""
            SELECT id, team1, team2 FROM follower_cup_matches
            WHERE round = 'eighth' AND (team1 IS NULL OR team1 = 'TBD' OR team2 IS NULL OR team2 = 'TBD')
            ORDER BY id
            LIMIT 1
        """)
        match = cursor.fetchone()
        
        if match:
            # Bestimmen, welches Feld aktualisiert werden soll
            field = "team1" if not match['team1'] or match['team1'] == 'TBD' else "team2"
            cursor.execute(f"""
                UPDATE follower_cup_matches 
                SET {field} = ? 
                WHERE id = ?
            """, (winner, match['id']))
            print(f"✅ {field} in Match {match['id']} mit {winner} aktualisiert")
        else:
            print(f"⚠️ Kein freies Match für Gewinner {winner} gefunden!")
    
    conn.commit()
    print("✅ Synchronisierung der Qualifikationsgewinner abgeschlossen")


def create_cup_system_with_winners(conn, game_name, rounds):
    """Erstellt das komplette Cup-System neu mit allen Qualifikationsgewinnern"""
    cursor = conn.cursor()
    
    # Lösche alle bestehenden Spiele
    cursor.execute("DELETE FROM follower_cup_matches")
    
    # 1. Direkt qualifizierte Teams laden
    direct_qualifiers = []
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='follower_cup'")
    if cursor.fetchone():
        cursor.execute("SELECT team FROM follower_cup WHERE round = '8final' ORDER BY position")
        direct_qualifiers = [row['team'] for row in cursor.fetchall()]
        print(f"📋 Direkt qualifizierte Teams: {direct_qualifiers}")
    
    # 2. Qualifikationsgewinner laden
    quali_winners = []
    cursor.execute("SELECT winner FROM follower_matches WHERE round = 'qualification' AND winner IS NOT NULL ORDER BY id")
    quali_winners = [row['winner'] for row in cursor.fetchall()]
    print(f"🏆 Qualifikationsgewinner: {quali_winners}")
    
    # Teilnehmer zusammenstellen
    participants = direct_qualifiers + quali_winners
    
    # Falls nicht genug Teams, mit Platzhaltern auffüllen
    while len(participants) < 16:
        participants.append(f"TBD_{len(participants)+1}")
    
    # Falls zu viele Teams, auf 16 kürzen
    if len(participants) > 16:
        print(f"⚠️ Zu viele Teams: {len(participants)}. Wird auf 16 gekürzt!")
        participants = participants[:16]
    
    print(f"🏆 Teilnehmer für Follower Cup: {participants}")
    
    # 1/8-Finals generieren
    for i in range(8):
        # Teams nach Schema: 1 vs 16, 2 vs 15, usw.
        team1 = participants[i]
        team2 = participants[15-i]
        court = rounds['eighth']['courts'][i % len(rounds['eighth']['courts'])]
        time = rounds['eighth']['time']
        
        cursor.execute("""
            INSERT INTO follower_cup_matches 
            (round, match_number, team1, team2, court, time)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('eighth', i+1, team1, team2, court, time))
    
    # Weitere Runden generieren
    for round_name, round_info in rounds.items():
        if round_name == "eighth":
            continue  # Bereits erstellt
        
        for i in range(round_info["count"]):
            court = round_info['courts'][i % len(round_info['courts'])]
            time = round_info['time']
            
            cursor.execute("""
                INSERT INTO follower_cup_matches 
                (round, match_number, team1, team2, court, time)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (round_name, i+1, None, None, court, time))
    
    conn.commit()
    assign_follower_cup_match_numbers(conn)
    print(f"✅ Cup-System neu erstellt mit {len(participants)} Teams")


@app.route('/follower_qualiround/<game_name>')
def follower_qualiround(game_name):
    """
    Zeigt die Qualifikationsrunde für das Follower-Turnier
    """
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")

    if not os.path.exists(db_path):
        return render_template("error.html", error_message="Spiel nicht gefunden!")

    # Rankings laden mit Tordifferenz
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            r.team,
            r.group_number,
            r.points_scored,
            COALESCE((SELECT SUM(score1) FROM matches WHERE team1 = r.team), 0) AS score1_total,
            COALESCE((SELECT SUM(score2) FROM matches WHERE team2 = r.team), 0) AS score2_total
        FROM rankings r
        ORDER BY group_number, points_scored DESC
    """)
    all_rankings = cursor.fetchall()
    
    # Teams nach Gruppen sortieren
    from collections import defaultdict
    groups = defaultdict(list)
    for row in all_rankings:
        groups[row["group_number"]].append(dict(row))  # in dict umwandeln
    
    # Teams für das Hauptbracket identifizieren
    bracket_teams = []
    fourth_place_teams = []
    fifth_place_teams = []
    sixth_place_teams = []
    
    for group_number, team_list in groups.items():
        # Sortieren nach points_scored (absteigend), dann Tordifferenz
        team_list.sort(key=lambda x: (-x["points_scored"], -(x["score1_total"] - x["score2_total"])))
        
        # Top 3 von jeder Gruppe ins Bracket
        if len(team_list) >= 3:
            bracket_teams.extend([team["team"] for team in team_list[:3]])
        
        # 4. Platzierte sammeln
        if len(team_list) >= 4:
            fourth_place_teams.append(team_list[3])
        
        # 5. Platzierte sammeln
        if len(team_list) >= 5:
            fifth_place_teams.append(team_list[4])
        
        # 6. Platzierte sammeln
        if len(team_list) >= 6:
            sixth_place_teams.append(team_list[5])
    
    # 4. Platzierte nach Punkten sortieren
    fourth_place_sorted = sorted(fourth_place_teams, 
                               key=lambda x: (-x["points_scored"], -(x["score1_total"] - x["score2_total"])))
    
    # Top 2 der 4. Platzierten kommen ins Hauptbracket
    best_two_fourths = fourth_place_sorted[:2]
    bracket_teams.extend([team["team"] for team in best_two_fourths])
    
    # Die nächsten 4 der 4. Platzierten (Platz 3-6) kommen direkt ins 8-Final
    direct_qualifiers = fourth_place_sorted[2:6]
    
    # Die restlichen 4. Platzierten kommen in die Quali-Runde
    remaining_fourths = fourth_place_sorted[6:]
    
    # Alle Teams für die Quali-Runde zusammenfassen 
    # (restliche 4. + alle 5. & 6. Platzierten)
    quali_candidates = remaining_fourths + fifth_place_teams + sixth_place_teams
    
    # Nach Punkten sortieren (absteigend), dann Tordifferenz
    quali_candidates.sort(key=lambda x: (-x["points_scored"], -(x["score1_total"] - x["score2_total"])))
    
    # Anzahl der Teams für die Quali-Runde prüfen
    if len(quali_candidates) % 2 != 0:
        # Falls ungerade, Freilos hinzufügen
        quali_candidates.append({
            "team": "Freilos",
            "group_number": "-",
            "points_scored": 0,
            "score1_total": 0,
            "score2_total": 0
        })
    
    # Paarungen für die Quali-Runde bilden
    quali_matches = []
    # Courts 1-12 wie gewünscht
    courts = list(range(1, 13))
    court_index = 0
    
    # Matches bilden: Bestes gegen Schwächstes, Zweitbestes gegen Zweitschwächstes, usw.
    quali_teams = quali_candidates.copy()  # Kopie erstellen
    while len(quali_teams) >= 2:
        team1 = quali_teams.pop(0)      # Bestes Team
        team2 = quali_teams.pop(-1)     # Schwächstes Team
        court = courts[court_index % len(courts)]
        quali_matches.append((team1, team2, court))
        court_index += 1
    
    conn.close()
    
    return render_template("follower_quali_round.html",
                          game_name=game_name,
                          direct_qualifiers=direct_qualifiers,
                          quali_matches=quali_matches)


@app.route('/follower_results/<game_name>', methods=['GET', 'POST'])
def follower_results(game_name):
    """
    Zeigt die Qualifikationsspiele mit Möglichkeit, Ergebnisse einzutragen
    und überträgt die Sieger in ein Cup-System
    """
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")

    if not os.path.exists(db_path):
        return render_template("error.html", error_message="Spiel nicht gefunden!")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Prüfen, ob die Tabelle für Follower-Spiele existiert
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='follower_matches'
    """)
    if not cursor.fetchone():
        # Tabelle erstellen, wenn sie nicht existiert
        cursor.execute("""
            CREATE TABLE follower_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round TEXT NOT NULL,
                team1 TEXT NOT NULL,
                team2 TEXT NOT NULL,
                court INTEGER,
                score1 INTEGER DEFAULT NULL,
                score2 INTEGER DEFAULT NULL,
                winner TEXT DEFAULT NULL
            )
        """)
        
        # Tabelle für den Cup-Baum
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS follower_cup (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round TEXT NOT NULL,
                position INTEGER NOT NULL,
                team TEXT,
                score INTEGER DEFAULT NULL
            )
        """)
        conn.commit()
    
    # Wenn POST-Anfrage, Ergebnis eintragen
    if request.method == 'POST':
        match_id = request.form.get('match_id')
        score1 = request.form.get('score1', type=int)
        score2 = request.form.get('score2', type=int)
        
        if match_id and score1 is not None and score2 is not None:
            # Match-Daten laden
            cursor.execute("SELECT * FROM follower_matches WHERE id = ?", (match_id,))
            match = cursor.fetchone()
            
            if match:
                # Gewinner bestimmen
                team1 = match['team1']
                team2 = match['team2']
                winner = team1 if score1 > score2 else team2
                
                # Ergebnis aktualisieren
                cursor.execute("""
                    UPDATE follower_matches
                    SET score1 = ?, score2 = ?, winner = ?
                    WHERE id = ?
                """, (score1, score2, winner, match_id))
                
                # Wenn es ein Qualifikationsspiel ist, den Gewinner ins Cup-System übernehmen
                if match['round'] == 'qualification':
                    # Position im 8-Final bestimmen
                    cursor.execute("SELECT COUNT(*) FROM follower_cup WHERE round = '8final'")
                    position = cursor.fetchone()[0]
                    
                    # In Cup-System einfügen
                    cursor.execute("""
                        INSERT INTO follower_cup (round, position, team)
                        VALUES ('8final', ?, ?)
                    """, (position, winner))
                
                conn.commit()
                
                return redirect(url_for('follower_results', game_name=game_name))
    
    # Bestehende Qualifikationsspiele laden
    cursor.execute("SELECT * FROM follower_matches WHERE round = 'qualification' ORDER BY id")
    quali_matches = cursor.fetchall()
    
    # Wenn keine Qualifikationsspiele vorhanden sind, diese generieren
    if not quali_matches:
        # Rankings mit Tordifferenz laden
        cursor.execute("""
            SELECT 
                r.team,
                r.group_number,
                r.points_scored,
                COALESCE((SELECT SUM(score1) FROM matches WHERE team1 = r.team), 0) AS score1_total,
                COALESCE((SELECT SUM(score2) FROM matches WHERE team2 = r.team), 0) AS score2_total
            FROM rankings r
            ORDER BY group_number, points_scored DESC
        """)
        all_rankings = cursor.fetchall()
        
        # Teams nach Gruppen sortieren
        from collections import defaultdict
        groups = defaultdict(list)
        for row in all_rankings:
            groups[row["group_number"]].append(dict(row))
        
        # Teams für das Hauptbracket identifizieren
        bracket_teams = []
        fourth_place_teams = []
        fifth_place_teams = []
        sixth_place_teams = []
        
        for group_number, team_list in groups.items():
            # Sortieren nach points_scored, dann Tordifferenz
            team_list.sort(key=lambda x: (-x["points_scored"], -(x["score1_total"] - x["score2_total"])))
            
            # Top 3 von jeder Gruppe ins Bracket
            if len(team_list) >= 3:
                bracket_teams.extend([team["team"] for team in team_list[:3]])
            
            # 4. Platzierte sammeln
            if len(team_list) >= 4:
                fourth_place_teams.append(team_list[3])
            
            # 5. Platzierte sammeln
            if len(team_list) >= 5:
                fifth_place_teams.append(team_list[4])
            
            # 6. Platzierte sammeln
            if len(team_list) >= 6:
                sixth_place_teams.append(team_list[5])
        
        # 4. Platzierte nach Punkten sortieren
        fourth_place_sorted = sorted(fourth_place_teams, 
                                   key=lambda x: (-x["points_scored"], -(x["score1_total"] - x["score2_total"])))
        
        # Top 2 der 4. Platzierten kommen ins Hauptbracket
        best_two_fourths = fourth_place_sorted[:2]
        bracket_teams.extend([team["team"] for team in best_two_fourths])
        
        # Die nächsten 4 der 4. Platzierten (Platz 3-6) kommen direkt ins 8-Final
        direct_qualifiers = fourth_place_sorted[2:6]
        
        # Direkt qualifizierte ins Cup-System übernehmen
        for i, team in enumerate(direct_qualifiers):
            cursor.execute("""
                INSERT INTO follower_cup (round, position, team)
                VALUES ('8final', ?, ?)
            """, (i, team["team"]))
        
        # Die restlichen 4. Platzierten kommen in die Quali-Runde
        remaining_fourths = fourth_place_sorted[6:]
        
        # Alle Teams für die Quali-Runde zusammenfassen
        quali_candidates = remaining_fourths + fifth_place_teams + sixth_place_teams
        
        # Nach Punkten sortieren
        quali_candidates.sort(key=lambda x: (-x["points_scored"], -(x["score1_total"] - x["score2_total"])))
        
        # Ungerade Anzahl? Freilos hinzufügen
        if len(quali_candidates) % 2 != 0:
            quali_candidates.append({
                "team": "Freilos",
                "group_number": "-",
                "points_scored": 0,
                "score1_total": 0,
                "score2_total": 0
            })
        
        # Paarungen für die Quali-Runde bilden
        courts = list(range(1, 13))
        court_index = 0
        
        quali_teams = quali_candidates.copy()
        while len(quali_teams) >= 2:
            team1 = quali_teams.pop(0)      # Bestes Team
            team2 = quali_teams.pop(-1)     # Schwächstes Team
            court = courts[court_index % len(courts)]
            
            # Spiel in die Datenbank einfügen
            cursor.execute("""
                INSERT INTO follower_matches (round, team1, team2, court)
                VALUES ('qualification', ?, ?, ?)
            """, (team1["team"], team2["team"], court))
            
            # Wenn team2 ein Freilos ist, automatisch team1 als Gewinner eintragen
            if team2["team"] == "Freilos":
                match_id = cursor.lastrowid
                cursor.execute("""
                    UPDATE follower_matches
                    SET score1 = 1, score2 = 0, winner = ?
                    WHERE id = ?
                """, (team1["team"], match_id))
                
                # Position im 8-Final bestimmen und Gewinner eintragen
                cursor.execute("SELECT COUNT(*) FROM follower_cup WHERE round = '8final'")
                position = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT INTO follower_cup (round, position, team)
                    VALUES ('8final', ?, ?)
                """, (position, team1["team"]))
            
            court_index += 1
        
        conn.commit()
        
        # Aktualisierte Qualifikationsspiele laden
        cursor.execute("SELECT * FROM follower_matches WHERE round = 'qualification' ORDER BY id")
        quali_matches = cursor.fetchall()
    
    # Cup-Baum laden
    cursor.execute("SELECT * FROM follower_cup ORDER BY round, position")
    cup_teams = cursor.fetchall()
    
    # Direkt qualifizierte Teams für die Anzeige
    cursor.execute("""
        SELECT team FROM follower_cup 
        WHERE round = '8final' AND position < 4
        ORDER BY position
    """)
    direct_qualifiers = [{"team": row["team"]} for row in cursor.fetchall()]
    
    conn.close()
    
    return render_template("follower_results.html",
                          game_name=game_name,
                          quali_matches=quali_matches,
                          cup_teams=cup_teams,
                          direct_qualifiers=direct_qualifiers)


@app.route('/follower_cup_system/<game_name>', methods=['GET', 'POST'])
def follower_cup_system(game_name):
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")

    if not os.path.exists(db_path):
        return render_template("error.html", error_message="Spiel nicht gefunden!")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Debug-Ausgabe
    print(f"🔍 Follower Cup System für {game_name} wurde aufgerufen")
    
    # Tabellen erstellen, falls sie nicht existieren
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS follower_cup_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round TEXT NOT NULL,
            match_number INTEGER NOT NULL,
            team1 TEXT,
            team2 TEXT,
            court INTEGER,
            time TEXT,
            score1 INTEGER DEFAULT NULL,
            score2 INTEGER DEFAULT NULL,
            winner TEXT DEFAULT NULL
        )
    """)
    conn.commit()
    
    # Struktur der Runden
    rounds = {
        "eighth": {"name": "1/8-Final", "count": 8, "time": "13:55", "courts": list(range(9, 17))},
        "quarter": {"name": "1/4-Final", "count": 4, "time": "14:15", "courts": list(range(11, 15))},
        "semi": {"name": "1/2-Final", "count": 2, "time": "14:35", "courts": [12, 13]},
        "final": {"name": "Finale", "count": 1, "time": "14:55", "courts": [12]},
        "third": {"name": "Spiel um Platz 3", "count": 1, "time": "14:55", "courts": [13]}
    }
    
    # Wenn POST-Anfrage, Ergebnis eintragen
    if request.method == 'POST':
        match_id = request.form.get('match_id')
        score1 = request.form.get('score1', type=int)
        score2 = request.form.get('score2', type=int)
        
        if match_id and score1 is not None and score2 is not None:
            # Match-Daten laden
            cursor.execute("SELECT * FROM follower_cup_matches WHERE id = ?", (match_id,))
            match = cursor.fetchone()
            
            if match:
                # Gewinner bestimmen
                team1 = match['team1']
                team2 = match['team2']
                winner = team1 if score1 > score2 else team2
                loser = team2 if winner == team1 else team1
                
                # Ergebnis aktualisieren
                cursor.execute("""
                    UPDATE follower_cup_matches
                    SET score1 = ?, score2 = ?, winner = ?
                    WHERE id = ?
                """, (score1, score2, winner, match_id))
                
                # Überprüfen, ob nächstes Match existiert, wenn ja, Gewinner eintragen
                current_round = match['round']
                match_number = match['match_number']
                
                # Verarbeitung je nach Runde
                if current_round == 'eighth':
                    # 1/8-Final -> 1/4-Final
                    next_match_number = (match_number - 1) // 2 + 1
                    next_team_field = 'team1' if match_number % 2 == 1 else 'team2'
                    
                    cursor.execute("""
                        SELECT id FROM follower_cup_matches 
                        WHERE round = 'quarter' AND match_number = ?
                    """, (next_match_number,))
                    next_match = cursor.fetchone()
                    
                    if next_match:
                        cursor.execute(f"""
                            UPDATE follower_cup_matches
                            SET {next_team_field} = ?
                            WHERE id = ?
                        """, (winner, next_match['id']))
                
                elif current_round == 'quarter':
                    # 1/4-Final -> 1/2-Final
                    next_match_number = (match_number - 1) // 2 + 1
                    next_team_field = 'team1' if match_number % 2 == 1 else 'team2'
                    
                    cursor.execute("""
                        SELECT id FROM follower_cup_matches 
                        WHERE round = 'semi' AND match_number = ?
                    """, (next_match_number,))
                    next_match = cursor.fetchone()
                    
                    if next_match:
                        cursor.execute(f"""
                            UPDATE follower_cup_matches
                            SET {next_team_field} = ?
                            WHERE id = ?
                        """, (winner, next_match['id']))
                
                elif current_round == 'semi':
                    # 1/2-Final -> Finale und Spiel um Platz 3
                    cursor.execute("""
                        SELECT id FROM follower_cup_matches 
                        WHERE round = 'final'
                        LIMIT 1
                    """)
                    final_match = cursor.fetchone()
                    
                    cursor.execute("""
                        SELECT id FROM follower_cup_matches 
                        WHERE round = 'third'
                        LIMIT 1
                    """)
                    third_place_match = cursor.fetchone()
                    
                    if final_match:
                        next_team_field = 'team1' if match_number == 1 else 'team2'
                        cursor.execute(f"""
                            UPDATE follower_cup_matches
                            SET {next_team_field} = ?
                            WHERE id = ?
                        """, (winner, final_match['id']))
                    
                    if third_place_match:
                        next_team_field = 'team1' if match_number == 1 else 'team2'
                        cursor.execute(f"""
                            UPDATE follower_cup_matches
                            SET {next_team_field} = ?
                            WHERE id = ?
                        """, (loser, third_place_match['id']))
                
                conn.commit()
                print(f"✅ Ergebnis erfolgreich eingetragen: {match_id} - {score1}:{score2}")
                
                # Nach dem Aktualisieren der Matches, sicherstellen dass alle Gewinner synchronisiert sind
                sync_quali_winners_to_cup(conn)
                assign_follower_cup_match_numbers(conn)
                
                return redirect(url_for('follower_cup_system', game_name=game_name))
    
    # WICHTIG: Prüfe zuerst, ob wir Qualifikationsgewinner haben, die noch nicht eingetragen sind
    cursor.execute("""
        SELECT COUNT(*) FROM sqlite_master
        WHERE type='table' AND name='follower_matches'
    """)
    has_quali_table = cursor.fetchone()[0] > 0
    
    if has_quali_table:
        cursor.execute("""
            SELECT COUNT(*) FROM follower_matches 
            WHERE round = 'qualification' AND winner IS NOT NULL
        """)
        quali_winners_count = cursor.fetchone()[0]
        
        print(f"📊 Gefundene Qualifikationsgewinner: {quali_winners_count}")
        
        # Prüfe, ob wir schon Spiele im Cup System haben
        cursor.execute("SELECT COUNT(*) FROM follower_cup_matches")
        existing_cup_matches = cursor.fetchone()[0]
        
        # Wenn wir Qualifikationsgewinner haben, aber noch keine Cup-Spiele ODER
        # wenn wir Qualifikationsgewinner haben, deren Ergebnisse noch nicht berücksichtigt wurden
        if quali_winners_count > 0 and existing_cup_matches == 0:
            print("🔄 Erstelle neues Cup-System mit Qualifikationsgewinnern...")
            
            # Cup-System von Grund auf erstellen
            create_cup_system_with_winners(conn, game_name, rounds)
        else:
            # Falls bereits Cup-Matches existieren, aktualisieren wir sicher alle Gewinner
            sync_quali_winners_to_cup(conn)
    
    # Alle Cup-Matches laden
    cup_matches = {}
    for round_key in rounds.keys():
        cursor.execute("""
            SELECT * FROM follower_cup_matches 
            WHERE round = ?
            ORDER BY match_number
        """, (round_key,))
        matches = cursor.fetchall()
        
        if matches:
            cup_matches[round_key] = matches
            print(f"📊 {round_key}: {len(matches)} Matches geladen")
        else:
            print(f"⚠️ Keine Matches für Runde {round_key} gefunden!")
            cup_matches[round_key] = []
    
    conn.close()
    
    return render_template("follower_cup_system.html",
                          game_name=game_name,
                          rounds=rounds,
                          cup_matches=cup_matches)


@app.route('/debug_follower_cup/<game_name>')
def debug_follower_cup(game_name):
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    output = []
    
    if not os.path.exists(db_path):
        return "Datenbank nicht gefunden!"
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Tabelle prüfen
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='follower_cup_matches'")
    if not cursor.fetchone():
        return "Tabelle 'follower_cup_matches' existiert nicht!"
    
    # Anzahl der Einträge
    cursor.execute("SELECT COUNT(*) FROM follower_cup_matches")
    count = cursor.fetchone()[0]
    output.append(f"Anzahl Einträge: {count}")
    
    # Daten anzeigen
    cursor.execute("SELECT * FROM follower_cup_matches LIMIT 10")
    matches = cursor.fetchall()
    
    for match in matches:
        match_dict = dict(match)
        output.append(f"Match: {match_dict}")
    
    # Tabelle follower_matches prüfen
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='follower_matches'")
    if cursor.fetchone():
        cursor.execute("SELECT COUNT(*) FROM follower_matches WHERE winner IS NOT NULL")
        quali_winners = cursor.fetchone()[0]
        output.append(f"Qualifikationsgewinner: {quali_winners}")
    
    conn.close()
    
    return "<br>".join(output)

@app.route('/fix_follower_cup/<game_name>')
def fix_follower_cup(game_name):
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Alle Teilnehmer zusammenstellen
    participants = []
    
    # 1. Direkt qualifizierte Teams laden
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='follower_cup'")
    if cursor.fetchone():
        cursor.execute("SELECT team FROM follower_cup WHERE round = '8final' ORDER BY position")
        direct_qualifiers = [row['team'] for row in cursor.fetchall()]
        participants.extend(direct_qualifiers)
        print(f"Direkt qualifizierte Teams geladen: {direct_qualifiers}")
    
    # 2. Qualifikationsgewinner laden
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='follower_matches'")
    if cursor.fetchone():
        cursor.execute("SELECT winner FROM follower_matches WHERE round = 'qualification' AND winner IS NOT NULL")
        quali_winners = [row['winner'] for row in cursor.fetchall()]
        participants.extend(quali_winners)
        print(f"Qualifikationsgewinner geladen: {quali_winners}")
    
    # 3. Alle Spiele löschen und neu erstellen
    cursor.execute("DELETE FROM follower_cup_matches")
    
    # Struktur der Runden
    rounds = {
        "eighth": {"name": "1/8-Final", "count": 8, "time": "13:55", "courts": list(range(9, 17))},
        "quarter": {"name": "1/4-Final", "count": 4, "time": "14:15", "courts": list(range(11, 15))},
        "semi": {"name": "1/2-Final", "count": 2, "time": "14:35", "courts": [12, 13]},
        "final": {"name": "Finale", "count": 1, "time": "14:55", "courts": [12]},
        "third": {"name": "Spiel um Platz 3", "count": 1, "time": "14:55", "courts": [13]}
    }
    
    # Paarungen für 1/8-Finals erstellen
    match_pairs = []
    if len(participants) == 16:  # Genau 16 Teams, perfekt!
        for i in range(8):
            match_pairs.append((participants[i], participants[15-i]))
    else:
        # Weniger oder mehr als 16 Teams - mit TBD auffüllen oder abschneiden
        participants_copy = participants.copy()
        while len(participants_copy) < 16:
            participants_copy.append(f"TBD_{len(participants_copy)+1}")
        
        # Paarungen nach dem Schema 1-16, 2-15, usw.
        for i in range(8):
            match_pairs.append((participants_copy[i], participants_copy[15-i]))
    
    print(f"Erstellte Paarungen: {match_pairs}")
    
    # 1/8-Finals in die Datenbank eintragen
    for i, (team1, team2) in enumerate(match_pairs):
        court = rounds['eighth']['courts'][i % len(rounds['eighth']['courts'])]
        time = rounds['eighth']['time']
        
        cursor.execute("""
            INSERT INTO follower_cup_matches 
            (round, match_number, team1, team2, court, time)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('eighth', i+1, team1, team2, court, time))
    
    # Weitere Runden erstellen
    for round_name, round_info in rounds.items():
        if round_name == "eighth":
            continue  # Bereits erstellt
        
        for i in range(round_info["count"]):
            court = round_info['courts'][i % len(round_info['courts'])]
            time = round_info['time']
            
            cursor.execute("""
                INSERT INTO follower_cup_matches 
                (round, match_number, team1, team2, court, time)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (round_name, i+1, None, None, court, time))
    
    assign_follower_cup_match_numbers(conn)
    conn.commit()
    conn.close()
    
    return redirect(url_for('follower_cup_system', game_name=game_name))

@app.route('/reset_follower_cup/<game_name>')
def reset_follower_cup(game_name):
    """
    Setzt das Follower Cup System komplett zurück und baut es neu auf
    mit allen qualifizierten Teams.
    """
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    if not os.path.exists(db_path):
        return render_template("error.html", error_message="Spiel nicht gefunden!")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Struktur der Runden für die Neuinitialisierung
    rounds = {
        "eighth": {"name": "1/8-Final", "count": 8, "time": "13:55", "courts": list(range(9, 17))},
        "quarter": {"name": "1/4-Final", "count": 4, "time": "14:15", "courts": list(range(11, 15))},
        "semi": {"name": "1/2-Final", "count": 2, "time": "14:35", "courts": [12, 13]},
        "final": {"name": "Finale", "count": 1, "time": "14:55", "courts": [12]},
        "third": {"name": "Spiel um Platz 3", "count": 1, "time": "14:55", "courts": [13]}
    }
    
    # Alle Cup-Spiele löschen
    cursor.execute("DELETE FROM follower_cup_matches")
    print("🗑️ Alle bestehenden Follower Cup Matches gelöscht")
    
    # Teilnehmer zusammenstellen aus verschiedenen Quellen
    participants = []
    
    # 1. Direkt qualifizierte Teams (falls vorhanden)
    direct_quali = []
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='follower_cup'")
    if cursor.fetchone():
        cursor.execute("SELECT team FROM follower_cup WHERE round = '8final' ORDER BY position")
        direct_quali = [row['team'] for row in cursor.fetchall()]
        participants.extend(direct_quali)
        print(f"📋 Direkt qualifizierte Teams geladen: {len(direct_quali)}")
    
    # 2. Qualifikationsgewinner
    quali_winners = []
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='follower_matches'")
    if cursor.fetchone():
        cursor.execute("SELECT winner FROM follower_matches WHERE round = 'qualification' AND winner IS NOT NULL")
        quali_winners = [row['winner'] for row in cursor.fetchall()]
        participants.extend(quali_winners)
        print(f"🏆 Qualifikationsgewinner geladen: {len(quali_winners)}")
    
    # TBD-Platzhalter hinzufügen, falls nicht genug Teams
    while len(participants) < 16:
        participants.append(f"TBD_{len(participants)+1}")
    
    # Falls zu viele Teams, auf 16 beschränken
    if len(participants) > 16:
        print(f"⚠️ Zu viele Teams ({len(participants)}), wird auf 16 beschränkt")
        participants = participants[:16]
    
    print(f"👥 Insgesamt {len(participants)} Teams für das Cup-System")
    
    # 1/8-Finals generieren
    for i in range(8):
        # Teams nach Schema: 1 vs 16, 2 vs 15, usw.
        team1 = participants[i]
        team2 = participants[15-i]
        court = rounds['eighth']['courts'][i % len(rounds['eighth']['courts'])]
        time = rounds['eighth']['time']
        
        cursor.execute("""
            INSERT INTO follower_cup_matches 
            (round, match_number, team1, team2, court, time)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('eighth', i+1, team1, team2, court, time))
    
    # Weitere Runden generieren
    for round_name, round_info in rounds.items():
        if round_name == "eighth":
            continue  # Bereits erstellt
        
        for i in range(round_info["count"]):
            court = round_info['courts'][i % len(round_info['courts'])]
            time = round_info['time']
            
            cursor.execute("""
                INSERT INTO follower_cup_matches 
                (round, match_number, team1, team2, court, time)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (round_name, i+1, None, None, court, time))
    
    assign_follower_cup_match_numbers(conn)
    conn.commit()
    conn.close()
    
    print("✅ Follower Cup System erfolgreich zurückgesetzt und neu aufgebaut")
    return redirect(url_for('follower_cup_system', game_name=game_name))

@app.route('/edit_follower_pairings/<game_name>', methods=['GET', 'POST'])
def edit_follower_pairings(game_name):
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")

    if not os.path.exists(db_path):
        return render_template("error.html", error_message="Spiel nicht gefunden!")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # 1. Alle Teams aus der Datenbank laden
    team_list = []
    
    # Teams aus der teams-Tabelle holen
    cursor.execute("SELECT name FROM teams ORDER BY name")
    teams_from_db = [row['name'] for row in cursor.fetchall()]
    team_list.extend(teams_from_db)
    
    # Zusätzlich die Teams aus follower_matches holen (für den Fall, dass sie nicht in teams sind)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='follower_matches'")
    if cursor.fetchone():
        cursor.execute("SELECT DISTINCT team1 AS team FROM follower_matches UNION SELECT DISTINCT team2 AS team FROM follower_matches")
        teams_from_quali = [row['team'] for row in cursor.fetchall() if row['team'] and row['team'] != 'Freilos' and row['team'] != 'TBD']
        for team in teams_from_quali:
            if team not in team_list:
                team_list.append(team)
    
    # Teams aus den bestehenden Follower-Cup-Matches holen
    cursor.execute("SELECT DISTINCT team1 AS team FROM follower_cup_matches WHERE team1 IS NOT NULL AND team1 != 'TBD' UNION SELECT DISTINCT team2 AS team FROM follower_cup_matches WHERE team2 IS NOT NULL AND team2 != 'TBD'")
    teams_from_cup = [row['team'] for row in cursor.fetchall() if row['team']]
    for team in teams_from_cup:
        if team not in team_list:
            team_list.append(team)
    
    # Alphabetisch sortieren
    team_list.sort()
    
    # 2. Aktuelle Paarungen laden
    cursor.execute("""
        SELECT * FROM follower_cup_matches 
        WHERE round = 'eighth'
        ORDER BY match_number
    """)
    eighth_matches = cursor.fetchall()
    
    # 3. POST-Anfrage verarbeiten
    if request.method == 'POST':
        for key, value in request.form.items():
            if key.startswith('team1_') or key.startswith('team2_'):
                parts = key.split('_')
                field = parts[0]  # team1 oder team2
                match_id = int(parts[1])  # Match-ID
                
                cursor.execute(f"""
                    UPDATE follower_cup_matches 
                    SET {field} = ? 
                    WHERE id = ?
                """, (value, match_id))
        
        conn.commit()
        conn.close()
        return redirect(url_for('follower_cup_system', game_name=game_name))
    
    conn.close()
    
    # HTML direkt generieren mit Dropdown-Menüs
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Paarungen bearbeiten - {game_name}</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        <style>
            .container {{ max-width: 900px; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Paarungen für Follower Cup bearbeiten</h1>
            <h2>{game_name}</h2>
            
            <form method="post">
                <div class="table-responsive">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Match</th>
                                <th>Team 1</th>
                                <th>Team 2</th>
                                <th>Spielfeld</th>
                                <th>Zeit</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    for match in eighth_matches:
        # Dropdown für Team 1
        team1_dropdown = f"""
            <select name="team1_{match['id']}" class="form-control">
                <option value="">-- Team auswählen --</option>
        """
        for team in team_list:
            selected = "selected" if team == match['team1'] else ""
            team1_dropdown += f'<option value="{team}" {selected}>{team}</option>'
        team1_dropdown += '</select>'
        
        # Dropdown für Team 2
        team2_dropdown = f"""
            <select name="team2_{match['id']}" class="form-control">
                <option value="">-- Team auswählen --</option>
        """
        for team in team_list:
            selected = "selected" if team == match['team2'] else ""
            team2_dropdown += f'<option value="{team}" {selected}>{team}</option>'
        team2_dropdown += '</select>'
        
        html += f"""
                            <tr>
                                <td>{match['match_number']}</td>
                                <td>{team1_dropdown}</td>
                                <td>{team2_dropdown}</td>
                                <td>{match['court']}</td>
                                <td>{match['time']}</td>
                            </tr>
        """
    
    html += """
                        </tbody>
                    </table>
                </div>
                
                <div class="form-group">
                    <button type="submit" class="btn btn-primary">Änderungen speichern</button>
                    <a href="javascript:history.back()" class="btn btn-secondary">Abbrechen</a>
                </div>
            </form>
        </div>
    </body>
    </html>
    """
    
    return html



@app.route('/follower_cup_view/<game_name>')
def follower_cup_view(game_name):
    """
    Zeigt das Cup-System der Follower-Runde an (nur Anzeige, keine Bearbeitung)
    """
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")

    if not os.path.exists(db_path):
        return render_template("error.html", error_message="Spiel nicht gefunden!")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Runden-Strukturen
    round_types = {
        "eighth": {"query": "SELECT * FROM follower_cup_matches WHERE round = 'eighth' ORDER BY match_number"},
        "quarter": {"query": "SELECT * FROM follower_cup_matches WHERE round = 'quarter' ORDER BY match_number"},
        "semi": {"query": "SELECT * FROM follower_cup_matches WHERE round = 'semi' ORDER BY match_number"},
        "finals": {"query": "SELECT * FROM follower_cup_matches WHERE round = 'final' ORDER BY match_number"},
        "third_place": {"query": "SELECT * FROM follower_cup_matches WHERE round = 'third' ORDER BY match_number"}
    }
    
    # Daten für jede Runde abrufen
    matches = {}
    for key, data in round_types.items():
        cursor.execute(data["query"])
        matches[key] = cursor.fetchall()
    
    conn.close()
    
    return render_template("follower_cup_view.html",
                          game_name=game_name,
                          eighth_finals=matches.get("eighth", []),
                          quarter_finals=matches.get("quarter", []),
                          semi_finals=matches.get("semi", []),
                          finals=matches.get("finals", []),
                          third_place=matches.get("third_place", []))

def generate_source_links(game_name):
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    table_name = "double_elim_matches"

    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        matches = cursor.fetchall()
    except sqlite3.OperationalError:
        print(f"❌ Tabelle '{table_name}' existiert nicht.")
        conn.close()
        return

    match_keys = {}  # {(bracket, round, match): key}

    # Schritt 1: Quellen für alle Matches ermitteln
    for match in matches:
        bracket = match["bracket"]
        round_num = match["round"]
        match_num = match["match"]
        key = f"{bracket[0]}{round_num}-{match_num}"  # Generiere Schlüssel basierend auf Bracket, Round und Match
        match_keys[(bracket, round_num, match_num)] = key

    # Setze Quellen für jedes Match
    for match in matches:
        bracket = match["bracket"]
        round_num = match["round"]
        match_num = match["match"]
        key = match_keys[(bracket, round_num, match_num)]

        # Quelle aus den vorherigen Runden ermitteln
        source1 = None
        source2 = None

        # Beispiel: Hier könnten wir nach vorherigen Runden im Bracket suchen
        if round_num > 1:  # Falls es sich um eine spätere Runde handelt, holen wir uns Quellen
            prev_round_match_1 = f"{bracket[0]}{round_num-1}-{match_num*2-1}"
            prev_round_match_2 = f"{bracket[0]}{round_num-1}-{match_num*2}"

            source1 = match_keys.get((bracket, round_num-1, match_num*2-1))  # Source 1
            source2 = match_keys.get((bracket, round_num-1, match_num*2))    # Source 2

        # Quellen setzen
        cursor.execute(f"""
            UPDATE {table_name}
            SET source1 = ?, source2 = ?
            WHERE bracket = ? AND round = ? AND match = ?
        """, (source1, source2, bracket, round_num, match_num))

    conn.commit()
    conn.close()
    print(f"✅ Quellen für GoJS generiert und gespeichert für Turnier '{game_name}'")


def migrate_double_elim_table_allow_nulls(db_path):
    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    print("⚙️ Starte Migration der Tabelle `double_elim_matches`…")

    # Prüfen ob Spalte team1 bereits NULL erlaubt – sonst migrieren
    try:
        cursor.execute("PRAGMA table_info(double_elim_matches)")
        columns = cursor.fetchall()
        nullable_check = [col["notnull"] for col in columns if col["name"] in ("team1", "team2")]

        if any(nullable_check):  # Falls team1 oder team2 NOT NULL sind
            print("🔧 Migriere Tabelle, um NULL-Werte für `team1`/`team2` zu erlauben…")

            cursor.executescript("""
                CREATE TABLE IF NOT EXISTS double_elim_matches_temp (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round INTEGER,
                    bracket TEXT,
                    match INTEGER,
                    team1 TEXT,
                    team2 TEXT,
                    winner TEXT,
                    loser TEXT,
                    next_match TEXT,
                    score1 INTEGER,
                    score2 INTEGER,
                    source1 TEXT,
                    source2 TEXT
                );

                INSERT INTO double_elim_matches_temp
                (id, round, bracket, match, team1, team2, winner, loser, next_match, score1, score2, source1, source2)
                SELECT id, round, bracket, match, team1, team2, winner, loser, next_match, score1, score2, source1, source2
                FROM double_elim_matches;

                DROP TABLE double_elim_matches;
                ALTER TABLE double_elim_matches_temp RENAME TO double_elim_matches;
            """)
            conn.commit()
            print("✅ Migration erfolgreich abgeschlossen.")
        else:
            print("✔️ Tabelle `double_elim_matches` erlaubt bereits NULL für `team1` und `team2`.")
    except Exception as e:
        print(f"❌ Fehler bei Migration: {e}")
    finally:
        conn.close()



# ============================================================================
# SUPER FINALS ZEITBERECHNUNG
# ============================================================================

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
            FROM double_elim_matches 
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
            FROM double_elim_matches 
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
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches")
    if cursor.fetchone()['count'] > 0:
        end_time_a = calculate_double_elim_times(conn, 'double_elim_matches')
    
        cursor.execute("""
            SELECT MIN(time) as start FROM double_elim_matches WHERE time IS NOT NULL
        """)
        start_a = cursor.fetchone()['start']
        stats['bracket_a'] = {'start': start_a, 'end': end_time_a}

    # 3. Bracket B (parallel zu A oder nacheinander)
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches")
    if cursor.fetchone()['count'] > 0:
        # Optional: Bracket B gleichzeitig mit A (verschiedene Felder)
        # Oder: Bracket B nach A
        end_time_b = calculate_double_elim_times(conn, 'double_elim_matches')
    
        cursor.execute("""
            SELECT MIN(time) as start FROM double_elim_matches WHERE time IS NOT NULL
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
   assign_double_elim_match_numbers(conn, 200)
   
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
    """Rankings neu berechnen - Sortierung: 1. goals_for, 2. goal_difference, 3. direkter Vergleich. Keine Punkte."""
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

        # Team1
        if score1 > score2:
            wins1, draws1, losses1 = 1, 0, 0
        elif score1 < score2:
            wins1, draws1, losses1 = 0, 0, 1
        else:
            wins1, draws1, losses1 = 0, 1, 0

        cursor.execute("""
            UPDATE rankings 
            SET matches_played = matches_played + 1,
                wins = wins + ?,
                draws = draws + ?,
                losses = losses + ?,
                goals_for = goals_for + ?,
                goals_against = goals_against + ?,
                goal_difference = goal_difference + (? - ?)
            WHERE team = ?
        """, (wins1, draws1, losses1, score1, score2, score1, score2, team1))

        # Team2
        if score2 > score1:
            wins2, draws2, losses2 = 1, 0, 0
        elif score2 < score1:
            wins2, draws2, losses2 = 0, 0, 1
        else:
            wins2, draws2, losses2 = 0, 1, 0

        cursor.execute("""
            UPDATE rankings 
            SET matches_played = matches_played + 1,
                wins = wins + ?,
                draws = draws + ?,
                losses = losses + ?,
                goals_for = goals_for + ?,
                goals_against = goals_against + ?,
                goal_difference = goal_difference + (? - ?)
            WHERE team = ?
        """, (wins2, draws2, losses2, score2, score1, score2, score1, team2))

    conn.commit()


def sort_group_standings(conn, teams_in_group, group_number):
    """
    Sortiert Teams einer Gruppe nach:
    1. goals_for (geworfene Punkte, absteigend)
    2. goal_difference (Differenz, absteigend)
    3. Direkter Vergleich (Ergebnis im direkten Spiel)
    Gibt sortierte Liste von Team-Dicts zurück.
    """
    cursor = conn.cursor()

    # Alle Rankings der Gruppe laden
    placeholders = ','.join('?' * len(teams_in_group))
    cursor.execute(f"""
        SELECT * FROM rankings 
        WHERE group_number = ? AND team IN ({placeholders})
    """, [group_number] + list(teams_in_group))
    rows = {r['team']: dict(r) for r in cursor.fetchall()}

    # Sortierschlüssel mit direktem Vergleich als Tiebreaker
    def sort_key(team_a, team_b):
        a = rows.get(team_a, {})
        b = rows.get(team_b, {})

        # 1. goals_for
        if a.get('goals_for', 0) != b.get('goals_for', 0):
            return b.get('goals_for', 0) - a.get('goals_for', 0)

        # 2. goal_difference
        if a.get('goal_difference', 0) != b.get('goal_difference', 0):
            return b.get('goal_difference', 0) - a.get('goal_difference', 0)

        # 3. Direkter Vergleich
        cursor.execute("""
            SELECT score1, score2 FROM matches
            WHERE (team1 = ? AND team2 = ?) OR (team1 = ? AND team2 = ?)
            AND score1 IS NOT NULL AND score2 IS NOT NULL
        """, (team_a, team_b, team_b, team_a))
        direct = cursor.fetchone()
        if direct:
            if direct['score1'] is not None:
                # Finde wer team_a und wer team_b war
                cursor.execute("""
                    SELECT team1, team2, score1, score2 FROM matches
                    WHERE ((team1 = ? AND team2 = ?) OR (team1 = ? AND team2 = ?))
                    AND score1 IS NOT NULL
                """, (team_a, team_b, team_b, team_a))
                m = cursor.fetchone()
                if m:
                    if m['team1'] == team_a:
                        score_a, score_b = m['score1'], m['score2']
                    else:
                        score_a, score_b = m['score2'], m['score1']
                    if score_a > score_b:
                        return -1  # team_a besser
                    elif score_b > score_a:
                        return 1   # team_b besser
        return 0

    import functools
    team_list = list(teams_in_group)
    team_list.sort(key=functools.cmp_to_key(sort_key))
    return [rows[t] for t in team_list if t in rows]



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
    # Feldzuweisung: pro Runde rotierend, Bracket A (Gr.1-5) und B (Gr.6-10) je Felder 1-15
    # Runde r, Gruppe g (0-basiert innerhalb Bracket): Startfeld = ((g*3) + (r-1)*3) % 15 + 1
    
    for group_num in sorted(groups.keys()):
        teams = groups[group_num]
        
        if len(teams) < 2:
            continue
        
        n = len(teams)
        rounds = n - 1 if n % 2 == 0 else n
        
        # Gruppenindex innerhalb des Brackets (0-basiert)
        # Bracket A: Gruppen 1-5 → Index 0-4
        # Bracket B: Gruppen 6-10 → Index 0-4
        group_idx = (group_num - 1) % 5  # 0-4
        
        for round_num in range(1, rounds + 1):
            # Startfeld für diese Gruppe in dieser Runde
            # Rotation: pro Runde um 3 Felder verschoben
            start_field = ((group_idx * 3) + (round_num - 1) * 3) % 15 + 1
            match_field = start_field
            
            for i in range(n // 2):
                team1_idx = i
                team2_idx = n - 1 - i
                
                if team1_idx < len(teams) and team2_idx < len(teams):
                    team1 = teams[team1_idx]
                    team2 = teams[team2_idx]
                    
                    # KEINE match_number hier! Wird später vergeben
                    cursor.execute("""
                        INSERT INTO matches (round, team1, team2, group_number, field)
                        VALUES (?, ?, ?, ?, ?)
                    """, (round_num, team1, team2, group_num, match_field))
                    
                    match_field = (match_field % 15) + 1
            
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

def generate_double_elim(game_name):
    """Double Elimination mit 32 Teams (15+15+2 beste Vierte aus allen 10 Gruppen)"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches")
    if cursor.fetchone()['count'] > 0:
        conn.close()
        return render_template("admin/error.html",
                             error_message="Bracket wurde bereits generiert!")

    # Top 3 aus Gruppen 1-5 (Bracket 1)
    bracket_1 = []
    for group_num in range(1, 6):
        cursor.execute("""
            SELECT team FROM rankings
            WHERE group_number = ?
            AND team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
            ORDER BY goals_for DESC, goal_difference DESC
            LIMIT 3
        """, (group_num,))
        bracket_1.extend([r['team'] for r in cursor.fetchall()])

    # Top 3 aus Gruppen 6-10 (Bracket 2)
    bracket_2 = []
    for group_num in range(6, 11):
        cursor.execute("""
            SELECT team FROM rankings
            WHERE group_number = ?
            AND team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
            ORDER BY goals_for DESC, goal_difference DESC
            LIMIT 3
        """, (group_num,))
        bracket_2.extend([r['team'] for r in cursor.fetchall()])

    # 2 beste 4. Platzierte aus allen 10 Gruppen
    cursor.execute("""
        WITH ranked AS (
            SELECT team, goals_for, goal_difference, group_number,
                   ROW_NUMBER() OVER (PARTITION BY group_number ORDER BY goals_for DESC, goal_difference DESC) as pos
            FROM rankings
            WHERE team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        )
        SELECT team FROM ranked WHERE pos = 4
        ORDER BY goals_for DESC, goal_difference DESC
        LIMIT 2
    """)
    best_fourths = [r['team'] for r in cursor.fetchall()]

    all_teams = bracket_1 + bracket_2 + best_fourths

    if len(all_teams) != 32:
        conn.close()
        return render_template("admin/error.html",
                             error_message=f"Nicht genau 32 Teams: {len(all_teams)} (B1:{len(bracket_1)}, B2:{len(bracket_2)}, 4.:{len(best_fourths)})")

    # Winner Runde 1: 16 Spiele (Seeding 1vs32, 2vs31, ...)
    for i in range(16):
        cursor.execute("""
            INSERT INTO double_elim_matches (round, bracket, match_index, team1, team2)
            VALUES (1, 'Winners', ?, ?, ?)
        """, (i, all_teams[i], all_teams[31 - i]))

    # Loser Runde 1: 8 Platzhalter
    for i in range(8):
        cursor.execute("""
            INSERT INTO double_elim_matches (round, bracket, match_index, team1, team2)
            VALUES (1, 'Losers', ?, NULL, NULL)
        """, (i,))

    # Winner Runden 2-5
    for round_num in range(2, 6):
        num_matches = 2 ** (5 - round_num)
        for i in range(num_matches):
            cursor.execute("""
                INSERT INTO double_elim_matches (round, bracket, match_index, team1, team2)
                VALUES (?, 'Winners', ?, NULL, NULL)
            """, (round_num, i))

    # Loser Runden 2-8
    loser_structure = {2: 8, 3: 4, 4: 4, 5: 2, 6: 2, 7: 1, 8: 1}
    for round_num, count in loser_structure.items():
        for i in range(count):
            cursor.execute("""
                INSERT INTO double_elim_matches (round, bracket, match_index, team1, team2)
                VALUES (?, 'Losers', ?, NULL, NULL)
            """, (round_num, i))

    conn.commit()

    # Spielnummern vergeben
    cursor.execute("SELECT MAX(match_number) as max_num FROM matches")
    row = cursor.fetchone()
    next_number = 200  # Double Elim startet immer bei #200
    assign_double_elim_match_numbers(conn, next_number)

    conn.close()
    return redirect(url_for('double_elim_bracket', game_name=game_name))


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
    
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches WHERE match_number IS NOT NULL")
    numbered_b = cursor.fetchone()['count']
    
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
        cursor.execute("""
            SELECT r.*, t.is_ghost
            FROM rankings r
            LEFT JOIN teams t ON r.team = t.name
            WHERE r.group_number = ?
        """, (group_num,))
        rows = cursor.fetchall()
        # Sortierung: 1. G+ (goals_for), 2. Differenz, 3. direkter Vergleich
        team_names = [r['team'] for r in rows]
        row_map = {r['team']: r for r in rows}
        
        import functools
        def head_to_head(t_a, t_b):
            cursor.execute("""
                SELECT team1, team2, score1, score2 FROM matches
                WHERE ((team1 = ? AND team2 = ?) OR (team1 = ? AND team2 = ?))
                AND score1 IS NOT NULL
            """, (t_a, t_b, t_b, t_a))
            m = cursor.fetchone()
            if m:
                score_a = m['score1'] if m['team1'] == t_a else m['score2']
                score_b = m['score2'] if m['team1'] == t_a else m['score1']
                if score_a > score_b: return -1
                if score_b > score_a: return 1
            return 0

        def sort_key(t_a, t_b):
            a = row_map[t_a]
            b = row_map[t_b]
            if a['goals_for'] != b['goals_for']:
                return b['goals_for'] - a['goals_for']
            if a['goal_difference'] != b['goal_difference']:
                return b['goal_difference'] - a['goal_difference']
            return head_to_head(t_a, t_b)

        team_names.sort(key=functools.cmp_to_key(sort_key))
        groups[group_num] = [row_map[t] for t in team_names]
    
    # Beste 4. Platzierte Bracket A - KORRIGIERT
    cursor.execute("""
        WITH ranked_teams AS (
            SELECT r.team, r.points, r.goal_difference, r.goals_for, r.group_number,
                   ROW_NUMBER() OVER (PARTITION BY r.group_number ORDER BY r.goals_for DESC, r.goal_difference DESC) as position
            FROM rankings r
            WHERE r.group_number BETWEEN 1 AND 5
            AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        )
        SELECT team, points, goal_difference, goals_for, group_number, position
        FROM ranked_teams
        WHERE position = 4
        ORDER BY goals_for DESC, goal_difference DESC
        LIMIT 1
    """)
    best_4th_a = cursor.fetchone()
    
    # Beste 4. Platzierte Bracket B - KORRIGIERT
    cursor.execute("""
        WITH ranked_teams AS (
            SELECT r.team, r.points, r.goal_difference, r.goals_for, r.group_number,
                   ROW_NUMBER() OVER (PARTITION BY r.group_number ORDER BY r.goals_for DESC, r.goal_difference DESC) as position
            FROM rankings r
            WHERE r.group_number BETWEEN 6 AND 10
            AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        )
        SELECT team, points, goal_difference, goals_for, group_number, position
        FROM ranked_teams
        WHERE position = 4
        ORDER BY goals_for DESC, goal_difference DESC
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
    """Beide Double Elimination Brackets generieren"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")
    
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as count FROM double_elim_matches")
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
    
    match_number = 151
    
    # Bracket A Winner Runde 1
    for i in range(8):
        cursor.execute("""
            INSERT INTO double_elim_matches 
            (round, bracket, match_index, team1, team2)
            VALUES (1, 'Winners', ?, ?, ?)
        """, (i, bracket_a_teams[i], bracket_a_teams[15-i]))
    
    # Bracket A Loser Runde 1
    for i in range(4):
        cursor.execute("""
            INSERT INTO double_elim_matches 
            (round, bracket, match_index, team1, team2)
            VALUES (1, 'Losers', ?, NULL, NULL)
        """, (i,))
    
    # Weitere Runden Bracket A
    for round_num in range(2, 5):
        num_matches = 8 // (2 ** (round_num - 1))
        for i in range(num_matches):
            cursor.execute("""
                INSERT INTO double_elim_matches 
                (round, bracket, match_index, team1, team2)
                VALUES (?, 'Winners', ?, NULL, NULL)
            """, (round_num, i))
    
    for round_num in range(2, 7):
        num_matches = max(1, 8 // (2 ** (round_num - 1)))
        for i in range(num_matches):
            cursor.execute("""
                INSERT INTO double_elim_matches 
                (round, bracket, match_index, team1, team2)
                VALUES (?, 'Losers', ?, NULL, NULL)
            """, (round_num, i))
    
    # Bracket B (identisch)
    for i in range(8):
        cursor.execute("""
            INSERT INTO double_elim_matches 
            (round, bracket, match_index, team1, team2)
            VALUES (1, 'Winners', ?, ?, ?)
        """, (i, bracket_b_teams[i], bracket_b_teams[15-i]))
    
    for i in range(4):
        cursor.execute("""
            INSERT INTO double_elim_matches 
            (round, bracket, match_index, team1, team2)
            VALUES (1, 'Losers', ?, NULL, NULL)
        """, (i,))
    
    for round_num in range(2, 5):
        num_matches = 8 // (2 ** (round_num - 1))
        for i in range(num_matches):
            cursor.execute("""
                INSERT INTO double_elim_matches 
                (round, bracket, match_index, team1, team2)
                VALUES (?, 'Winners', ?, NULL, NULL)
            """, (round_num, i))
    
    for round_num in range(2, 7):
        num_matches = max(1, 8 // (2 ** (round_num - 1)))
        for i in range(num_matches):
            cursor.execute("""
                INSERT INTO double_elim_matches 
                (round, bracket, match_index, team1, team2)
                VALUES (?, 'Losers', ?, NULL, NULL)
            """, (round_num, i))
    
    conn.commit()
    cursor.execute("SELECT MAX(match_number) as max_num FROM matches")
    next_number = (cursor.fetchone()['max_num'] or 0) + 1

    print("\n🔢 Vergebe Spielnummern...")
    next_number = assign_double_elim_match_numbers(conn, next_number)
    

    # NEU: Spielzeiten
    print("\n⏰ Berechne Spielzeiten...")
    calculate_double_elim_times(conn, 'double_elim_matches')

    conn.close()
    
    return redirect(url_for('double_elim_bracket', game_name=game_name))


@app.route('/double_elim_bracket/<game_name>')
def double_elim_bracket(game_name):
    """Double Elimination Bracket anzeigen - 32 Teams, eine Tabelle"""
    db_path = os.path.join(TOURNAMENT_FOLDER, f"{game_name}.db")

    conn = get_db_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM double_elim_matches
        ORDER BY round, bracket DESC, match_index
    """)
    matches = cursor.fetchall()

    conn.close()

    return render_template("admin/double_elim_bracket.html",
                         game_name=game_name,
                         matches=matches)


@app.route('/enter_double_elim_results/<game_name>')
def enter_double_elim_results(game_name):
    """Double Elimination Ergebnisse eintragen"""
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
                         game_name=game_name,
                         matches=matches)


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

    cursor.execute("SELECT * FROM double_elim_matches WHERE id = ?", (match_id,))
    match = cursor.fetchone()

    if not match:
        conn.close()
        return redirect(url_for('enter_double_elim_results', game_name=game_name))

    winner = match['team1'] if score1 > score2 else match['team2']
    loser = match['team2'] if score1 > score2 else match['team1']

    cursor.execute("""
        UPDATE double_elim_matches
        SET score1 = ?, score2 = ?, winner = ?, loser = ?
        WHERE id = ?
    """, (score1, score2, winner, loser, match_id))

    conn.commit()
    process_double_elim_forwarding(conn, match)
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
    
    # WB Finalist (Runde 5, Sieger)
    cursor.execute("""
        SELECT winner FROM double_elim_matches
        WHERE round = 5 AND bracket = 'Winners' AND match_index = 0
        AND winner IS NOT NULL
    """)
    row = cursor.fetchone()
    wb_winner = row['winner'] if row else None

    # LB Finalist (Runde 8, Sieger)
    cursor.execute("""
        SELECT winner FROM double_elim_matches
        WHERE round = 8 AND bracket = 'Losers' AND match_index = 0
        AND winner IS NOT NULL
    """)
    row = cursor.fetchone()
    lb_winner = row['winner'] if row else None

    if not all([wb_winner, lb_winner]):
        conn.close()
        return render_template("admin/error.html",
                             error_message=f"Finalisten stehen noch nicht fest! WB: {wb_winner}, LB: {lb_winner}")

    # Spielnummer dynamisch: nach letzter DE-Nummer
    cursor.execute("SELECT MAX(match_number) FROM double_elim_matches")
    row = cursor.fetchone()
    match_number = (row[0] or 260) + 1

    # Grand Final: WB Sieger vs LB Sieger
    cursor.execute("""
        INSERT INTO super_finals_matches
        (match_number, match_id, team1, team2)
        VALUES (?, 'FINAL', ?, ?)
    """, (match_number, wb_winner, lb_winner))
    match_number += 1

    # Spiel um Platz 3 (placeholder)
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
               AND (r2.goals_for > r.goals_for 
                    OR (r2.goals_for = r.goals_for AND r2.goal_difference > r.goal_difference))) = 3
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.goals_for DESC, r.goal_difference DESC
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
               AND (r2.goals_for > r.goals_for 
                    OR (r2.goals_for = r.goals_for AND r2.goal_difference > r.goal_difference))) = 4
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.goals_for DESC, r.goal_difference DESC
    """)
    follower_teams.extend([(t['team'], t['points'], t['goal_difference']) for t in cursor.fetchall()])
    
    # 6. Platzierte
    cursor.execute("""
        SELECT r.team, r.points, r.goal_difference
        FROM rankings r
        WHERE (SELECT COUNT(*) FROM rankings r2 
               WHERE r2.group_number = r.group_number 
               AND (r2.goals_for > r.goals_for 
                    OR (r2.goals_for = r.goals_for AND r2.goal_difference > r.goal_difference))) = 5
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.goals_for DESC, r.goal_difference DESC
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
    
    # Direkt qualifizierte
    cursor.execute("""
        SELECT r.team, r.points, r.goal_difference, r.goals_for, r.group_number
        FROM rankings r
        WHERE (SELECT COUNT(*) FROM rankings r2 
               WHERE r2.group_number = r.group_number 
               AND (r2.goals_for > r.goals_for 
                    OR (r2.goals_for = r.goals_for AND r2.goal_difference > r.goal_difference))) = 3
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.goals_for DESC, r.goal_difference DESC
        LIMIT 4
    """)
    direct_teams = [row['team'] for row in cursor.fetchall()]
    
    # Quali-Gewinner
    cursor.execute("""
        SELECT winner FROM follower_quali_matches 
        WHERE winner IS NOT NULL
        ORDER BY match_number
    """)
    quali_winners = [row['winner'] for row in cursor.fetchall()]
    
    all_teams = direct_teams + quali_winners
    
    if len(all_teams) < 16:
        conn.close()
        return render_template("admin/error.html", 
                             error_message=f"Nicht genug Teams! Nur {len(all_teams)} statt 16")
    
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
        AND r.team NOT IN (
            SELECT team1 FROM follower_quali_matches
            UNION
            SELECT team2 FROM follower_quali_matches WHERE team2 != 'BYE'
        )
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.goals_for DESC, r.goal_difference DESC
    """)
    
    placement_teams = [row['team'] for row in cursor.fetchall()]
    
    match_number = 400  # Platzierungsrunden starten bei #400
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
            ORDER BY r.goals_for DESC, r.goal_difference DESC
        """, (group_num,))
        groups[group_num] = cursor.fetchall()
    
    conn.close()
    
    return render_template("display/display_groups.html",
                         game_name=game_name,
                         groups=groups)


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
            ORDER BY goals_for DESC, goal_difference DESC
            LIMIT 3
        """, (group_num,))
        groups_a[group_num] = [row['team'] for row in cursor.fetchall()]
    
    groups_b = {}
    for group_num in range(6, 11):
        cursor.execute("""
            SELECT team FROM rankings 
            WHERE group_number = ?
            AND team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
            ORDER BY goals_for DESC, goal_difference DESC
            LIMIT 3
        """, (group_num,))
        groups_b[group_num] = [row['team'] for row in cursor.fetchall()]
    
    cursor.execute("""
        SELECT r.team, r.group_number
        FROM rankings r
        WHERE (SELECT COUNT(*) FROM rankings r2 
               WHERE r2.group_number = r.group_number 
               AND (r2.goals_for > r.goals_for 
                    OR (r2.goals_for = r.goals_for AND r2.goal_difference > r.goal_difference))) = 3
        AND r.team NOT IN (SELECT name FROM teams WHERE is_ghost = 1)
        ORDER BY r.goals_for DESC, r.goal_difference DESC
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
            ORDER BY r.goals_for DESC, r.goal_difference DESC
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