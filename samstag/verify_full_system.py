
import os
import re
import unittest
import shutil
import tempfile
from flask import url_for

# Import the application
try:
    from app import app, initialize_db, get_db_connection, TOURNAMENT_FOLDER
except ImportError:
    print("CRITICAL: Could not import app.py. Make sure you are in the correct directory.")
    exit(1)

class TestComprehensiveIntegrity(unittest.TestCase):
    
    def setUp(self):
        # Setup Test Environment
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'consistency_check.db')
        self.game_name = 'consistency_check'
        
        # Patch app
        app.config['TESTING'] = True
        app.config['WTF_CSRF_ENABLED'] = False
        app.config['SERVER_NAME'] = 'localhost'
        
        # Overwrite global if possible or just rely on path mocking if app allows
        # Since app.py likely uses the global TOURNAMENT_FOLDER, we need to try to patch it
        import app as app_module
        self.original_folder = app_module.TOURNAMENT_FOLDER
        app_module.TOURNAMENT_FOLDER = self.test_dir
        
        # Initialize DB
        initialize_db(self.db_path)
        self.conn = get_db_connection(self.db_path)
        
        self.client = app.test_client()
        self.ctx = app.test_request_context()
        self.ctx.push()

    def tearDown(self):
        self.ctx.pop()
        self.conn.close()
        # Restore logic
        import app as app_module
        app_module.TOURNAMENT_FOLDER = self.original_folder
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_01_static_template_analysis(self):
        """
        Scans all .html files in templates/ folder.
        Finds all {{ url_for('endpoint', ...) }}.
        Verifies that 'endpoint' exists in app.view_functions.
        """
        print("\n[1] STATIC TEMPLATE ANALYSIS (Checking for broken links)...")
        
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        self.assertTrue(os.path.exists(template_dir), "Templates directory not found!")
        
        # Get all registered endpoints
        registered_endpoints = list(app.view_functions.keys())
        # Add static
        registered_endpoints.append('static')
        
        broken_links = []
        
        for root, dirs, files in os.walk(template_dir):
            for file in files:
                if file.endswith('.html'):
                    path = os.path.join(root, file)
                    rel_path = os.path.relpath(path, template_dir)
                    
                    with open(path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    # Regex to find url_for('endpoint'...)
                    # Matches: url_for('foo') or url_for("foo")
                    matches = re.findall(r"url_for\s*\(\s*['\"]([^'\"]+)['\"]", content)
                    
                    for endpoint in matches:
                        if endpoint not in registered_endpoints:
                            broken_links.append(f"File: {rel_path} -> Invalid Endpoint: '{endpoint}'")
                            
        if broken_links:
            print("!!! FOUND BROKEN LINKS IN TEMPLATES !!!")
            for link in broken_links:
                print(f"  - {link}")
            self.fail(f"Found {len(broken_links)} broken links in templates.")
        else:
            print("    -> All url_for() calls in templates point to valid routes.")

    def test_02_smoke_test_routes(self):
        """
        Simulates a user flow and tries to GET every page.
        """
        print("\n[2] DYNAMIC SMOKE TEST (Rendering all pages)...")
        
        # 1. Create Game (Already effectively done by setup but let's ensure DB entry)
        with self.client as c:
            # Manually insert config to match app behavior
            c.post('/create_new_game', data={
                'game_name': self.game_name,
                'match_duration': 10,
                'break_between_games': 2,
                'break_between_rounds': 5,
                'start_time': '09:00'
            }, follow_redirects=True)
            
            # Helper to check a route
            def check_route(endpoint, **kwargs):
                try:
                    url = url_for(endpoint, **kwargs)
                    response = c.get(url, follow_redirects=True)
                    if response.status_code != 200:
                        print(f"    FAIL: {url} returned {response.status_code}")
                        self.fail(f"Route {url} failed with {response.status_code}")
                    else:
                        # print(f"    OK: {url}")
                        pass
                except Exception as e:
                    print(f"    CRASH: {endpoint} -> {e}")
                    raise e

            print("    - Checking Admin Routes...")
            check_route('game_overview', game_name=self.game_name)
            check_route('manage_teams', game_name=self.game_name)
            check_route('tournament_config', game_name=self.game_name)
            
            # Add Teams for further steps
            for i in range(1, 13): # 12 Teams = 2 Groups (min for logic)
                # We do 60 for full robustness like user wanted
                pass
            
            # Let's populate DB directly for speed, reusing logic from other test
            conn = get_db_connection(self.db_path)
            for i in range(1, 61):
                grp = (i - 1) // 6 + 1
                conn.execute("INSERT INTO teams (name, group_number) VALUES (?, ?)", (f"Team{i}", grp))
                conn.execute("INSERT INTO rankings (team, group_number) VALUES (?, ?)", (f"Team{i}", grp))
            conn.commit()
            conn.close()
            
            # Generate Matches
            print("    - Generating Round Robin...")
            c.get(url_for('generate_matches', game_name=self.game_name))
            
            print("    - Checking Round Robin Routes...")
            check_route('match_overview', game_name=self.game_name)
            check_route('enter_results', game_name=self.game_name)
            check_route('group_standings', game_name=self.game_name)
            
            # Simulate Results
            conn = get_db_connection(self.db_path)
            conn.execute("UPDATE matches SET score1=10, score2=5")
            conn.commit()
            
            # Generate Double Elim
            print("    - Generating Double Elimination...")
            # We need to call the recalculate internal first usually done by result entry
            from app import recalculate_rankings_internal
            recalculate_rankings_internal(get_db_connection(self.db_path))
            
            c.get(url_for('generate_double_elim', game_name=self.game_name))
            
            print("    - Checking Double Elim Routes...")
            check_route('double_elim_bracket', game_name=self.game_name)
            check_route('enter_double_elim_results', game_name=self.game_name)
            
            # Checks for Follower Cup Routes
            print("    - Checking Follower Cup Routes...")
            # Ideally generate it first
            c.get(url_for('generate_follower_quali', game_name=self.game_name))
            check_route('follower_quali_overview', game_name=self.game_name)
            check_route('enter_follower_quali_results', game_name=self.game_name)
            
            c.get(url_for('generate_follower_cup', game_name=self.game_name))
            check_route('follower_cup_overview', game_name=self.game_name)
            check_route('enter_follower_cup_results', game_name=self.game_name)
            
            # Checks for Placement Routes
            print("    - Checking Placement Routes...")
            c.get(url_for('generate_placement_round', game_name=self.game_name))
            check_route('placement_round_overview', game_name=self.game_name)
            check_route('enter_placement_results', game_name=self.game_name)
            
            # Checks for Display Routes
            print("    - Checking Display Routes...")
            check_route('display_home', game_name=self.game_name)
            check_route('display_groups', game_name=self.game_name)
            check_route('display_brackets', game_name=self.game_name)
            check_route('display_super_finals', game_name=self.game_name)
            check_route('display_follower_cup', game_name=self.game_name)
            
            # Final Rankings
            check_route('final_rankings', game_name=self.game_name)
            
            print("    -> All main routes rendered successfully (HTTP 200).")

if __name__ == '__main__':
    unittest.main()
