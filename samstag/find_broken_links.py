import re
import os
from app import app

# Get all registered endpoints
registered_endpoints = list(app.view_functions.keys())
registered_endpoints.append('static')

broken_links = []
template_dir = 'templates'

for root, dirs, files in os.walk(template_dir):
    for file in files:
        if file.endswith('.html'):
            path = os.path.join(root, file)
            rel_path = os.path.relpath(path, template_dir)
            
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            matches = re.findall(r"url_for\s*\(\s*['\"]([^'\"]+)['\"]", content)
            
            for endpoint in matches:
                if endpoint not in registered_endpoints:
                    broken_links.append(f"{rel_path} -> {endpoint}")

print("DEFEKTE LINKS:")
for link in broken_links:
    print(f"  - {link}")
print(f"\nGESAMT: {len(broken_links)} defekte Links")
