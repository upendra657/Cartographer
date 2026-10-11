"""
Cartographer Documentation Server
Step 4.6: Serves static dbt documentation (lineage DAG, catalog, and schema metadata) locally on port 8080.
"""

import http.server
import socketserver
import os
import sys

PORT = 8080
TARGET_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "target")

def serve_docs():
    if not os.path.isdir(TARGET_DIR):
        print(f"[-] ERROR: Documentation directory '{TARGET_DIR}' not found. Run 'dbt docs generate' first.")
        sys.exit(1)

    required_files = ["index.html", "manifest.json", "catalog.json"]
    for f in required_files:
        f_path = os.path.join(TARGET_DIR, f)
        if not os.path.isfile(f_path):
            print(f"[-] ERROR: Missing required docs bundle file: '{f_path}'.")
            sys.exit(1)

    class CustomHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=TARGET_DIR, **kwargs)

        def log_message(self, format, *args):
            # Suppress noisy access logs unless error
            if "404" in args[1]:
                sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))

    print("=" * 70)
    print("CARTOGRAPHER DBT STATIC DOCUMENTATION SERVER (STEP 4.6)")
    print("=" * 70)
    print(f"[+] Serving dbt documentation from: {TARGET_DIR}")
    print(f"[+] Documentation URL:              http://localhost:{PORT}")
    print(f"[+] Direct Lineage DAG:             http://localhost:{PORT}/#!/overview")
    print("[+] Press Ctrl+C to stop the server.")
    print("=" * 70)

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), CustomHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n[+] Documentation server stopped cleanly.")
            httpd.server_close()

if __name__ == "__main__":
    serve_docs()
