import json
import urllib.request
from http.server import HTTPServer

from medallion.run.listener import _HealthHandler


def test_health_server_responds_200():
    server = HTTPServer(("127.0.0.1", 0), _HealthHandler)
    port = server.server_address[1]
    import threading

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz")
        assert resp.status == 200
        body = json.loads(resp.read())
        assert body == {"ok": True}
    finally:
        server.shutdown()
