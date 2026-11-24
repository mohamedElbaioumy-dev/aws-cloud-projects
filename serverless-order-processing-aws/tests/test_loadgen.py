import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import loadgen


class Fake(BaseHTTPRequestHandler):
    seen = []

    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        Fake.seen.append((self.headers["x-api-key"], self.headers["Idempotency-Key"]))
        self.send_response(202)
        self.end_headers()
        self.wfile.write(json.dumps({"orderId": "ord_" + self.headers["Idempotency-Key"]}).encode())

    def log_message(self, *a):
        pass


def test_loadgen_sends_keyed_requests_with_duplicates(tmp_path, capsys):
    srv = HTTPServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    out = tmp_path / "run.jsonl"
    loadgen.main(["--url", "http://127.0.0.1:%d/orders" % srv.server_port, "--api-key", "k", "--rps", "10",
                  "--duration", "2", "--duplicate-rate", "0.5", "--out", str(out)])
    srv.shutdown()
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    assert len(rows) == 20 and {r["status"] for r in rows} == {202}
    assert len({r["key"] for r in rows}) < 20  # some keys were re-sent on purpose
    assert all(k == "k" for k, _ in Fake.seen)
    assert "distinct orderIds accepted" in capsys.readouterr().out


def test_loadgen_refuses_runaway_rate():
    import pytest

    with pytest.raises(SystemExit):
        loadgen.main(["--url", "http://x", "--api-key", "k", "--rps", "5000"])
