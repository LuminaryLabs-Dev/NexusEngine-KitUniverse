from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from .pipeline import REPO_ROOT, default_config, merge_config
from .workspace import Workspace, code_epoch, read_jsonl, write_json_atomic


STATIC_ROOT = Path(__file__).resolve().parent / "static"


class WorkerSupervisor:
    def __init__(self, workspace_root: Path) -> None:
        self.workspace = Workspace(workspace_root)
        if not self.workspace.config_path.exists():
            write_json_atomic(self.workspace.config_path, default_config())
        self.process: Optional[subprocess.Popen[str]] = None
        self.worker_code_hash: Optional[str] = None
        self.pending_restart = False
        self.lock = threading.Lock()
        self.closed = False
        self.monitor = threading.Thread(target=self._monitor, daemon=True)
        self.monitor.start()

    def start(self, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self.lock:
            if self.process and self.process.poll() is None:
                return {"ok": True, "status": "already-running", "pid": self.process.pid}
            config = merge_config({**self.workspace.config(), **(overrides or {})})
            write_json_atomic(self.workspace.config_path, config)
            self.workspace.set_control("run")
            self._spawn()
            return {"ok": True, "status": "started", "pid": self.process.pid if self.process else None}

    def action(self, action: str) -> Dict[str, Any]:
        if action not in {"pause", "resume", "drain", "stop"}:
            return {"ok": False, "error": f"unsupported action: {action}"}
        self.workspace.set_control("run" if action == "resume" else action)
        if action == "resume" and (not self.process or self.process.poll() is not None):
            return self.start()
        return {"ok": True, "status": action}

    def status(self) -> Dict[str, Any]:
        process_running = bool(self.process and self.process.poll() is None)
        state = self.workspace.state()
        config = self.workspace.config()
        return {
            **state,
            "workspace": str(self.workspace.root),
            "process_running": process_running,
            "pid": self.process.pid if process_running and self.process else None,
            "pending_code_reload": self.pending_restart,
            "free_gib": round(self.workspace.free_gib(), 2),
            "config": config,
        }

    def events(self) -> list[Dict[str, Any]]:
        return read_jsonl(self.workspace.events_path)[-100:]

    def close(self) -> None:
        self.closed = True
        with self.lock:
            if not self.process or self.process.poll() is not None:
                return
            self.workspace.set_control("stop")
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()

    def _spawn(self) -> None:
        epoch = code_epoch(REPO_ROOT)
        self.worker_code_hash = epoch["code_hash"]
        command = [
            sys.executable,
            "-m",
            "workflow_harnesses.rawg_capability_pipeline.worker",
            "--workspace",
            str(self.workspace.root),
        ]
        self.process = subprocess.Popen(command, cwd=REPO_ROOT, text=True)
        self.workspace.event("worker-started", {"pid": self.process.pid, **epoch})

    def _monitor(self) -> None:
        while not self.closed:
            time.sleep(1.0)
            with self.lock:
                if not self.process:
                    continue
                running = self.process.poll() is None
                if running and self.worker_code_hash:
                    current_hash = code_epoch(REPO_ROOT)["code_hash"]
                    if current_hash != self.worker_code_hash and not self.pending_restart:
                        self.pending_restart = True
                        self.workspace.set_control("drain")
                        self.workspace.event("code-change-detected", {"previous": self.worker_code_hash, "current": current_hash})
                if not running and self.pending_restart:
                    self.pending_restart = False
                    self.workspace.set_control("run")
                    self._spawn()


class OperatorHandler(BaseHTTPRequestHandler):
    supervisor: WorkerSupervisor

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/api/status":
            self._json(self.supervisor.status())
            return
        if path == "/api/events":
            self._json({"events": self.supervisor.events()})
            return
        if path in {"/", "/index.html"}:
            self._file(STATIC_ROOT / "index.html", "text/html; charset=utf-8")
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        action = path.removeprefix("/api/")
        length = int(self.headers.get("Content-Length") or 0)
        payload = {}
        if length:
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except json.JSONDecodeError:
                self._json({"ok": False, "error": "invalid JSON body"}, HTTPStatus.BAD_REQUEST)
                return
        result = self.supervisor.start(payload.get("config")) if action == "start" else self.supervisor.action(action)
        self._json(result, HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json(self, value: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(value, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path: Path, content_type: str) -> None:
        if not path.exists():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def serve(workspace: Path, host: str = "127.0.0.1", port: int = 8765, open_browser: bool = False) -> None:
    supervisor = WorkerSupervisor(workspace)
    handler = type("BoundOperatorHandler", (OperatorHandler,), {"supervisor": supervisor})
    server = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{port}"
    print(json.dumps({"ok": True, "url": url, "workspace": str(workspace.resolve())}, indent=2))
    if open_browser:
        import webbrowser

        webbrowser.open(url)
    try:
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
    finally:
        supervisor.close()
        server.server_close()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="kituniverse serve")
    parser.add_argument("--workspace", type=Path, default=Path("runs/rawg-881k/default"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--open", action="store_true")
    args = parser.parse_args(argv)
    serve(args.workspace, args.host, args.port, args.open)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
