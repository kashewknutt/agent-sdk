"""Append-only JSONL run event streams."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from agent_sdk.models import RunEvent


class RunEventLogger:
    """Writes run events to data/runs/<run_id>.jsonl."""

    def __init__(self, runs_dir: Path) -> None:
        self.runs_dir = Path(runs_dir)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self._path: Path | None = None
        self._run_id: str | None = None
        self._memory: list[RunEvent] = []

    @property
    def run_id(self) -> str | None:
        return self._run_id

    def start(self, run_id: str) -> Path:
        self._run_id = run_id
        self._path = self.runs_dir / f"{run_id}.jsonl"
        self._memory = []
        self._path.touch(exist_ok=True)
        return self._path

    def ensure_active(self, *, prefer_latest: bool = True) -> str:
        """Attach to an existing run log, or create an ops session.

        Standalone actions (Execute approved, Resume engage) call set_step
        outside a research run. Without this, emit() is a no-op and the
        Fleet live trail never shows engage progress.
        """
        if self._run_id and self._path:
            return self._run_id
        if prefer_latest:
            files = sorted(self.runs_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime)
            if files:
                rid = files[-1].stem
                # Re-open for append; do not truncate the existing trail.
                self._run_id = rid
                self._path = files[-1]
                return rid
        rid = datetime.now().strftime("%Y%m%dT%H%M%S") + "-ops-" + uuid.uuid4().hex[:6]
        self.start(rid)
        return rid

    def emit(
        self,
        message: str,
        *,
        level: str = "info",
        step: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> RunEvent:
        if not self._run_id or not self._path:
            self.ensure_active()
        assert self._run_id and self._path
        event = RunEvent(
            run_id=self._run_id,
            level=level,
            step=step,
            message=message,
            data=data or {},
        )
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(event.model_dump_json() + "\n")
        self._memory.append(event)
        return event

    def tail(self, n: int = 200) -> list[dict[str, Any]]:
        if self._path and self._path.exists():
            lines = self._path.read_text(encoding="utf-8").splitlines()
            return [json.loads(line) for line in lines[-n:] if line.strip()]
        return [e.model_dump() for e in self._memory[-n:]]

    def latest_run_tail(self, n: int = 200) -> list[dict[str, Any]]:
        files = sorted(self.runs_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime)
        if not files:
            return self.tail(n)
        lines = files[-1].read_text(encoding="utf-8").splitlines()
        return [json.loads(line) for line in lines[-n:] if line.strip()]
