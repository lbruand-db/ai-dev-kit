"""Asciicast v2 recorder for generating terminal session demo files.

Produces .cast files playable with `asciinema play` or embeddable
via the asciinema web player.
"""

from __future__ import annotations

import json
from pathlib import Path

# ANSI color codes
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
WHITE = "\033[37m"
BOLD_CYAN = "\033[1;36m"
BOLD_GREEN = "\033[1;32m"
BOLD_WHITE = "\033[1;37m"


class AsciicastRecorder:
    """Builds asciicast v2 recordings event by event."""

    def __init__(self, width: int = 90, height: int = 32, title: str = ""):
        self.width = width
        self.height = height
        self.title = title
        self._events: list[tuple[float, str, str]] = []
        self._time = 0.0

    # -- timing --

    def pause(self, seconds: float) -> AsciicastRecorder:
        self._time += seconds
        return self

    @property
    def duration(self) -> float:
        return self._time

    # -- primitives --

    def emit(self, text: str) -> AsciicastRecorder:
        self._events.append((self._time, "o", text))
        return self

    def newline(self) -> AsciicastRecorder:
        return self.emit("\r\n")

    def line(self, text: str = "", delay: float = 0.0) -> AsciicastRecorder:
        self.emit(text + "\r\n")
        self._time += delay
        return self

    # -- semantic helpers (mimic Claude Code UI) --

    def comment(self, text: str, delay: float = 0.05) -> AsciicastRecorder:
        return self.line(f"{DIM}  # {text}{RESET}", delay)

    def prompt(self, text: str, delay: float = 0.0) -> AsciicastRecorder:
        return self.line(f"{BOLD_CYAN}❯{RESET} {BOLD_WHITE}{text}{RESET}", delay)

    def prompt_cont(self, text: str, delay: float = 0.0) -> AsciicastRecorder:
        return self.line(f"  {BOLD_WHITE}{text}{RESET}", delay)

    def assistant_header(self, text: str, delay: float = 0.0) -> AsciicastRecorder:
        return self.line(f"{MAGENTA}⏺{RESET} {text}", delay)

    def yaml_line(self, text: str, delay: float = 0.05) -> AsciicastRecorder:
        return self.line(f"    {YELLOW}{text}{RESET}", delay)

    def success(self, text: str, delay: float = 0.0) -> AsciicastRecorder:
        return self.line(f"  {BOLD_GREEN}✓{RESET} {GREEN}{text}{RESET}", delay)

    # -- output --

    def write(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        header = {"version": 2, "width": self.width, "height": self.height}
        if self.title:
            header["title"] = self.title

        with open(path, "w") as f:
            f.write(json.dumps(header) + "\n")
            for ts, event_type, data in self._events:
                f.write(json.dumps([round(ts, 4), event_type, data]) + "\n")

        return path
