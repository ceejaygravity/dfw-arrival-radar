from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import partial
from html import unescape
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
TIMEZONE_NAME = "America/Chicago"
SOURCE_BASE = "https://www.airport-dallas.com"
BOARD_URL = f"{SOURCE_BASE}/dfw-arrivals"
TIME_PERIODS = (0, 6, 12, 18)
CACHE_TTL_SECONDS = 300
MAX_BOARD_WORKERS = 4
MAX_DETAIL_WORKERS = 16
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)


FIELD_BLOCK_RE = re.compile(
    r"""
    <div\s+class="flight-info__infobox-title">\s*
        (?P<label>[^:<]+):
    \s*</div>\s*
    <div\s+class="flight-info__infobox-text[^"]*">\s*
        (?P<value>.*?)
    \s*</div>
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)
SCHEDULED_ARRIVAL_RE = re.compile(
    r"Scheduled Arrival Time:\s*(?P<value>[^<]+)<",
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")
TEXT_RE = re.compile(r">([^<>]+)<")
TIME_RE = re.compile(
    r"(?P<clock>\d{1,2}:\d{2}\s*[ap]m)(?:\s*\((?P<date>\d{4}-\d{2}-\d{2})\))?",
    re.IGNORECASE,
)

_cache_lock = threading.Lock()
_cache = {"timestamp": 0.0, "payload": None}


def clean_text(value: str) -> str:
    if not value:
        return ""
    text = TAG_RE.sub(" ", value)
    text = unescape(text).replace("\xa0", " ")
    return " ".join(text.split())


def normalize_field(value: str | None) -> str | None:
    text = clean_text(value or "")
    if not text or text in {"-", "N/A"}:
        return None
    return text


def current_local_time() -> datetime:
    if ZoneInfo is None:
        return datetime.now()
    return datetime.now(ZoneInfo(TIMEZONE_NAME))


def fetch_html(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(request, timeout=20) as response:
        return response.read().decode("utf-8", errors="ignore")


def parse_codeshares(block: str) -> list[str]:
    values = []
    for raw in TEXT_RE.findall(block or ""):
        text = clean_text(raw)
        if text:
            values.append(text)
    return values


def parse_primary_airline(block: str) -> str:
    values = parse_codeshares(block)
    return values[0] if values else "Unknown"


def parse_board_page(time_period: int) -> list[dict]:
    html = fetch_html(f"{BOARD_URL}?tp={time_period}")
    flights = []

    start = html.find('<div class="flights-info">')
    end = html.find('<div class="flights-avis2">', start)
    container = html[start:end] if start != -1 and end != -1 else html
    rows = container.split('<div class="flight-row">')

    for row in rows:
        if "flight-col__dest-term" not in row or "flight-col__hour" not in row:
            continue

        origin_match = re.search(
            r'flight-col__dest-term"><b>(?P<origin>.*?)</b>\s*<span>\((?P<code>.*?)\)</span>',
            row,
            re.DOTALL,
        )
        schedule_match = re.search(
            r'flight-col\s+flight-col__hour">(?P<time>.*?)</div>',
            row,
            re.DOTALL,
        )
        detail_match = re.search(
            r'href="(?P<detail>/dfw-flight-status\?arrival=(?P<flight>[^"]+))"',
            row,
            re.DOTALL,
        )
        flight_block_match = re.search(
            r'flight-col__flight">(?P<block>.*?)</div>\s*<div\s+class="flight-col\s+flight-col__airline">',
            row,
            re.DOTALL,
        )
        airline_block_match = re.search(
            r'flight-col__airline">(?P<block>.*?)</div>\s*</div>\s*<div\s+class="flight-col\s+flight-col__terminal">',
            row,
            re.DOTALL,
        )
        terminal_match = re.search(
            r'flight-col\s+flight-col__terminal">(?P<terminal>[^<]+)</div>',
            row,
            re.DOTALL,
        )
        status_match = re.search(
            r'flight-col__status[^"]*">\s*<a[^>]*>(?P<status>[^<]+)</a>',
            row,
            re.DOTALL,
        )

        if not origin_match or not schedule_match or not detail_match:
            continue

        origin = clean_text(origin_match.group("origin"))
        origin_code = clean_text(origin_match.group("code"))
        scheduled_arrival = clean_text(schedule_match.group("time"))
        flight_number = clean_text(detail_match.group("flight"))
        detail_path = clean_text(detail_match.group("detail"))
        terminal = normalize_field(terminal_match.group("terminal") if terminal_match else None)
        status = clean_text(status_match.group("status") if status_match else "")
        codeshares = parse_codeshares(flight_block_match.group("block") if flight_block_match else "")
        if codeshares and codeshares[0] == flight_number:
            codeshares = codeshares[1:]
        airline = parse_primary_airline(airline_block_match.group("block") if airline_block_match else "")

        flights.append(
            {
                "flightNumber": flight_number,
                "codeshares": codeshares,
                "airline": airline,
                "origin": origin,
                "originCode": origin_code,
                "terminal": terminal,
                "scheduledArrival": scheduled_arrival,
                "status": status,
                "detailPath": detail_path,
                "detailUrl": urljoin(SOURCE_BASE, detail_path),
                "boardPeriodStartHour": time_period,
            }
        )

    return flights


def extract_arrival_segment(html: str) -> str:
    anchor = html.find("Estimated Arrival Time:")
    if anchor == -1:
        anchor = html.find("Scheduled Arrival Time:")
    if anchor == -1:
        return html

    start = max(0, anchor - 600)
    end = min(len(html), anchor + 2000)
    return html[start:end]


def parse_detail_page(detail_url: str) -> dict:
    html = fetch_html(detail_url)
    segment = extract_arrival_segment(html)

    fields = {}
    for match in FIELD_BLOCK_RE.finditer(segment):
        label = clean_text(match.group("label")).lower()
        value = clean_text(match.group("value"))
        if label and value:
            fields[label] = value

    scheduled_match = SCHEDULED_ARRIVAL_RE.search(segment)
    scheduled_arrival = clean_text(scheduled_match.group("value")) if scheduled_match else ""

    return {
        "estimatedArrival": fields.get("estimated arrival time"),
        "terminal": normalize_field(fields.get("terminal")),
        "gate": normalize_field(fields.get("gate")),
        "scheduledArrival": scheduled_arrival,
    }


def parse_clock_time(value: str) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None

    match = TIME_RE.search(text)
    if not match:
        return None

    try:
        moment = datetime.strptime(match.group("clock").upper(), "%I:%M %p").time()
    except ValueError:
        return None

    local_now = current_local_time()
    date_text = match.group("date")
    if date_text:
        try:
            target_date = datetime.strptime(date_text, "%Y-%m-%d").date()
        except ValueError:
            target_date = local_now.date()
    else:
        target_date = local_now.date()

    return datetime.combine(target_date, moment, tzinfo=local_now.tzinfo)


def enrich_flight(flight: dict) -> dict:
    try:
        detail = parse_detail_page(flight["detailUrl"])
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        flight["detailError"] = str(exc)
        detail = {}

    if detail.get("terminal"):
        flight["terminal"] = detail["terminal"]
    if detail.get("scheduledArrival"):
        flight["scheduledArrival"] = detail["scheduledArrival"]
    if detail.get("estimatedArrival"):
        flight["estimatedArrival"] = detail["estimatedArrival"]
    else:
        flight["estimatedArrival"] = None
    flight["terminal"] = normalize_field(flight.get("terminal"))
    flight["gate"] = detail.get("gate") or None

    scheduled_dt = parse_clock_time(flight.get("scheduledArrival", ""))
    estimated_dt = parse_clock_time(flight.get("estimatedArrival", ""))

    flight["scheduledArrivalIso"] = scheduled_dt.isoformat() if scheduled_dt else None
    flight["estimatedArrivalIso"] = estimated_dt.isoformat() if estimated_dt else None

    delta_minutes = None
    if scheduled_dt and estimated_dt:
        delta_minutes = int((estimated_dt - scheduled_dt).total_seconds() // 60)

    status_text = flight.get("status", "").lower()
    is_early = False
    minutes_early = 0

    if delta_minutes is not None and delta_minutes < 0:
        is_early = True
        minutes_early = abs(delta_minutes)
    elif "early" in status_text:
        is_early = True

    flight["deltaMinutes"] = delta_minutes
    flight["isEarly"] = is_early
    flight["minutesEarly"] = minutes_early
    flight["gateLabel"] = flight["gate"] or "TBD"
    return flight


def sort_flights(flights: list[dict]) -> list[dict]:
    def sort_key(flight: dict) -> tuple:
        terminal = flight.get("terminal") or "Z"
        gate = flight.get("gate") or "ZZZ"
        scheduled = flight.get("scheduledArrivalIso") or "9999"
        return terminal, gate, scheduled, flight.get("flightNumber") or ""

    return sorted(flights, key=sort_key)


def build_terminal_groups(flights: list[dict]) -> list[dict]:
    terminals: dict[str, dict] = {}

    for flight in flights:
        terminal_name = flight.get("terminal") or "Unassigned"
        gate_name = flight.get("gate") or "TBD"

        terminal = terminals.setdefault(
            terminal_name,
            {
                "terminal": terminal_name,
                "totalFlights": 0,
                "earlyFlights": 0,
                "gateCount": 0,
                "gates": {},
            },
        )
        terminal["totalFlights"] += 1
        if flight.get("isEarly"):
            terminal["earlyFlights"] += 1

        gate = terminal["gates"].setdefault(
            gate_name,
            {
                "gate": gate_name,
                "totalFlights": 0,
                "earlyFlights": 0,
                "flights": [],
            },
        )
        gate["totalFlights"] += 1
        if flight.get("isEarly"):
            gate["earlyFlights"] += 1
        gate["flights"].append(flight)

    payload = []
    sorted_terminal_names = sorted(
        terminals,
        key=lambda name: (name == "Unassigned", name),
    )

    for terminal_name in sorted_terminal_names:
        terminal = terminals[terminal_name]
        gate_rows = []
        for gate_name in sorted(terminal["gates"]):
            gate = terminal["gates"][gate_name]
            gate["flights"] = sort_flights(gate["flights"])
            gate_rows.append(gate)

        gate_rows.sort(
            key=lambda gate: (
                -gate["earlyFlights"],
                gate["gate"] == "TBD",
                gate["gate"],
            )
        )
        terminal["gates"] = gate_rows
        terminal["gateCount"] = len(gate_rows)
        payload.append(terminal)

    return payload


def summarize(terminals: list[dict], flights: list[dict]) -> dict:
    total_flights = len(flights)
    early_flights = sum(1 for flight in flights if flight.get("isEarly"))
    total_gates = sum(terminal["gateCount"] for terminal in terminals)
    busiest_terminal = max(
        terminals,
        key=lambda terminal: terminal["totalFlights"],
        default=None,
    )

    return {
        "totalFlights": total_flights,
        "earlyFlights": early_flights,
        "trackedGates": total_gates,
        "terminalCount": len(terminals),
        "busiestTerminal": busiest_terminal["terminal"] if busiest_terminal else None,
        "busiestTerminalFlights": busiest_terminal["totalFlights"] if busiest_terminal else 0,
    }


def scrape_arrivals() -> dict:
    board_flights = {}
    errors = []

    with ThreadPoolExecutor(max_workers=MAX_BOARD_WORKERS) as pool:
        future_map = {
            pool.submit(parse_board_page, time_period): time_period for time_period in TIME_PERIODS
        }
        for future in as_completed(future_map):
            time_period = future_map[future]
            try:
                flights = future.result()
            except Exception as exc:  # pragma: no cover
                errors.append(f"Board tp={time_period}: {exc}")
                continue

            for flight in flights:
                board_flights[flight["flightNumber"]] = flight

    flights = list(board_flights.values())

    with ThreadPoolExecutor(max_workers=MAX_DETAIL_WORKERS) as pool:
        future_map = {
            pool.submit(enrich_flight, flight): flight["flightNumber"] for flight in flights
        }
        enriched = []
        for future in as_completed(future_map):
            flight_number = future_map[future]
            try:
                enriched.append(future.result())
            except Exception as exc:  # pragma: no cover
                errors.append(f"Detail {flight_number}: {exc}")

    flights = sort_flights(enriched)
    terminals = build_terminal_groups(flights)

    payload = {
        "airport": "DFW",
        "airportName": "Dallas Fort Worth International Airport",
        "date": current_local_time().date().isoformat(),
        "timeZone": TIMEZONE_NAME,
        "fetchedAt": current_local_time().isoformat(),
        "cacheTtlSeconds": CACHE_TTL_SECONDS,
        "summary": summarize(terminals, flights),
        "terminals": terminals,
        "flights": flights,
        "source": {
            "name": "airport-dallas.com",
            "boardUrl": BOARD_URL,
            "detailBaseUrl": f"{SOURCE_BASE}/dfw-flight-status",
        },
        "errors": errors,
    }
    return payload


def get_arrivals(force_refresh: bool = False) -> dict:
    with _cache_lock:
        cached = _cache["payload"]
        is_fresh = time.time() - _cache["timestamp"] < CACHE_TTL_SECONDS
        if cached and is_fresh and not force_refresh:
            return cached

    payload = scrape_arrivals()

    with _cache_lock:
        _cache["payload"] = payload
        _cache["timestamp"] = time.time()

    return payload


class AppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self.handle_healthcheck()
            return
        if parsed.path == "/api/arrivals":
            self.handle_arrivals_api(parsed.query)
            return
        if parsed.path == "/":
            self.path = "/index.html"
        else:
            self.path = parsed.path
        return super().do_GET()

    def handle_healthcheck(self) -> None:
        body = json.dumps({"ok": True}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def handle_arrivals_api(self, query: str) -> None:
        params = parse_qs(query)
        force_refresh = params.get("refresh", ["0"])[0] == "1"
        try:
            payload = get_arrivals(force_refresh=force_refresh)
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
        except Exception as exc:
            body = json.dumps({"error": str(exc)}).encode("utf-8")
            self.send_response(502)

        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def run_server() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer((host, port), partial(AppHandler))
    print(f"Serving DFW arrival radar at http://{host}:{port}")
    server.serve_forever()


def run_once() -> None:
    payload = get_arrivals(force_refresh=True)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    if "--once" in sys.argv:
        run_once()
    else:
        run_server()
