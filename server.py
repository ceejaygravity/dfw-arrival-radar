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
ARRIVALS_BOARD_URL = f"{SOURCE_BASE}/dfw-arrivals"
DEPARTURES_BOARD_URL = f"{SOURCE_BASE}/dfw-departures"
TIME_PERIODS = (0, 6, 12, 18)
CACHE_TTL_SECONDS = 300
MAX_BOARD_WORKERS = 4
MAX_DETAIL_WORKERS = 16
QUICK_TURN_MAX_MINUTES = 60
LONG_TURN_MIN_MINUTES = 180
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
SCHEDULED_DEPARTURE_RE = re.compile(
    r"Scheduled Departure Time:\s*(?P<value>[^<]+)<",
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


def parse_board_page(direction: str, time_period: int) -> list[dict]:
    board_url = ARRIVALS_BOARD_URL if direction == "arrival" else DEPARTURES_BOARD_URL
    detail_param = "arrival" if direction == "arrival" else "departure"
    html = fetch_html(f"{board_url}?tp={time_period}")
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
            rf'href="(?P<detail>/dfw-flight-status\?{detail_param}=(?P<flight>[^"]+))"',
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
                "direction": direction,
                "directionLabel": "Arrival" if direction == "arrival" else "Departure",
                "flightNumber": flight_number,
                "codeshares": codeshares,
                "airline": airline,
                "city": origin,
                "cityCode": origin_code,
                "terminal": terminal,
                "scheduledTime": scheduled_arrival,
                "status": status,
                "detailPath": detail_path,
                "detailUrl": urljoin(SOURCE_BASE, detail_path),
                "boardPeriodStartHour": time_period,
                "gate": None,
                "gateLabel": "TBD",
            }
        )

    return flights


def extract_detail_segment(html: str, direction: str) -> str:
    if direction == "arrival":
        anchors = ("Estimated Arrival Time:", "Arrived at:", "Scheduled Arrival Time:")
        before = 800
        after = 2200
    else:
        anchors = ("Estimated Departure Time:", "Departed at:", "Scheduled Departure Time:")
        before = 1200
        after = 1600

    anchor = -1
    for candidate in anchors:
        anchor = html.find(candidate)
        if anchor != -1:
            break
    if anchor == -1:
        return html

    start = max(0, anchor - before)
    end = min(len(html), anchor + after)
    return html[start:end]


def pick_first_field(fields: dict[str, str], *names: str) -> str | None:
    for name in names:
        value = normalize_field(fields.get(name))
        if value:
            return value
    return None


def parse_detail_page(detail_url: str, direction: str) -> dict:
    html = fetch_html(detail_url)
    segment = extract_detail_segment(html, direction)

    fields = {}
    for match in FIELD_BLOCK_RE.finditer(segment):
        label = clean_text(match.group("label")).lower()
        value = clean_text(match.group("value"))
        if label and value:
            fields[label] = value

    scheduled_regex = SCHEDULED_ARRIVAL_RE if direction == "arrival" else SCHEDULED_DEPARTURE_RE
    scheduled_match = scheduled_regex.search(segment)
    scheduled_arrival = clean_text(scheduled_match.group("value")) if scheduled_match else ""
    if direction == "arrival":
        event_time = pick_first_field(fields, "estimated arrival time", "arrived at")
    else:
        event_time = pick_first_field(fields, "estimated departure time", "departed at")

    return {
        "eventTime": event_time,
        "terminal": normalize_field(fields.get("terminal")),
        "gate": normalize_field(fields.get("gate")),
        "scheduledTime": scheduled_arrival,
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
        detail = parse_detail_page(flight["detailUrl"], flight["direction"])
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        flight["detailError"] = str(exc)
        detail = {}

    if detail.get("terminal"):
        flight["terminal"] = detail["terminal"]
    if detail.get("scheduledTime"):
        flight["scheduledTime"] = detail["scheduledTime"]
    flight["eventTime"] = detail.get("eventTime") or None
    flight["terminal"] = normalize_field(flight.get("terminal"))
    flight["gate"] = detail.get("gate") or None

    scheduled_dt = parse_clock_time(flight.get("scheduledTime", ""))
    event_dt = parse_clock_time(flight.get("eventTime", ""))

    flight["scheduledTimeIso"] = scheduled_dt.isoformat() if scheduled_dt else None
    flight["eventTimeIso"] = event_dt.isoformat() if event_dt else None

    delta_minutes = None
    if flight["direction"] == "arrival" and scheduled_dt and event_dt:
        delta_minutes = int((event_dt - scheduled_dt).total_seconds() // 60)

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
    flight["turnReferenceTimeIso"] = flight["eventTimeIso"] or flight["scheduledTimeIso"]
    flight["turnReferenceTime"] = flight["eventTime"] or flight["scheduledTime"]
    if flight["direction"] == "arrival":
        flight["origin"] = flight["city"]
        flight["originCode"] = flight["cityCode"]
        flight["scheduledArrival"] = flight["scheduledTime"]
        flight["estimatedArrival"] = flight["eventTime"]
    else:
        # Keep legacy fields populated so an older cached frontend bundle can
        # still render without crashing while the new assets propagate.
        flight["origin"] = flight["city"]
        flight["originCode"] = flight["cityCode"]
        flight["scheduledArrival"] = flight["scheduledTime"]
        flight["estimatedArrival"] = flight["eventTime"]
        flight["destination"] = flight["city"]
        flight["destinationCode"] = flight["cityCode"]
        flight["scheduledDeparture"] = flight["scheduledTime"]
        flight["estimatedDeparture"] = flight["eventTime"]
    return flight


def sort_flights(flights: list[dict]) -> list[dict]:
    def sort_key(flight: dict) -> tuple:
        terminal = flight.get("terminal") or "Z"
        gate = flight.get("gate") or "ZZZ"
        scheduled = flight.get("scheduledTimeIso") or "9999"
        return terminal, gate, scheduled, flight.get("flightNumber") or ""

    return sorted(flights, key=sort_key)


def classify_turn_window(minutes: int) -> str:
    if minutes <= QUICK_TURN_MAX_MINUTES:
        return "quick"
    if minutes >= LONG_TURN_MIN_MINUTES:
        return "long"
    return "standard"


def build_turn_windows(arrivals: list[dict], departures: list[dict]) -> list[dict]:
    sorted_arrivals = sorted(
        [flight for flight in arrivals if flight.get("turnReferenceTimeIso")],
        key=lambda flight: flight["turnReferenceTimeIso"],
    )
    sorted_departures = sorted(
        [flight for flight in departures if flight.get("turnReferenceTimeIso")],
        key=lambda flight: flight["turnReferenceTimeIso"],
    )

    windows = []
    departure_index = 0

    for arrival in sorted_arrivals:
        arrival_dt = parse_clock_time(arrival.get("turnReferenceTime") or "")
        if arrival_dt is None:
            continue

        while departure_index < len(sorted_departures):
            departure = sorted_departures[departure_index]
            departure_dt = parse_clock_time(departure.get("turnReferenceTime") or "")
            if departure_dt is None:
                departure_index += 1
                continue
            if departure_dt <= arrival_dt:
                departure_index += 1
                continue

            minutes = int((departure_dt - arrival_dt).total_seconds() // 60)
            windows.append(
                {
                    "arrivalFlightNumber": arrival["flightNumber"],
                    "departureFlightNumber": departure["flightNumber"],
                    "arrivalAirline": arrival.get("airline"),
                    "departureAirline": departure.get("airline"),
                    "arrivalTime": arrival.get("turnReferenceTime"),
                    "departureTime": departure.get("turnReferenceTime"),
                    "minutesBetween": minutes,
                    "category": classify_turn_window(minutes),
                    "sameAirline": arrival.get("airline") == departure.get("airline"),
                }
            )
            departure_index += 1
            break

    return windows


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
                "arrivalCount": 0,
                "departureCount": 0,
                "gateCount": 0,
                "gates": {},
            },
        )
        terminal["totalFlights"] += 1
        if flight.get("isEarly"):
            terminal["earlyFlights"] += 1
        if flight.get("direction") == "arrival":
            terminal["arrivalCount"] += 1
        else:
            terminal["departureCount"] += 1

        gate = terminal["gates"].setdefault(
            gate_name,
            {
                "gate": gate_name,
                "totalFlights": 0,
                "earlyFlights": 0,
                "arrivalCount": 0,
                "departureCount": 0,
                "arrivals": [],
                "departures": [],
                "turnWindows": [],
                "quickestTurnMinutes": None,
                "longestTurnMinutes": None,
            },
        )
        gate["totalFlights"] += 1
        if flight.get("isEarly"):
            gate["earlyFlights"] += 1
        if flight.get("direction") == "arrival":
            gate["arrivalCount"] += 1
            gate["arrivals"].append(flight)
        else:
            gate["departureCount"] += 1
            gate["departures"].append(flight)

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
            gate["arrivals"] = sort_flights(gate["arrivals"])
            gate["departures"] = sort_flights(gate["departures"])
            gate["flights"] = sort_flights(gate["arrivals"] + gate["departures"])
            gate["turnWindows"] = build_turn_windows(gate["arrivals"], gate["departures"])
            if gate["turnWindows"]:
                minutes = [window["minutesBetween"] for window in gate["turnWindows"]]
                gate["quickestTurnMinutes"] = min(minutes)
                gate["longestTurnMinutes"] = max(minutes)
            gate_rows.append(gate)

        gate_rows.sort(
            key=lambda gate: (
                gate["quickestTurnMinutes"] is None,
                gate["quickestTurnMinutes"] if gate["quickestTurnMinutes"] is not None else 999999,
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
    arrivals = [flight for flight in flights if flight.get("direction") == "arrival"]
    departures = [flight for flight in flights if flight.get("direction") == "departure"]
    total_flights = len(flights)
    early_flights = sum(1 for flight in arrivals if flight.get("isEarly"))
    total_gates = sum(terminal["gateCount"] for terminal in terminals)
    turn_windows = [
        window
        for terminal in terminals
        for gate in terminal["gates"]
        for window in gate["turnWindows"]
    ]
    busiest_terminal = max(
        terminals,
        key=lambda terminal: terminal["totalFlights"],
        default=None,
    )

    return {
        "totalFlights": total_flights,
        "totalArrivals": len(arrivals),
        "totalDepartures": len(departures),
        "earlyFlights": early_flights,
        "trackedGates": total_gates,
        "terminalCount": len(terminals),
        "quickTurnWindows": sum(1 for window in turn_windows if window["category"] == "quick"),
        "longTurnWindows": sum(1 for window in turn_windows if window["category"] == "long"),
        "busiestTerminal": busiest_terminal["terminal"] if busiest_terminal else None,
        "busiestTerminalFlights": busiest_terminal["totalFlights"] if busiest_terminal else 0,
    }


def scrape_gate_activity() -> dict:
    board_flights = {}
    errors = []

    with ThreadPoolExecutor(max_workers=MAX_BOARD_WORKERS) as pool:
        future_map = {
            pool.submit(parse_board_page, direction, time_period): (direction, time_period)
            for direction in ("arrival", "departure")
            for time_period in TIME_PERIODS
        }
        for future in as_completed(future_map):
            direction, time_period = future_map[future]
            try:
                flights = future.result()
            except Exception as exc:  # pragma: no cover
                errors.append(f"{direction.title()} board tp={time_period}: {exc}")
                continue

            for flight in flights:
                board_flights[(direction, flight["flightNumber"])] = flight

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
            "arrivalsBoardUrl": ARRIVALS_BOARD_URL,
            "departuresBoardUrl": DEPARTURES_BOARD_URL,
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

    payload = scrape_gate_activity()

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
