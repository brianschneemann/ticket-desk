"""
Ticket Desk — Render Scraper Server
Bruce Springsteen · MSG · May 11, 2026 · Section 224

Deploy to Render.com (free tier). Scrapes StubHub, SeatGeek,
TickPick, and Ticketmaster for Section 224 pricing data.
Serves JSON to the dashboard's auto-discovery endpoint.
"""

import os
import json
import time
import threading
import logging
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import urllib.request
import urllib.error
import re

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPE_TOKEN = os.environ.get('SCRAPE_TOKEN', 'changeme')
PORT = int(os.environ.get('PORT', 10000))
DATA_FILE = 'ticket_data.json'
LOG_FILE = 'scrape_log.txt'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── Scrape State ──────────────────────────────────────────────────────────────
state = {
    'running': False,
    'last_success': None,
    'last_error': None,
    'started_at': None,
}

# ── Scraper helpers ───────────────────────────────────────────────────────────

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def fetch(url, timeout=25):
    """Simple HTTP fetch returning text, or None on failure."""
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode('utf-8', errors='replace')
    except Exception as e:
        log.warning(f"fetch error {url}: {e}")
        return None


def extract_prices(text, pattern):
    """Extract all dollar amounts matching pattern from text."""
    if not text:
        return []
    nums = re.findall(pattern, text)
    prices = []
    for n in nums:
        try:
            v = float(n.replace(',', ''))
            if 200 < v < 20000:
                prices.append(v)
        except ValueError:
            pass
    return prices


def stats(prices):
    """Return floor and median from a list of prices."""
    if not prices:
        return None, None
    s = sorted(prices)
    floor = s[0]
    mid = len(s) // 2
    median = s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2
    return round(floor), round(median)


# ── Platform scrapers ─────────────────────────────────────────────────────────

def scrape_stubhub():
    """
    StubHub search for Section 224, May 11 2026 Springsteen MSG.
    Uses their mobile search endpoint which returns JSON-LD data.
    """
    url = (
        'https://www.stubhub.com/bruce-springsteen-new-york-tickets-5-11-2026/'
        '?quantity=2&sectionId=224'
    )
    html = fetch(url)
    if not html:
        return None

    # Try structured data first (JSON-LD)
    jld_matches = re.findall(r'<script type="application/json"[^>]*>(.*?)</script>', html, re.DOTALL)
    prices = []
    for block in jld_matches:
        try:
            data = json.loads(block)
            # Flatten and search
            text = json.dumps(data)
            found = re.findall(r'"price"\s*:\s*([\d,]+\.?\d*)', text)
            for f in found:
                try:
                    v = float(f.replace(',', ''))
                    if 200 < v < 20000:
                        prices.append(v)
                except ValueError:
                    pass
        except Exception:
            pass

    # Fallback: regex price extraction from page
    if not prices:
        prices = extract_prices(html, r'\$\s*([\d,]+(?:\.\d{2})?)')

    floor, median = stats(prices)
    count = len(prices)
    log.info(f"StubHub: {count} prices, floor={floor}, median={median}")
    if not floor:
        return None
    return {'floor': floor, 'median': median, 'total_count': count}


def scrape_seatgeek():
    """SeatGeek API search for event + section filter."""
    # SeatGeek has a public search endpoint
    url = (
        'https://seatgeek.com/bruce-springsteen-tickets/new-york-new-york-madison-square-garden-2026-05-11-20-00'
        '?range=200s'
    )
    html = fetch(url)
    if not html:
        return None

    # SeatGeek embeds __NEXT_DATA__ JSON
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    prices = []
    if match:
        try:
            data = json.loads(match.group(1))
            text = json.dumps(data)
            found = re.findall(r'"price"\s*:\s*([\d.]+)', text)
            for f in found:
                try:
                    v = float(f)
                    if 200 < v < 20000:
                        prices.append(v)
                except ValueError:
                    pass
        except Exception:
            pass

    if not prices:
        prices = extract_prices(html, r'\$([\d,]+(?:\.\d{2})?)')

    floor, median = stats(prices)
    count = len(prices)
    log.info(f"SeatGeek: {count} prices, floor={floor}, median={median}")
    if not floor:
        return None
    return {'floor': floor, 'median': median, 'total_count': count}


def scrape_tickpick():
    """TickPick — no-fee marketplace, tends to show all-in prices."""
    url = (
        'https://www.tickpick.com/buy-bruce-springsteen-tickets-madison-square-garden-5-11-26/'
        '?q=section+224'
    )
    html = fetch(url)
    if not html:
        return None

    prices = extract_prices(html, r'\$([\d,]+(?:\.\d{2})?)')
    # TickPick prices tend to be all-in; filter plausible range
    prices = [p for p in prices if 300 < p < 15000]

    floor, median = stats(prices)
    count = len(prices)
    log.info(f"TickPick: {count} prices, floor={floor}, median={median}")
    if not floor:
        return None
    return {'floor': floor, 'median': median, 'total_count': count}


def scrape_ticketmaster():
    """Ticketmaster resale search."""
    url = (
        'https://www.ticketmaster.com/event/Z7r9jZ1A7nj_G'  # MSG Springsteen event
        '?sc_id=listings&section=224'
    )
    html = fetch(url)
    if not html:
        # Try alternate URL pattern
        url2 = 'https://www.ticketmaster.com/bruce-springsteen-new-york-tickets/event/Z7r9jZ1A7nj_G'
        html = fetch(url2)
    if not html:
        return None

    prices = extract_prices(html, r'\$([\d,]+(?:\.\d{2})?)')
    prices = [p for p in prices if 400 < p < 20000]

    floor, median = stats(prices)
    count = len(prices)
    log.info(f"Ticketmaster: {count} prices, floor={floor}, median={median}")
    if not floor:
        return None
    return {'floor': floor, 'median': median, 'total_count': count}


# ── Cross-platform aggregation ────────────────────────────────────────────────

def cross_stats(platforms):
    """Compute cross-platform composite median and floor."""
    all_medians = [p['median'] for p in platforms.values() if p and p.get('median')]
    all_floors  = [p['floor']  for p in platforms.values() if p and p.get('floor')]
    if not all_medians:
        return None, None
    cross_median = round(sum(all_medians) / len(all_medians))
    cross_floor  = round(min(all_floors)) if all_floors else None
    return cross_median, cross_floor


def load_history():
    try:
        with open(DATA_FILE, 'r') as f:
            data = json.load(f)
            return data.get('history', [])
    except Exception:
        return []


def save_data(today_record, platforms):
    history = load_history()

    # Upsert by date
    today_str = today_record['date']
    history = [h for h in history if h['date'] != today_str]
    history.append(today_record)
    history.sort(key=lambda x: x['date'])

    # Compute trends
    medians = [h['cross_median'] for h in history if h.get('cross_median')]
    floors  = [h['cross_floor']  for h in history if h.get('cross_floor')]
    invs    = [h['total_inventory'] for h in history if h.get('total_inventory')]

    prior_inv = invs[-2] if len(invs) >= 2 else None
    med_slope = round((medians[-1] - medians[-2]) / 7, 1) if len(medians) >= 2 else 0
    floor_accel = round((floors[-1] - floors[-2]), 1) if len(floors) >= 2 else 0
    inv_wow = round((invs[-1] - prior_inv) / prior_inv * 100, 1) if prior_inv else None

    output = {
        'meta': {
            'generated': datetime.now(timezone.utc).isoformat(),
            'event': 'Bruce Springsteen · MSG · May 11, 2026',
            'section': '224',
            'row': '17',
            'seat_type': 'Aisle',
            'source': 'scraper',
            'service': 'Ticket Desk v1.0',
        },
        'today': today_record,
        'trends': {
            'median_7d_slope': med_slope,
            'floor_7d_accel': floor_accel,
            'inventory_wow_pct': inv_wow,
            'prior_inventory': prior_inv,
            'medians_7d': medians[-7:],
            'floors_7d': floors[-7:],
            'inventory_7d': invs[-7:],
        },
        'history': history,
    }

    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    log.info(f"Saved data: {DATA_FILE}")
    return output


# ── Main scrape job ───────────────────────────────────────────────────────────

def run_scrape():
    state['running'] = True
    state['started_at'] = datetime.now(timezone.utc).isoformat()
    state['last_error'] = None
    log.info("Scrape started")

    try:
        platforms = {
            'stubhub':      scrape_stubhub(),
            'seatgeek':     scrape_seatgeek(),
            'tickpick':     scrape_tickpick(),
            'ticketmaster': scrape_ticketmaster(),
        }

        # Filter None platforms
        active = {k: v for k, v in platforms.items() if v}
        log.info(f"Active platforms: {list(active.keys())}")

        if not active:
            raise Exception("All platforms returned no data — likely blocked. Check Render logs.")

        cross_median, cross_floor = cross_stats(active)
        total_inventory = sum(p.get('total_count', 0) for p in active.values())

        today = {
            'date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'cross_median': cross_median,
            'cross_floor': cross_floor,
            'total_inventory': total_inventory,
            'aisle_count': 2,  # manual — update if you track this
            'platform_spread_pct': round(
                (max(p['median'] for p in active.values()) -
                 min(p['median'] for p in active.values())) / cross_median * 100, 1
            ) if len(active) >= 2 else 0,
            'source': 'scraper',
            'platforms': active,
        }

        output = save_data(today, active)

        state['last_success'] = datetime.now(timezone.utc).isoformat()
        log.info(f"Scrape complete. cross_median={cross_median}, floor={cross_floor}, inv={total_inventory}")

    except Exception as e:
        state['last_error'] = str(e)
        log.error(f"Scrape failed: {e}")
    finally:
        state['running'] = False


# ── HTTP Server ───────────────────────────────────────────────────────────────

def json_response(handler, data, status=200):
    body = json.dumps(data).encode()
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', str(len(body)))
    handler.send_header('Access-Control-Allow-Origin', '*')
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass  # Suppress default access log noise

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')
        qs = parse_qs(parsed.query)

        # ── /status — dashboard polls this while scrape runs ──────────────────
        if path == '/status':
            json_response(self, {
                'service': 'Ticket Desk v1.0',
                'running': state['running'],
                'last_success': state['last_success'],
                'last_error': state['last_error'],
                'started_at': state['started_at'],
            })

        # ── /scrape?token=XXX — trigger a scrape ──────────────────────────────
        elif path == '/scrape':
            token = qs.get('token', [''])[0]
            if token != SCRAPE_TOKEN:
                json_response(self, {'error': 'unauthorized'}, 401)
                return
            if state['running']:
                json_response(self, {'status': 'already_running', 'started_at': state['started_at']})
                return
            t = threading.Thread(target=run_scrape, daemon=True)
            t.start()
            json_response(self, {'status': 'started'})

        # ── /data — dashboard fetches this for live JSON ──────────────────────
        elif path == '/data':
            try:
                with open(DATA_FILE, 'r') as f:
                    raw = f.read()
                body = raw.encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                json_response(self, {'error': 'no data yet — trigger /scrape first'}, 404)

        # ── / — health check (auto-discovery uses this) ───────────────────────
        elif path in ('', '/'):
            json_response(self, {
                'service': 'Ticket Desk v1.0',
                'event': 'Bruce Springsteen · MSG · May 11 2026',
                'endpoints': ['/status', '/scrape?token=XXX', '/data'],
                'status': 'ok',
            })

        else:
            json_response(self, {'error': 'not found'}, 404)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    log.info(f"Ticket Desk server starting on port {PORT}")
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    server.serve_forever()
