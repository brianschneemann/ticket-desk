"""
Ticket Desk — Render Scraper Server v2
Bruce Springsteen · MSG · May 11, 2026 · Section 224

Key fixes over v1:
- Correct event IDs and URL patterns for all platforms
- Uses mobile/API endpoints that return JSON instead of HTML
- Rotating user agents to reduce blocking
- Vivid Seats added as a 4th source (more reliable than TM)
- Graceful per-platform fallback with detailed error logging
"""

import os
import json
import time
import threading
import logging
import random
import re
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import urllib.request
import urllib.error

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPE_TOKEN = os.environ.get('SCRAPE_TOKEN', 'changeme')
PORT = int(os.environ.get('PORT', 10000))
DATA_FILE = 'ticket_data.json'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── Scrape State ──────────────────────────────────────────────────────────────
state = {
    'running': False,
    'last_success': None,
    'last_error': None,
    'started_at': None,
}

# ── HTTP helpers ──────────────────────────────────────────────────────────────

USER_AGENTS = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
]


def fetch(url, extra_headers=None, timeout=30):
    """HTTP GET returning text or None."""
    h = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
    }
    if extra_headers:
        h.update(extra_headers)
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            enc = resp.headers.get('Content-Encoding', '')
            if enc == 'gzip':
                import gzip
                raw = gzip.decompress(raw)
            return raw.decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        log.warning(f"HTTP {e.code} — {url}")
        return None
    except Exception as e:
        log.warning(f"Fetch error — {url} — {e}")
        return None


def fetch_json(url, extra_headers=None, timeout=30):
    """Fetch URL and parse as JSON. Returns dict/list or None."""
    h = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    if extra_headers:
        h.update(extra_headers)
    text = fetch(url, extra_headers=h, timeout=timeout)
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


# ── Price extraction ──────────────────────────────────────────────────────────

def pluck_prices(obj):
    """Recursively extract numeric values from JSON that look like ticket prices."""
    PRICE_KEYS = {'price', 'listingprice', 'amount', 'cost', 'ticketprice',
                  'currentprice', 'sellingprice', 'rawprice', 'baseprice'}
    prices = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() in PRICE_KEYS:
                try:
                    p = float(str(v).replace(',', '').replace('$', ''))
                    if 400 < p < 20000:
                        prices.append(p)
                except (ValueError, TypeError):
                    pass
            prices.extend(pluck_prices(v))
    elif isinstance(obj, list):
        for item in obj:
            prices.extend(pluck_prices(item))
    return prices


def regex_prices(text, lo=500, hi=15000):
    """Regex fallback — extract dollar amounts from raw HTML."""
    if not text:
        return []
    prices = []
    for m in re.finditer(r'\$\s*([\d,]+(?:\.\d{1,2})?)', text):
        try:
            v = float(m.group(1).replace(',', ''))
            if lo <= v <= hi:
                prices.append(v)
        except ValueError:
            pass
    return prices


def price_stats(prices):
    """Return (floor, median, count) or (None, None, 0)."""
    if not prices:
        return None, None, 0
    s = sorted(set(prices))  # deduplicate
    n = len(s)
    median = s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2
    return round(s[0]), round(median), n


# ══════════════════════════════════════════════════════════════════════════════
#  PLATFORM SCRAPERS
#
#  IMPORTANT: Event IDs below were correct as of Feb 28 2026.
#  If a platform returns no data, open its site in your browser, navigate to
#  the event, and copy the numeric ID from the URL. Update the constant here,
#  commit to GitHub, and Render will auto-redeploy.
# ══════════════════════════════════════════════════════════════════════════════

# StubHub event page URL pattern: stubhub.com/event/{ID}
# Find it: go to stubhub.com, search "Springsteen MSG May 11", open event, copy ID from URL
STUBHUB_EVENT_ID = '160512935'

# SeatGeek event ID: confirmed from search results
SEATGEEK_EVENT_ID = '18076751'

# Vivid Seats production ID: confirmed from search results
VIVIDSEATS_PROD_ID = '6671831'

# TickPick: no stable event ID needed — uses search URL
TICKPICK_SLUG = 'buy-bruce-springsteen-tickets-madison-square-garden-5-11-26'


def scrape_stubhub():
    """StubHub — two strategies: internal listings API, then page scrape."""

    # Strategy 1: Internal listing search API
    api = (
        f'https://www.stubhub.com/listingCatalog/select/?'
        f'q=event_id:{STUBHUB_EVENT_ID}'
        f'+AND+section_name:224&rows=100&start=0&wt=json'
    )
    data = fetch_json(api, extra_headers={
        'Referer': f'https://www.stubhub.com/bruce-springsteen-new-york-tickets-5-11-2026/event/{STUBHUB_EVENT_ID}/',
    })
    if data:
        prices = pluck_prices(data)
        if prices:
            f, m, c = price_stats(prices)
            log.info(f"StubHub (API1): {c} prices, floor={f}, median={m}")
            if f:
                return {'floor': f, 'median': m, 'total_count': c}

    # Strategy 2: Event page with embedded JSON
    page = fetch(
        f'https://www.stubhub.com/bruce-springsteen-new-york-tickets-5-11-2026/event/{STUBHUB_EVENT_ID}/',
        extra_headers={'Referer': 'https://www.stubhub.com/'}
    )
    if page:
        prices = []
        for blob in re.findall(r'<script[^>]*type="application/(?:json|ld\+json)"[^>]*>(.*?)</script>', page, re.DOTALL):
            try:
                prices.extend(pluck_prices(json.loads(blob)))
            except Exception:
                pass
        if not prices:
            prices = regex_prices(page)
        f, m, c = price_stats(prices)
        if f:
            log.info(f"StubHub (page): {c} prices, floor={f}, median={m}")
            return {'floor': f, 'median': m, 'total_count': c}

    log.warning("StubHub: no data retrieved — event ID may need updating")
    return None


def scrape_seatgeek():
    """SeatGeek — public API first, then page scrape."""

    # Strategy 1: SeatGeek recommendations/listings endpoint
    api = f'https://seatgeek.com/api/events/{SEATGEEK_EVENT_ID}/listings?section=224&quantity=2'
    data = fetch_json(api, extra_headers={
        'Referer': 'https://seatgeek.com/',
    })
    if data:
        prices = pluck_prices(data)
        if prices:
            f, m, c = price_stats(prices)
            log.info(f"SeatGeek (API): {c} prices, floor={f}, median={m}")
            if f:
                return {'floor': f, 'median': m, 'total_count': c}

    # Strategy 2: Event page __NEXT_DATA__
    page_url = (
        f'https://seatgeek.com/bruce-springsteen-and-the-e-street-band-tickets'
        f'/new-york-new-york-madison-square-garden-2026-05-11-7-30-pm/concert/{SEATGEEK_EVENT_ID}'
    )
    page = fetch(page_url)
    if page:
        prices = []
        for blob in re.findall(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', page, re.DOTALL):
            try:
                prices.extend(pluck_prices(json.loads(blob)))
            except Exception:
                pass
        if not prices:
            prices = regex_prices(page)
        f, m, c = price_stats(prices)
        if f:
            log.info(f"SeatGeek (page): {c} prices, floor={f}, median={m}")
            return {'floor': f, 'median': m, 'total_count': c}

    log.warning("SeatGeek: no data retrieved")
    return None


def scrape_tickpick():
    """TickPick — no-fee platform, generally less aggressive blocking."""

    page_url = f'https://www.tickpick.com/{TICKPICK_SLUG}/?filter_section=224&qty=2'
    page = fetch(page_url, extra_headers={'Referer': 'https://www.tickpick.com/'})
    if page:
        prices = []
        for blob in re.findall(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', page, re.DOTALL):
            try:
                prices.extend(pluck_prices(json.loads(blob)))
            except Exception:
                pass
        # TickPick also embeds data in a window.tp variable
        for blob in re.findall(r'window\.tp\s*=\s*(\{.*?\})\s*;', page, re.DOTALL):
            try:
                prices.extend(pluck_prices(json.loads(blob)))
            except Exception:
                pass
        if not prices:
            prices = regex_prices(page)
        f, m, c = price_stats(prices)
        if f:
            log.info(f"TickPick: {c} prices, floor={f}, median={m}")
            return {'floor': f, 'median': m, 'total_count': c}

    log.warning("TickPick: no data retrieved — slug may need updating")
    return None


def scrape_vividseats():
    """Vivid Seats — generally less aggressive than Ticketmaster."""

    # Strategy 1: Vivid Seats production API
    api = f'https://www.vividseats.com/api/production/{VIVIDSEATS_PROD_ID}/listings?quantity=2'
    data = fetch_json(api, extra_headers={
        'Referer': f'https://www.vividseats.com/production/{VIVIDSEATS_PROD_ID}',
        'Accept': 'application/json',
    })
    if data:
        prices = pluck_prices(data)
        if prices:
            f, m, c = price_stats(prices)
            log.info(f"VividSeats (API): {c} prices, floor={f}, median={m}")
            if f:
                return {'floor': f, 'median': m, 'total_count': c}

    # Strategy 2: Event page
    page_url = (
        f'https://www.vividseats.com/bruce-springsteen-tickets-new-york-madison-square-garden'
        f'-5-11-2026--concerts-rock/production/{VIVIDSEATS_PROD_ID}'
    )
    page = fetch(page_url)
    if page:
        prices = []
        for blob in re.findall(r'<script[^>]*type="application/(?:json|ld\+json)"[^>]*>(.*?)</script>', page, re.DOTALL):
            try:
                prices.extend(pluck_prices(json.loads(blob)))
            except Exception:
                pass
        if not prices:
            prices = regex_prices(page)
        f, m, c = price_stats(prices)
        if f:
            log.info(f"VividSeats (page): {c} prices, floor={f}, median={m}")
            return {'floor': f, 'median': m, 'total_count': c}

    log.warning("VividSeats: no data retrieved")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  AGGREGATION + PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════

def cross_stats(platforms):
    medians = [p['median'] for p in platforms.values() if p and p.get('median')]
    floors  = [p['floor']  for p in platforms.values() if p and p.get('floor')]
    if not medians:
        return None, None
    return round(sum(medians) / len(medians)), round(min(floors)) if floors else None


def load_history():
    try:
        with open(DATA_FILE, 'r') as f:
            return json.load(f).get('history', [])
    except Exception:
        return []


def save_data(today_record):
    history = load_history()
    today_str = today_record['date']
    history = [h for h in history if h['date'] != today_str]
    history.append(today_record)
    history.sort(key=lambda x: x['date'])

    medians = [h['cross_median'] for h in history if h.get('cross_median')]
    floors  = [h['cross_floor']  for h in history if h.get('cross_floor')]
    invs    = [h['total_inventory'] for h in history if h.get('total_inventory')]
    prior   = invs[-2] if len(invs) >= 2 else None

    output = {
        'meta': {
            'generated': datetime.now(timezone.utc).isoformat(),
            'event': 'Bruce Springsteen · MSG · May 11, 2026',
            'section': '224', 'row': '17', 'seat_type': 'Aisle',
            'source': 'scraper', 'service': 'Ticket Desk v2.0',
        },
        'today': today_record,
        'trends': {
            'median_7d_slope': round((medians[-1]-medians[-2])/7, 1) if len(medians)>=2 else 0,
            'floor_7d_accel':  round(floors[-1]-floors[-2], 1) if len(floors)>=2 else 0,
            'inventory_wow_pct': round((invs[-1]-prior)/prior*100, 1) if prior else None,
            'prior_inventory': prior,
            'medians_7d': medians[-7:],
            'floors_7d': floors[-7:],
            'inventory_7d': invs[-7:],
        },
        'history': history,
    }
    with open(DATA_FILE, 'w') as f:
        json.dump(output, f, indent=2)
    log.info("Data saved to ticket_data.json")
    return output


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN SCRAPE JOB
# ══════════════════════════════════════════════════════════════════════════════

def run_scrape():
    state['running'] = True
    state['started_at'] = datetime.now(timezone.utc).isoformat()
    state['last_error'] = None
    log.info("=== Ticket Desk scrape v2 started ===")

    try:
        scrapers = {
            'stubhub':    scrape_stubhub,
            'seatgeek':   scrape_seatgeek,
            'tickpick':   scrape_tickpick,
            'vividseats': scrape_vividseats,
        }
        platforms = {}
        for name, fn in scrapers.items():
            try:
                platforms[name] = fn()
            except Exception as e:
                log.error(f"{name} unhandled exception: {e}")
                platforms[name] = None
            time.sleep(1.5 + random.random())  # polite pacing

        active = {k: v for k, v in platforms.items() if v}
        log.info(f"Results: {len(active)}/4 platforms live — {list(active.keys())}")

        if not active:
            raise Exception(
                "All 4 platforms returned no data. Most likely cause: event IDs in server.py "
                "need updating. Open each platform in your browser, find the MSG May 11 event, "
                "copy the numeric ID from the URL, and update STUBHUB_EVENT_ID, "
                "SEATGEEK_EVENT_ID, and VIVIDSEATS_PROD_ID at the top of server.py."
            )

        cross_median, cross_floor = cross_stats(active)
        total_inventory = sum(p.get('total_count', 0) for p in active.values())
        plat_medians = [p['median'] for p in active.values()]
        spread = (
            round((max(plat_medians) - min(plat_medians)) / cross_median * 100, 1)
            if len(plat_medians) >= 2 and cross_median else 0
        )

        today = {
            'date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'cross_median': cross_median,
            'cross_floor': cross_floor,
            'total_inventory': total_inventory,
            'aisle_count': 2,
            'platform_spread_pct': spread,
            'source': 'scraper',
            'platforms': active,
        }

        save_data(today)
        state['last_success'] = datetime.now(timezone.utc).isoformat()
        log.info(f"=== Done: median=${cross_median}, floor=${cross_floor}, inv={total_inventory} ===")

    except Exception as e:
        state['last_error'] = str(e)
        log.error(f"Scrape failed: {e}")
    finally:
        state['running'] = False


# ══════════════════════════════════════════════════════════════════════════════
#  HTTP SERVER
# ══════════════════════════════════════════════════════════════════════════════

def json_resp(handler, data, status=200):
    body = json.dumps(data, indent=2).encode()
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', str(len(body)))
    handler.send_header('Access-Control-Allow-Origin', '*')
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')
        qs = parse_qs(parsed.query)

        if path in ('', '/'):
            json_resp(self, {
                'service': 'Ticket Desk v2.0',
                'event': 'Bruce Springsteen · MSG · May 11 2026',
                'endpoints': ['/status', '/scrape?token=YOUR_TOKEN', '/data'],
                'status': 'ok',
            })

        elif path == '/status':
            json_resp(self, {
                'service': 'Ticket Desk v2.0',
                'running': state['running'],
                'last_success': state['last_success'],
                'last_error': state['last_error'],
                'started_at': state['started_at'],
            })

        elif path == '/scrape':
            if qs.get('token', [''])[0] != SCRAPE_TOKEN:
                json_resp(self, {'error': 'unauthorized'}, 401)
                return
            if state['running']:
                json_resp(self, {'status': 'already_running', 'started_at': state['started_at']})
                return
            threading.Thread(target=run_scrape, daemon=True).start()
            json_resp(self, {'status': 'started'})

        elif path == '/data':
            try:
                body = open(DATA_FILE, 'rb').read()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                json_resp(self, {'error': 'no data yet — trigger /scrape first'}, 404)

        else:
            json_resp(self, {'error': 'not found'}, 404)


if __name__ == '__main__':
    log.info(f"Ticket Desk v2.0 starting on port {PORT}")
    HTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
