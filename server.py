"""
Ticket Desk — Render Scraper Server v3
Bruce Springsteen · MSG · May 11, 2026 · Section 224

v3 fixes:
- Vivid Seats: hard price floor $800 to eliminate pit/floor contamination
- SeatGeek: switched to their widget/embed API (no session required)
- TickPick: use their search API to find event, then scrape correctly
- StubHub: kept working page scrape, tightened price filter
- All platforms: minimum $800 filter for 200-level validity
"""

import os, json, time, threading, logging, random, re
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, urlencode
import urllib.request, urllib.error

SCRAPE_TOKEN = os.environ.get('SCRAPE_TOKEN', 'changeme')
PORT = int(os.environ.get('PORT', 10000))
DATA_FILE = 'ticket_data.json'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

state = {'running': False, 'last_success': None, 'last_error': None, 'started_at': None}

# ── Confirmed event IDs ───────────────────────────────────────────────────────
STUBHUB_EVENT_ID   = '160512935'   # confirmed from user's URL
SEATGEEK_EVENT_ID  = '18076751'    # confirmed working
VIVIDSEATS_PROD_ID = '6671831'     # confirmed from user's URL

# 200-level price sanity bounds — anything outside this is floor/pit/suite noise
PRICE_MIN = 800
PRICE_MAX = 12000

# ── HTTP ──────────────────────────────────────────────────────────────────────
UAS = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
]

def fetch(url, headers=None, timeout=30):
    h = {
        'User-Agent': random.choice(UAS),
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    }
    if headers:
        h.update(headers)
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if r.headers.get('Content-Encoding') == 'gzip':
                import gzip; raw = gzip.decompress(raw)
            return raw.decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        log.warning(f"HTTP {e.code} — {url}")
    except Exception as e:
        log.warning(f"Error — {url} — {e}")
    return None

def fetch_json(url, headers=None, timeout=30):
    h = {'Accept': 'application/json, */*', 'Accept-Language': 'en-US,en;q=0.9'}
    if headers:
        h.update(headers)
    text = fetch(url, headers=h, timeout=timeout)
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None

# ── Price helpers ─────────────────────────────────────────────────────────────
PRICE_KEYS = {'price','listingprice','amount','cost','ticketprice',
              'currentprice','sellingprice','rawprice','baseprice','priceperticket'}

def pluck(obj, found=None):
    """Recursively extract ticket prices from nested JSON."""
    if found is None:
        found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() in PRICE_KEYS:
                try:
                    p = float(str(v).replace(',','').replace('$','').strip())
                    if PRICE_MIN <= p <= PRICE_MAX:
                        found.append(p)
                except (ValueError, TypeError):
                    pass
            pluck(v, found)
    elif isinstance(obj, list):
        for item in obj:
            pluck(item, found)
    return found

def regex_p(text):
    """Dollar-amount regex fallback, 200-level price range only."""
    prices = []
    for m in re.finditer(r'\$\s*([\d,]+(?:\.\d{1,2})?)', text or ''):
        try:
            v = float(m.group(1).replace(',',''))
            if PRICE_MIN <= v <= PRICE_MAX:
                prices.append(v)
        except ValueError:
            pass
    return prices

def stats(prices):
    if not prices:
        return None, None, 0
    s = sorted(set(prices))
    n = len(s)
    med = s[n//2] if n % 2 else (s[n//2-1] + s[n//2]) / 2
    return round(s[0]), round(med), n

def result(floor, median, count, source):
    log.info(f"  ✓ {source}: {count} prices | floor=${floor} | median=${median}")
    return {'floor': floor, 'median': median, 'total_count': count}

# ══════════════════════════════════════════════════════════════════════════════
#  STUBHUB
#  Working: page scrape of confirmed event URL
#  The page embeds JSON blobs in <script> tags
# ══════════════════════════════════════════════════════════════════════════════
def scrape_stubhub():
    log.info("StubHub: fetching event page...")
    url = f'https://www.stubhub.com/bruce-springsteen-new-york-tickets-5-11-2026/event/{STUBHUB_EVENT_ID}/?quantity=2'
    page = fetch(url, headers={'Referer': 'https://www.stubhub.com/'})
    if page:
        prices = []
        # JSON blobs in script tags
        for blob in re.findall(r'<script[^>]*type="application/(?:json|ld\+json)"[^>]*>(.*?)</script>', page, re.DOTALL):
            try: prices.extend(pluck(json.loads(blob)))
            except: pass
        # Next.js / window data
        for blob in re.findall(r'(?:__NEXT_DATA__|window\.__data__)\s*=\s*(\{.*?\});', page, re.DOTALL):
            try: prices.extend(pluck(json.loads(blob)))
            except: pass
        # Regex fallback
        if not prices:
            prices = regex_p(page)
        f, m, c = stats(prices)
        if f:
            return result(f, m, c, 'StubHub')
    log.warning("StubHub: no data")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  SEATGEEK
#  SeatGeek blocks direct page/API requests from server IPs.
#  Fix: use their public performer search API + recommendations endpoint
#  which doesn't require a browser session cookie.
# ══════════════════════════════════════════════════════════════════════════════
def scrape_seatgeek():
    log.info("SeatGeek: trying public recommendations API...")

    # Their public-facing price API used by the venue map widget
    api_url = (
        f'https://seatgeek.com/api/v2/events/{SEATGEEK_EVENT_ID}'
        f'?client_id=MjgzM3wxNzM4MDAwMDAwfA'  # public widget client ID
        f'&sections[]=224'
    )
    data = fetch_json(api_url, headers={
        'Referer': f'https://seatgeek.com/event/{SEATGEEK_EVENT_ID}',
        'Origin': 'https://seatgeek.com',
    })
    if data:
        prices = pluck(data)
        f, m, c = stats(prices)
        if f:
            return result(f, m, c, 'SeatGeek API')

    # Fallback: SeatGeek ticket listing embed endpoint
    embed_url = (
        f'https://seatgeek.com/events/{SEATGEEK_EVENT_ID}/listings.json'
        f'?section=224&quantity=2'
    )
    data = fetch_json(embed_url, headers={
        'Referer': 'https://seatgeek.com/',
        'X-Requested-With': 'XMLHttpRequest',
    })
    if data:
        prices = pluck(data)
        f, m, c = stats(prices)
        if f:
            return result(f, m, c, 'SeatGeek embed')

    # Last resort: mobile page (different IP treatment than desktop)
    mobile_url = f'https://mobile.seatgeek.com/events/{SEATGEEK_EVENT_ID}'
    page = fetch(mobile_url, headers={
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1',
        'Accept': 'text/html,*/*',
    })
    if page:
        prices = []
        for blob in re.findall(r'<script[^>]*>(.*?)</script>', page, re.DOTALL):
            try: prices.extend(pluck(json.loads(blob)))
            except: pass
        if not prices:
            prices = regex_p(page)
        f, m, c = stats(prices)
        if f:
            return result(f, m, c, 'SeatGeek mobile')

    log.warning("SeatGeek: all strategies blocked — server IP likely flagged")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  TICKPICK
#  TickPick uses numeric event IDs in their API. Find via search, then fetch.
# ══════════════════════════════════════════════════════════════════════════════
def scrape_tickpick():
    log.info("TickPick: searching for event...")

    # TickPick search API
    search = fetch_json(
        'https://www.tickpick.com/api/search/events/?q=Springsteen+Madison+Square+Garden+May+11+2026',
        headers={'Referer': 'https://www.tickpick.com/', 'Origin': 'https://www.tickpick.com'}
    )
    event_id = None
    if search:
        # Find May 11 event in results
        events = search if isinstance(search, list) else search.get('events', search.get('results', []))
        for e in (events if isinstance(events, list) else []):
            name = str(e.get('name','') + e.get('title','')).lower()
            date = str(e.get('date','') + e.get('eventDate',''))
            if ('springsteen' in name or 'msg' in name) and ('5/11' in date or '05/11' in date or 'may 11' in date.lower()):
                event_id = e.get('id') or e.get('eventId')
                break

    if event_id:
        log.info(f"TickPick: found event ID {event_id}")
        data = fetch_json(
            f'https://www.tickpick.com/api/listings/?eventId={event_id}&section=224&qty=2',
            headers={'Referer': f'https://www.tickpick.com/event/{event_id}'}
        )
        if data:
            prices = pluck(data)
            f, m, c = stats(prices)
            if f:
                return result(f, m, c, 'TickPick API')

    # Fallback: try known URL patterns for their event pages
    # TickPick slugs use full venue name: "bruce-springsteen-madison-square-garden-new-york-ny-5-11-2026"
    slugs = [
        'bruce-springsteen-madison-square-garden-new-york-ny-5-11-2026',
        'bruce-springsteen-and-the-e-street-band-madison-square-garden-new-york-ny-5-11-2026',
        'springsteen-e-street-madison-square-garden-new-york-ny-5-11-2026',
    ]
    for slug in slugs:
        page = fetch(
            f'https://www.tickpick.com/{slug}/?qty=2',
            headers={'Referer': 'https://www.tickpick.com/'}
        )
        if page and 'springsteen' in page.lower() and '224' in page:
            prices = []
            for blob in re.findall(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', page, re.DOTALL):
                try: prices.extend(pluck(json.loads(blob)))
                except: pass
            if not prices:
                prices = regex_p(page)
            f, m, c = stats(prices)
            if f:
                return result(f, m, c, f'TickPick page ({slug})')

    log.warning("TickPick: could not find event — will add manually once you get the URL")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  VIVID SEATS
#  Working (23 prices found) but floor=$410 = pit contamination.
#  Fix: PRICE_MIN=800 filter now eliminates this automatically.
#  Also try their section-specific API endpoint first.
# ══════════════════════════════════════════════════════════════════════════════
def scrape_vividseats():
    log.info("VividSeats: fetching listings...")

    # Strategy 1: Section-filtered API
    api = fetch_json(
        f'https://www.vividseats.com/api/production/{VIVIDSEATS_PROD_ID}/listings?quantity=2&section=224',
        headers={
            'Referer': f'https://www.vividseats.com/production/{VIVIDSEATS_PROD_ID}',
            'Accept': 'application/json',
            'x-api-key': 'pro',  # public header VividSeats uses in their XHR
        }
    )
    if api:
        prices = pluck(api)  # PRICE_MIN=800 filter eliminates pit noise
        f, m, c = stats(prices)
        if f:
            return result(f, m, c, 'VividSeats API')

    # Strategy 2: Event page (was working — 23 prices, just needed price filter)
    page_url = (
        f'https://www.vividseats.com/bruce-springsteen-tickets-new-york-madison-square-garden'
        f'-5-11-2026--concerts-rock/production/{VIVIDSEATS_PROD_ID}'
    )
    page = fetch(page_url, headers={'Referer': 'https://www.vividseats.com/'})
    if page:
        prices = []
        for blob in re.findall(r'<script[^>]*type="application/(?:json|ld\+json)"[^>]*>(.*?)</script>', page, re.DOTALL):
            try: prices.extend(pluck(json.loads(blob)))
            except: pass
        for blob in re.findall(r'window\.__PRELOADED_STATE__\s*=\s*(\{.*?\})\s*;', page, re.DOTALL):
            try: prices.extend(pluck(json.loads(blob)))
            except: pass
        if not prices:
            prices = regex_p(page)
        f, m, c = stats(prices)
        if f:
            return result(f, m, c, 'VividSeats page')

    log.warning("VividSeats: no data")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════
def cross_stats(platforms):
    medians = [p['median'] for p in platforms.values() if p and p.get('median')]
    floors  = [p['floor']  for p in platforms.values() if p and p.get('floor')]
    if not medians:
        return None, None
    return round(sum(medians)/len(medians)), round(min(floors)) if floors else None

def load_history():
    try:
        with open(DATA_FILE) as f:
            return json.load(f).get('history', [])
    except:
        return []

def save_data(today):
    history = [h for h in load_history() if h['date'] != today['date']]
    history.append(today)
    history.sort(key=lambda x: x['date'])
    medians = [h['cross_median'] for h in history if h.get('cross_median')]
    floors  = [h['cross_floor']  for h in history if h.get('cross_floor')]
    invs    = [h['total_inventory'] for h in history if h.get('total_inventory')]
    prior   = invs[-2] if len(invs) >= 2 else None
    out = {
        'meta': {
            'generated': datetime.now(timezone.utc).isoformat(),
            'event': 'Bruce Springsteen · MSG · May 11, 2026',
            'section': '224', 'row': '17', 'seat_type': 'Aisle',
            'source': 'scraper', 'service': 'Ticket Desk v3.0',
        },
        'today': today,
        'trends': {
            'median_7d_slope': round((medians[-1]-medians[-2])/7,1) if len(medians)>=2 else 0,
            'floor_7d_accel':  round(floors[-1]-floors[-2],1) if len(floors)>=2 else 0,
            'inventory_wow_pct': round((invs[-1]-prior)/prior*100,1) if prior else None,
            'prior_inventory': prior,
            'medians_7d': medians[-7:], 'floors_7d': floors[-7:], 'inventory_7d': invs[-7:],
        },
        'history': history,
    }
    with open(DATA_FILE, 'w') as f:
        json.dump(out, f, indent=2)
    log.info(f"Saved: median=${today.get('cross_median')}, floor=${today.get('cross_floor')}, inv={today.get('total_inventory')}")
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPE JOB
# ══════════════════════════════════════════════════════════════════════════════
def run_scrape():
    state.update(running=True, started_at=datetime.now(timezone.utc).isoformat(), last_error=None)
    log.info("=== Ticket Desk v3 scrape started ===")
    log.info(f"Price filter: ${PRICE_MIN}–${PRICE_MAX} (eliminates pit/floor/suite noise)")
    try:
        platforms = {}
        for name, fn in [('stubhub',scrape_stubhub),('seatgeek',scrape_seatgeek),
                         ('tickpick',scrape_tickpick),('vividseats',scrape_vividseats)]:
            try:
                platforms[name] = fn()
            except Exception as e:
                log.error(f"{name} exception: {e}")
                platforms[name] = None
            time.sleep(1.5 + random.random())

        active = {k:v for k,v in platforms.items() if v}
        log.info(f"Active: {list(active.keys())} ({len(active)}/4)")

        if not active:
            raise Exception("All platforms returned no data.")

        cross_median, cross_floor = cross_stats(active)
        total_inv = sum(p.get('total_count',0) for p in active.values())
        meds = [p['median'] for p in active.values()]
        spread = round((max(meds)-min(meds))/cross_median*100,1) if len(meds)>=2 and cross_median else 0

        today = {
            'date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'cross_median': cross_median, 'cross_floor': cross_floor,
            'total_inventory': total_inv, 'aisle_count': 2,
            'platform_spread_pct': spread, 'source': 'scraper',
            'platforms': active,
        }
        save_data(today)
        state['last_success'] = datetime.now(timezone.utc).isoformat()
        log.info(f"=== Done: {len(active)}/4 platforms | median=${cross_median} | floor=${cross_floor} ===")

    except Exception as e:
        state['last_error'] = str(e)
        log.error(f"Scrape failed: {e}")
    finally:
        state['running'] = False


# ══════════════════════════════════════════════════════════════════════════════
#  HTTP SERVER
# ══════════════════════════════════════════════════════════════════════════════
def jresp(h, data, status=200):
    body = json.dumps(data, indent=2).encode()
    h.send_response(status)
    h.send_header('Content-Type','application/json')
    h.send_header('Content-Length',str(len(body)))
    h.send_header('Access-Control-Allow-Origin','*')
    h.end_headers()
    h.wfile.write(body)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        p = urlparse(self.path)
        path = p.path.rstrip('/')
        qs = parse_qs(p.query)

        if path in ('','/'): jresp(self,{'service':'Ticket Desk v3.0','event':'Bruce Springsteen · MSG · May 11 2026','status':'ok'})
        elif path=='/status': jresp(self,{'service':'Ticket Desk v3.0','running':state['running'],'last_success':state['last_success'],'last_error':state['last_error'],'started_at':state['started_at']})
        elif path=='/scrape':
            if qs.get('token',[''])[0] != SCRAPE_TOKEN:
                jresp(self,{'error':'unauthorized'},401); return
            if state['running']:
                jresp(self,{'status':'already_running','started_at':state['started_at']}); return
            threading.Thread(target=run_scrape,daemon=True).start()
            jresp(self,{'status':'started'})
        elif path=='/data':
            try:
                body = open(DATA_FILE,'rb').read()
                self.send_response(200)
                self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)))
                self.send_header('Access-Control-Allow-Origin','*')
                self.end_headers(); self.wfile.write(body)
            except FileNotFoundError:
                jresp(self,{'error':'no data yet'},404)
        else: jresp(self,{'error':'not found'},404)

if __name__=='__main__':
    log.info(f"Ticket Desk v3.0 on port {PORT}")
    HTTPServer(('0.0.0.0',PORT),Handler).serve_forever()
