"""
Ticket Desk — Render Scraper Server v4
Bruce Springsteen · MSG · May 11, 2026 · Section 224

v4 changes:
- TickPick: correct event ID 7742380 and confirmed slug
- SeatGeek: /relay endpoint — your browser POSTs prices directly, bypassing IP block
- All confirmed event IDs locked in
"""

import os, json, time, threading, logging, random, re
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import urllib.request, urllib.error

SCRAPE_TOKEN    = os.environ.get('SCRAPE_TOKEN', 'changeme')
PORT            = int(os.environ.get('PORT', 10000))
DATA_FILE       = 'ticket_data.json'
RELAY_FILE      = 'relay_data.json'   # stores browser-relayed platform data

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

state = {'running': False, 'last_success': None, 'last_error': None, 'started_at': None}

# ── Confirmed event IDs ───────────────────────────────────────────────────────
STUBHUB_EVENT_ID   = '160512935'
SEATGEEK_EVENT_ID  = '18076751'
VIVIDSEATS_PROD_ID = '6671831'
TICKPICK_EVENT_ID  = '7742380'
TICKPICK_SLUG      = 'buy-bruce-springsteen-the-e-street-band-tickets-madison-square-garden-5-11-26-7pm'

# 200-level price sanity bounds
PRICE_MIN = 800
PRICE_MAX = 12000

# ── HTTP helpers ──────────────────────────────────────────────────────────────
UAS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
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
    prices = []
    for m in re.finditer(r'\$\s*([\d,]+(?:\.\d{1,2})?)', text or ''):
        try:
            v = float(m.group(1).replace(',',''))
            if PRICE_MIN <= v <= PRICE_MAX:
                prices.append(v)
        except ValueError:
            pass
    return prices

def pstats(prices):
    if not prices:
        return None, None, 0
    s = sorted(set(prices))
    n = len(s)
    med = s[n//2] if n % 2 else (s[n//2-1] + s[n//2]) / 2
    return round(s[0]), round(med), n

def ok(f, m, c, src):
    log.info(f"  ✓ {src}: {c} prices | floor=${f} | median=${m}")
    return {'floor': f, 'median': m, 'total_count': c}


# ══════════════════════════════════════════════════════════════════════════════
#  STUBHUB — working in v3, unchanged
# ══════════════════════════════════════════════════════════════════════════════
def scrape_stubhub():
    log.info("StubHub: fetching...")
    url = f'https://www.stubhub.com/bruce-springsteen-new-york-tickets-5-11-2026/event/{STUBHUB_EVENT_ID}/?quantity=2'
    page = fetch(url, headers={'Referer': 'https://www.stubhub.com/'})
    if page:
        prices = []
        for blob in re.findall(r'<script[^>]*type="application/(?:json|ld\+json)"[^>]*>(.*?)</script>', page, re.DOTALL):
            try: prices.extend(pluck(json.loads(blob)))
            except: pass
        for blob in re.findall(r'(?:__NEXT_DATA__|window\.__data__)\s*=\s*(\{.*?\});', page, re.DOTALL):
            try: prices.extend(pluck(json.loads(blob)))
            except: pass
        if not prices:
            prices = regex_p(page)
        f, m, c = pstats(prices)
        if f:
            return ok(f, m, c, 'StubHub')
    log.warning("StubHub: no data")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  SEATGEEK — IP blocked on Render. Served via /relay endpoint instead.
#  scrape_seatgeek() reads from relay_data.json if a recent relay exists.
# ══════════════════════════════════════════════════════════════════════════════
def scrape_seatgeek():
    log.info("SeatGeek: checking relay cache...")
    try:
        with open(RELAY_FILE) as f:
            relay = json.load(f)
        sg = relay.get('seatgeek', {})
        # Accept relay data if it's from today
        if sg.get('date') == datetime.now(timezone.utc).strftime('%Y-%m-%d'):
            f2, m2, c2 = sg.get('floor'), sg.get('median'), sg.get('count', 1)
            if f2 and m2:
                log.info(f"  ✓ SeatGeek (relay): floor=${f2} | median=${m2}")
                return {'floor': f2, 'median': m2, 'total_count': c2}
        log.info("SeatGeek: relay data stale or missing — skipping (use /relay to update)")
    except FileNotFoundError:
        log.info("SeatGeek: no relay file yet — use /relay endpoint from your browser")
    except Exception as e:
        log.warning(f"SeatGeek relay read error: {e}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  TICKPICK — relay only (JS-rendered, cannot scrape from server)
#  Use relay form: open tickpick.com/...7742380/, filter to sec 224,
#  enter floor + median in the relay form at /relay-form?token=YOUR_TOKEN
# ══════════════════════════════════════════════════════════════════════════════
def scrape_tickpick():
    log.info("TickPick: checking relay cache...")
    try:
        with open(RELAY_FILE) as f:
            relay = json.load(f)
        tp = relay.get('tickpick', {})
        if tp.get('date') == datetime.now(timezone.utc).strftime('%Y-%m-%d'):
            f2, m2, c2 = tp.get('floor'), tp.get('median'), tp.get('count', 1)
            if f2 and m2:
                log.info(f"  ✓ TickPick (relay): floor=${f2} | median=${m2}")
                return {'floor': f2, 'median': m2, 'total_count': c2}
        log.info("TickPick: relay data stale or missing — use /relay-form")
    except FileNotFoundError:
        log.info("TickPick: no relay file yet — use /relay-form")
    except Exception as e:
        log.warning(f"TickPick relay error: {e}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  VIVID SEATS — relay only (page scrape pulls all sections, not 224-specific)
#  Use relay form: open vividseats.com/production/6671831, filter to sec 224,
#  enter floor + median in the relay form at /relay-form?token=YOUR_TOKEN
# ══════════════════════════════════════════════════════════════════════════════
def scrape_vividseats():
    log.info("VividSeats: checking relay cache...")
    try:
        with open(RELAY_FILE) as f:
            relay = json.load(f)
        vs = relay.get('vividseats', {})
        if vs.get('date') == datetime.now(timezone.utc).strftime('%Y-%m-%d'):
            f2, m2, c2 = vs.get('floor'), vs.get('median'), vs.get('count', 1)
            if f2 and m2:
                log.info(f"  ✓ VividSeats (relay): floor=${f2} | median=${m2}")
                return {'floor': f2, 'median': m2, 'total_count': c2}
        log.info("VividSeats: relay data stale or missing — use /relay-form")
    except FileNotFoundError:
        log.info("VividSeats: no relay file yet — use /relay-form")
    except Exception as e:
        log.warning(f"VividSeats relay error: {e}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  AGGREGATION + PERSISTENCE
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
            'source': 'scraper', 'service': 'Ticket Desk v4.0',
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


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPE JOB
# ══════════════════════════════════════════════════════════════════════════════
def run_scrape():
    state.update(running=True, started_at=datetime.now(timezone.utc).isoformat(), last_error=None)
    log.info("=== Ticket Desk v4 scrape started ===")
    try:
        platforms = {}
        for name, fn in [('stubhub', scrape_stubhub), ('seatgeek', scrape_seatgeek),
                         ('tickpick', scrape_tickpick), ('vividseats', scrape_vividseats)]:
            try:
                platforms[name] = fn()
            except Exception as e:
                log.error(f"{name} exception: {e}")
                platforms[name] = None
            time.sleep(1.5 + random.random())

        active = {k: v for k, v in platforms.items() if v}
        log.info(f"Active: {list(active.keys())} ({len(active)}/4)")

        if not active:
            raise Exception("All platforms returned no data.")

        cross_median, cross_floor = cross_stats(active)
        total_inv = sum(p.get('total_count', 0) for p in active.values())
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
        log.info(f"=== Done: {len(active)}/4 | median=${cross_median} | floor=${cross_floor} ===")

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
    h.send_header('Content-Type', 'application/json')
    h.send_header('Content-Length', str(len(body)))
    h.send_header('Access-Control-Allow-Origin', '*')
    h.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    h.send_header('Access-Control-Allow-Headers', 'Content-Type')
    h.end_headers()
    h.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_OPTIONS(self):
        # CORS preflight for browser POSTs
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_POST(self):
        p = urlparse(self.path)
        path = p.path.rstrip('/')
        qs = parse_qs(p.query)

        # ── /relay — receive browser-extracted prices for blocked platforms ──
        # POST JSON: {"token":"bruce11may2026","platform":"seatgeek","floor":1700,"median":1850,"count":22}
        if path == '/relay':
            if qs.get('token', [''])[0] != SCRAPE_TOKEN:
                # Also accept token in body
                pass
            try:
                length = int(self.headers.get('Content-Length', 0))
                body = json.loads(self.rfile.read(length).decode())
            except Exception:
                jresp(self, {'error': 'invalid JSON body'}, 400)
                return

            # Token check (body or querystring)
            token = body.get('token', qs.get('token', [''])[0])
            if token != SCRAPE_TOKEN:
                jresp(self, {'error': 'unauthorized'}, 401)
                return

            platform = body.get('platform', '').lower().strip()
            floor    = body.get('floor')
            median   = body.get('median')
            count    = body.get('count', 10)

            if not platform or not floor or not median:
                jresp(self, {'error': 'required: platform, floor, median'}, 400)
                return

            # Validate price range
            if not (PRICE_MIN <= float(floor) <= PRICE_MAX and PRICE_MIN <= float(median) <= PRICE_MAX):
                jresp(self, {'error': f'prices must be between ${PRICE_MIN} and ${PRICE_MAX}'}, 400)
                return

            # Load existing relay data and upsert
            try:
                with open(RELAY_FILE) as f:
                    relay = json.load(f)
            except:
                relay = {}

            relay[platform] = {
                'floor': int(floor),
                'median': int(median),
                'count': int(count),
                'date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }

            with open(RELAY_FILE, 'w') as f:
                json.dump(relay, f, indent=2)

            log.info(f"Relay received: {platform} | floor=${floor} | median=${median}")
            jresp(self, {'status': 'saved', 'platform': platform, 'floor': floor, 'median': median})

        else:
            jresp(self, {'error': 'not found'}, 404)

    def do_GET(self):
        p = urlparse(self.path)
        path = p.path.rstrip('/')
        qs = parse_qs(p.query)

        if path in ('', '/'):
            jresp(self, {
                'service': 'Ticket Desk v4.0',
                'event': 'Bruce Springsteen · MSG · May 11 2026',
                'status': 'ok',
                'endpoints': {
                    'GET /status': 'scraper health',
                    'GET /scrape?token=X': 'trigger scrape',
                    'GET /data': 'latest JSON data',
                    'POST /relay?token=X': 'push browser-extracted prices (for blocked platforms)',
                }
            })

        elif path == '/status':
            relay_status = {}
            try:
                with open(RELAY_FILE) as f:
                    relay = json.load(f)
                for plat, d in relay.items():
                    relay_status[plat] = {'date': d.get('date'), 'median': d.get('median')}
            except:
                pass
            jresp(self, {
                'service': 'Ticket Desk v4.0',
                'running': state['running'],
                'last_success': state['last_success'],
                'last_error': state['last_error'],
                'started_at': state['started_at'],
                'relay_cache': relay_status,
            })

        elif path == '/scrape':
            if qs.get('token', [''])[0] != SCRAPE_TOKEN:
                jresp(self, {'error': 'unauthorized'}, 401); return
            if state['running']:
                jresp(self, {'status': 'already_running', 'started_at': state['started_at']}); return
            threading.Thread(target=run_scrape, daemon=True).start()
            jresp(self, {'status': 'started'})

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
                jresp(self, {'error': 'no data yet — trigger /scrape first'}, 404)

        # ── /relay-form — a simple browser UI for entering relay prices manually ──
        elif path == '/relay-form':
            token = qs.get('token', [''])[0]
            if token != SCRAPE_TOKEN:
                jresp(self, {'error': 'unauthorized'}, 401); return
            html = f"""<!DOCTYPE html>
<html><head><meta charset=UTF-8><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ticket Desk — Relay Prices</title>
<style>
  body{{font-family:monospace;background:#0a0c0f;color:#c8d4e0;padding:20px;max-width:500px;margin:0 auto}}
  h2{{color:#ffd700;letter-spacing:.1em}}
  label{{font-size:11px;color:#5a6a7a;letter-spacing:.15em;text-transform:uppercase;display:block;margin-top:14px;margin-bottom:4px}}
  input,select{{width:100%;background:#0f1217;border:1px solid #2a3444;color:#e8f0f8;font-family:monospace;font-size:14px;padding:8px;border-radius:3px;box-sizing:border-box}}
  button{{width:100%;margin-top:20px;padding:12px;background:#ffd700;color:#000;border:none;border-radius:3px;font-family:monospace;font-size:13px;font-weight:700;letter-spacing:.15em;cursor:pointer}}
  button:hover{{filter:brightness(1.1)}}
  #msg{{margin-top:14px;padding:10px;border-radius:3px;display:none;font-size:12px}}
  .ok{{background:rgba(0,230,118,.1);color:#00e676;border:1px solid rgba(0,230,118,.3)}}
  .err{{background:rgba(255,68,68,.1);color:#ff4444;border:1px solid rgba(255,68,68,.3)}}
  p{{font-size:12px;color:#5a6a7a;line-height:1.6}}
</style></head>
<body>
<h2>▸ RELAY PRICES</h2>
<p>Open the platform in this browser tab, find Section 224 listings, note the floor and median, then enter them here. This bypasses the server IP block.</p>
<label>Platform</label>
<select id=plat>
  <option value="seatgeek">SeatGeek</option>
  <option value="tickpick">TickPick</option>
  <option value="stubhub">StubHub (override)</option>
  <option value="vividseats">VividSeats (override)</option>
</select>
<label>Section 224 Floor Price ($)</label>
<input type=number id=floor placeholder="e.g. 1700" min=800 max=12000>
<label>Section 224 Median Price ($)</label>
<input type=number id=median placeholder="e.g. 1850" min=800 max=12000>
<label>Approx # of Listings Visible</label>
<input type=number id=count placeholder="e.g. 18" min=1 max=500>
<button onclick=submit()>▶ SAVE RELAY DATA</button>
<div id=msg></div>
<script>
async function submit(){{
  const body = {{
    token: '{SCRAPE_TOKEN}',
    platform: document.getElementById('plat').value,
    floor: parseInt(document.getElementById('floor').value),
    median: parseInt(document.getElementById('median').value),
    count: parseInt(document.getElementById('count').value)||10,
  }};
  if(!body.floor||!body.median){{show('Enter both floor and median prices.','err');return;}}
  try{{
    const r = await fetch('/relay',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(body)}});
    const d = await r.json();
    if(d.status==='saved') show(`✓ Saved: ${{body.platform}} | floor=${{body.floor}} | median=${{body.median}}. Trigger a scrape to incorporate.`,'ok');
    else show(JSON.stringify(d),'err');
  }}catch(e){{show('Error: '+e,'err');}}
}}
function show(msg,cls){{const el=document.getElementById('msg');el.textContent=msg;el.className=cls;el.style.display='block';}}
</script></body></html>"""
            body = html.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        else:
            jresp(self, {'error': 'not found'}, 404)


if __name__ == '__main__':
    log.info(f"Ticket Desk v4.0 on port {PORT}")
    HTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
