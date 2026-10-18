import base64
import threading
import time
from collections import defaultdict, deque

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---- Your assigned values ----
TOTAL_ORDERS = 60        # T
RATE_LIMIT = 16          # R requests ...
WINDOW_SECONDS = 10      # ... per 10 seconds

app = FastAPI()
lock = threading.Lock()

# ---------- 1. Idempotent POST ----------
orders_by_key = {}       # Idempotency-Key -> order
next_order_id = 1


@app.post("/orders")
async def create_order(request: Request):
    global next_order_id
    key = request.headers.get("Idempotency-Key")
    if not key:
        return JSONResponse({"error": "Idempotency-Key header required"}, status_code=400)

    try:
        body = await request.json()
    except Exception:
        body = {}

    with lock:
        if key in orders_by_key:                 # seen before -> same order
            return JSONResponse(orders_by_key[key], status_code=201)
        order = {"id": next_order_id, "details": body}
        next_order_id += 1
        orders_by_key[key] = order
    return JSONResponse(order, status_code=201)


# ---------- 2. Cursor pagination ----------
def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(f"offset:{offset}".encode()).decode()


def decode_cursor(cursor: str) -> int:
    try:
        text = base64.urlsafe_b64decode(cursor.encode()).decode()
        return max(0, int(text.split(":")[1]))
    except Exception:
        return 0


@app.get("/orders")
def list_orders(limit: int = 10, cursor: str = None):
    limit = max(1, min(limit, 100))
    start = decode_cursor(cursor) if cursor else 0   # how many already served
    ids = list(range(1, TOTAL_ORDERS + 1))
    page = ids[start:start + limit]
    end = start + len(page)
    next_cursor = encode_cursor(end) if end < TOTAL_ORDERS else None
    return {"items": [{"id": i} for i in page], "next_cursor": next_cursor}


# ---------- 3. Per-client rate limiting ----------
hits = defaultdict(deque)    # client id -> timestamps of recent requests


@app.middleware("http")
async def rate_limiter(request: Request, call_next):
    client_id = request.headers.get("X-Client-Id")
    if client_id and request.method != "OPTIONS":
        now = time.time()
        with lock:
            q = hits[client_id]
            while q and now - q[0] >= WINDOW_SECONDS:   # forget old hits
                q.popleft()
            if len(q) >= RATE_LIMIT:
                retry_after = max(1, int(WINDOW_SECONDS - (now - q[0])) + 1)
                return JSONResponse(
                    {"error": "Too Many Requests"},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
            q.append(now)
    return await call_next(request)


# ---------- CORS (added LAST so it wraps everything, even 429s) ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)
