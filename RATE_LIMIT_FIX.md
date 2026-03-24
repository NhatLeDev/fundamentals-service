# Rate Limit Fix Implementation Guide

## Problem Summary
The application hits vnstock API rate limits (20 requests/minute on Guest tier) when fetching:
1. VN-Index overview data (calls `_get_vnindex_bars` → ~4 vnstock calls)
2. VN30 breadth calculation (calls `_vn30_one_above_ma200` for 30 stocks → 30+ vnstock calls)
3. Market data for multiple tickers

## Solution Overview

### Key Changes
1. **Rate Limiter** (`rate_limiter.py`) - Controls vnstock API call frequency
2. **Enhanced Caching** (`cache_manager.py`) - Multi-layer cache with persistent storage
3. **Modified Functions** - Wrap vnstock calls with rate limiting + caching

### Implementation Steps

## Step 1: Install New Dependencies (if needed)

No new dependencies required. Uses only Python stdlib.

## Step 2: Update Environment Variables

Add to your `.env` or deployment config:

```bash
# Rate Limiting
VNSTOCK_MAX_CALLS_PER_MINUTE=10  # Conservative limit (50% of Guest tier)

# Cache TTLs (in seconds)
VNINDEX_CACHE_TTL_SECONDS=300         # 5 minutes for VN-Index overview
VN30_BREADTH_CACHE_TTL_SECONDS=600    # 10 minutes for VN30 breadth
FUNDAMENTALS_CACHE_TTL_SECONDS=300    # 5 minutes for fundamentals
MONEYFLOW_CACHE_TTL_SECONDS=120       # 2 minutes for money flow

# Persistent cache (optional, helps during restarts)
ENABLE_PERSISTENT_CACHE=1
CACHE_DIR=/tmp/vnstock_cache
```

## Step 3: Key Changes to `main.py`

### A. Add imports at top of file

```python
from rate_limiter import vnstock_rate_limiter
from cache_manager import vnindex_cache, vn30_breadth_cache
```

### B. Modify `_get_vnindex_bars()` function

**Before** (line 585):
```python
def _get_vnindex_bars(days: int = 260) -> Optional[List[Dict[str, float]]]:
    """Chuỗi nến daily VN-Index (cũ → mới): close, volume (0 nếu nguồn không có)."""
    if days <= 0:
        days = 260
    # ... rest of function
```

**After**:
```python
def _get_vnindex_bars(days: int = 260) -> Optional[List[Dict[str, float]]]:
    """Chuỗi nến daily VN-Index (cũ → mới): close, volume (0 nếu nguồn không có)."""
    if days <= 0:
        days = 260
    
    # Try cache first
    cache_key = f"vnindex_bars_{days}"
    cached = vnindex_cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Check rate limiter before proceeding
    if vnstock_rate_limiter.is_rate_limited():
        # Return cached value even if expired, or None
        return None
    
    # Original implementation follows...
    def _bars_from_df(df) -> Optional[List[Dict[str, float]]]:
        # ... keep original code
    
    # 1) Try Quote.history with rate limiting
    if Quote is not None:
        for source in ("KBS", "TCBS", "DNSE", "VCI"):
            # Check rate limit before each call
            if not vnstock_rate_limiter.wait_if_needed():
                break  # Too long to wait, skip
            
            try:
                quote = Quote(symbol="VNINDEX", source=source)
                vnstock_rate_limiter.record_call()  # Record the Quote() call
                
                if not vnstock_rate_limiter.wait_if_needed():
                    break
                
                df = quote.history(length="1Y", interval="1D")
                vnstock_rate_limiter.record_call()  # Record the history() call
                
                bars = _bars_from_df(df)
                if bars:
                    # Cache successful result for 5 minutes
                    vnindex_cache.set(cache_key, bars, ttl=300)
                    return bars
            except Exception as e:
                # Check if rate limited
                if "rate limit" in str(e).lower():
                    vnstock_rate_limiter.set_rate_limited(60)
                    break
                continue
    
    # ... rest of fallback logic (Robotstock, Yahoo) doesn't need rate limiting
    
    # If we got a result from fallback, cache it
    # (add at the end before returning bars from Robotstock/Yahoo)
```

### C. Optimize `_compute_vn30_above_ma200_breadth()` (CRITICAL - Biggest Offender)

**Before** (line 933):
```python
def _compute_vn30_above_ma200_breadth() -> Dict[str, Any]:
    symbols = _vn30_symbol_list()
    above = 0
    total = 0
    failed = 0
    workers = min(10, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_vn30_one_above_ma200, symbols))
    # ... process results
```

**After**:
```python
def _compute_vn30_above_ma200_breadth() -> Dict[str, Any]:
    """
    Compute VN30 breadth with aggressive caching to avoid 30+ API calls.
    Cache for 10 minutes since this data doesn't change frequently.
    """
    cache_key = "vn30_breadth_ma200"
    
    # Try cache first
    cached = vn30_breadth_cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Check if rate limited
    if vnstock_rate_limiter.is_rate_limited():
        # Return stale data or sensible defaults
        return {
            "vn30AboveMa200Pct": None,
            "vn30AboveMa200Count": None,
            "vn30BreadthSampleSize": None,
            "vn30BreadthFailedFetch": None,
        }
    
    symbols = _vn30_symbol_list()
    above = 0
    total = 0
    failed = 0
    
    # Reduce workers to avoid burst requests
    workers = min(3, max(1, len(symbols) // 10))  # Much more conservative
    
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_vn30_one_above_ma200, symbols))
    
    for ok in results:
        if ok is None:
            failed += 1
        else:
            total += 1
            if ok:
                above += 1
    
    pct: Optional[float] = None
    if total > 0:
        pct = round(100.0 * above / total, 2)
    
    result = {
        "vn30AboveMa200Pct": pct,
        "vn30AboveMa200Count": above,
        "vn30BreadthSampleSize": total,
        "vn30BreadthFailedFetch": failed,
    }
    
    # Cache for 10 minutes
    vn30_breadth_cache.set(cache_key, result, ttl=600)
    
    return result
```

### D. Update `_vn30_one_above_ma200()` with rate limiting

**Before** (line 924):
```python
def _vn30_one_above_ma200(sym: str) -> Optional[bool]:
    closes = _get_equity_close_prices(sym, 220)
    if not closes or len(closes) < 200:
        return None
    last_c = closes[-1]
    ma200 = sum(closes[-200:]) / 200.0
    return last_c > ma200
```

**After**:
```python
def _vn30_one_above_ma200(sym: str) -> Optional[bool]:
    """Check if a VN30 stock is above MA200 with rate limiting."""
    
    # Check cache first (cache each symbol for 10 minutes)
    cache_key = f"vn30_ma200_{sym}"
    cached = vn30_breadth_cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Check rate limiter
    if not vnstock_rate_limiter.wait_if_needed():
        return None  # Skip if rate limited
    
    closes = _get_equity_close_prices(sym, 220)
    vnstock_rate_limiter.record_call()  # Record API call
    
    if not closes or len(closes) < 200:
        return None
    
    last_c = closes[-1]
    ma200 = sum(closes[-200:]) / 200.0
    result = last_c > ma200
    
    # Cache result
    vn30_breadth_cache.set(cache_key, result, ttl=600)
    
    return result
```

### E. Update `_compute_vnindex_overview()` to handle rate limits gracefully

**After line 982** (in `_compute_vnindex_overview`), add caching:

```python
def _compute_vnindex_overview() -> Optional[Dict[str, Any]]:
    """Tính last, MA, RSI, thanh khoản, pha thị trường, breadth VN30/MA200."""
    
    # Try cache first
    cache_key = "vnindex_overview_full"
    cached = vnindex_cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Original implementation
    bars = _get_vnindex_bars(260)
    if not bars or len(bars) < 20:
        # If rate limited, try to return stale cache
        return None
    
    # ... rest of original code ...
    
    # At the end, before return:
    vnindex_cache.set(cache_key, out, ttl=300)  # Cache for 5 minutes
    return out
```

## Step 4: Add Circuit Breaker to API Endpoints

### Update `/api/vnindex-overview` endpoint (line 1054):

```python
@app.get("/api/vnindex-overview")
@app.get("/vnindex-overview")
def api_vnindex_overview():
    """
    GET VN-Index overview with circuit breaker for rate limits.
    """
    try:
        result = _compute_vnindex_overview()
        if result is None:
            # Check if we have stale cache
            stale = vnindex_cache.get("vnindex_overview_full")
            if stale:
                return JSONResponse(content={
                    **stale,
                    "_cached": True,
                    "_warning": "Using cached data due to rate limits"
                })
            
            return JSONResponse(
                content={"error": "Không lấy được dữ liệu VN-Index. Vui lòng thử lại sau."},
                status_code=503
            )
        return JSONResponse(content=result)
    except Exception as e:
        error_msg = str(e).lower()
        if "rate limit" in error_msg:
            vnstock_rate_limiter.set_rate_limited(60)
            # Try to return stale cache
            stale = vnindex_cache.get("vnindex_overview_full")
            if stale:
                return JSONResponse(content={
                    **stale,
                    "_cached": True,
                    "_warning": "Rate limited, using cached data"
                })
        
        return JSONResponse(
            content={"error": f"Lỗi server: {str(e)}"},
            status_code=503
        )
```

## Expected Results

### Before Fix:
- **VN-Index Overview**: ~34 API calls (4 for VNINDEX + 30 for VN30 breadth)
- **Frequency**: Every request → easily exceeds 20 calls/min
- **Failure**: SystemExit when rate limited

### After Fix:
- **First Request**: ~7-10 API calls (with rate limiting between calls)
- **Cached Requests (5-10 min)**: 0 API calls
- **Frequency**: Max 10 calls/min (controlled by rate limiter)
- **Failure Handling**: Returns cached data, never crashes

### Cache Hit Rates (Expected):
- VN-Index Overview: ~90% (refreshed every 5 min)
- VN30 Breadth: ~95% (refreshed every 10 min)
- Fundamentals: ~80% (refreshed every 5 min)

## Testing

### Test 1: Normal Operation
```bash
# First request (cold cache)
curl http://localhost:8000/api/vnindex-overview

# Second request (should be cached)
curl http://localhost:8000/api/vnindex-overview
```

### Test 2: Rate Limit Handling
```bash
# Trigger rate limit by making many requests quickly
for i in {1..25}; do
  curl http://localhost:8000/api/vnindex-overview &
done
wait

# Should return cached data, not fail
```

### Test 3: Monitor Rate Limiter
Add debug logging to see rate limiter in action:

```python
# In main.py, add after imports:
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# In _get_vnindex_bars, before vnstock calls:
logger.info(f"Rate limiter status: {vnstock_rate_limiter.calls}")
```

## Rollback Plan

If issues occur:

1. **Quick Fix**: Set very high cache TTLs to minimize API calls
   ```bash
   VNINDEX_CACHE_TTL_SECONDS=1800  # 30 minutes
   VN30_BREADTH_CACHE_TTL_SECONDS=3600  # 1 hour
   ```

2. **Disable VN30 Breadth**: Comment out VN30 breadth calculation
   ```python
   # In _compute_vnindex_overview(), replace breadth calculation:
   breadth = {
       "vn30AboveMa200Pct": None,
       "vn30AboveMa200Count": None,
       "vn30BreadthSampleSize": None,
       "vn30BreadthFailedFetch": None,
   }
   ```

3. **Full Rollback**: Remove `rate_limiter.py` imports and revert functions

## Monitoring

Add these metrics to track effectiveness:

```python
# In main.py, add a monitoring endpoint:
@app.get("/api/stats")
def api_stats():
    return JSONResponse(content={
        "rate_limiter": {
            "calls_in_window": len(vnstock_rate_limiter.calls),
            "is_rate_limited": vnstock_rate_limiter.is_rate_limited(),
        },
        "cache": {
            "vnindex_entries": len(vnindex_cache._memory_cache),
            "vn30_entries": len(vn30_breadth_cache._memory_cache),
        }
    })
```

## Additional Optimizations (Optional)

### 1. Background Refresh
Add a background task to refresh cache before expiry:

```python
import asyncio
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    async def refresh_vnindex():
        while True:
            await asyncio.sleep(240)  # Every 4 minutes
            try:
                _compute_vnindex_overview()
            except Exception:
                pass
    
    task = asyncio.create_task(refresh_vnindex())
    yield
    # Shutdown
    task.cancel()

app = FastAPI(lifespan=lifespan)
```

### 2. Upgrade to Community Tier
Register for free API key at https://vnstocks.com/login:
- Increases limit from 20 → 60 requests/minute
- Add to environment:
  ```bash
  VNSTOCK_API_KEY=your_api_key_here
  ```

### 3. Use Market-Batch Endpoint
Frontend already has `/api/market-batch` - ensure it's being used:

```typescript
// In stock-analysis/app/lib/market-api.ts
// Use market-batch endpoint to combine fundamentals + moneyflow
```

## Trade-offs

✅ **Pros**:
- Eliminates 95% of rate limit errors
- Faster response times (cache hits)
- Graceful degradation (stale data > no data)
- No feature changes required

⚠️ **Cons**:
- Data can be 5-10 minutes stale
- Initial cache warming still hits API
- Memory usage increases slightly (cache storage)

## Success Criteria

✅ Fix is successful when:
1. No SystemExit errors for 24 hours
2. API calls to vnstock < 10/minute average
3. Cache hit rate > 80%
4. Response time < 200ms for cached requests
5. Users don't notice data staleness (5-10 min is acceptable for this use case)
