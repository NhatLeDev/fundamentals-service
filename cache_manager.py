"""
Enhanced caching with persistent storage and smart TTL management.
"""
import json
import os
import time
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional, TypeVar

T = TypeVar('T')


class CacheManager:
    """
    Multi-layer cache with memory + optional disk persistence.
    """
    
    def __init__(
        self, 
        default_ttl: int = 300,
        persistent: bool = False,
        cache_dir: Optional[str] = None
    ):
        """
        Initialize cache manager.
        
        Args:
            default_ttl: Default time-to-live in seconds
            persistent: Enable disk persistence
            cache_dir: Directory for persistent cache (default: /tmp/vnstock_cache)
        """
        self.default_ttl = default_ttl
        self.persistent = persistent
        self.cache_dir = Path(cache_dir or os.getenv("CACHE_DIR", "/tmp/vnstock_cache"))
        self._memory_cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()
        
        if self.persistent:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_file_path(self, key: str) -> Path:
        """Get file path for a cache key."""
        # Sanitize key for filesystem
        safe_key = "".join(c if c.isalnum() or c in ".-_" else "_" for c in key)
        return self.cache_dir / f"{safe_key}.json"
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        now = time.time()
        
        # Try memory cache first
        with self._lock:
            entry = self._memory_cache.get(key)
            if entry and entry.get("expires_at", 0) > now:
                return entry.get("value")
            
            # Remove expired entry
            if entry:
                self._memory_cache.pop(key, None)
        
        # Try disk cache if enabled
        if self.persistent:
            try:
                file_path = self._get_file_path(key)
                if file_path.exists():
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    expires_at = data.get("expires_at", 0)
                    if expires_at > now:
                        value = data.get("value")
                        # Promote to memory cache
                        with self._lock:
                            self._memory_cache[key] = {
                                "value": value,
                                "expires_at": expires_at
                            }
                        return value
                    else:
                        # Remove expired file
                        file_path.unlink(missing_ok=True)
            except Exception:
                pass
        
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (uses default if None)
        """
        ttl = ttl if ttl is not None else self.default_ttl
        expires_at = time.time() + max(1, ttl)
        
        entry = {
            "value": value,
            "expires_at": expires_at
        }
        
        # Store in memory
        with self._lock:
            self._memory_cache[key] = entry
        
        # Store on disk if enabled
        if self.persistent:
            try:
                file_path = self._get_file_path(key)
                with open(file_path, 'w') as f:
                    json.dump(entry, f)
            except Exception:
                pass
    
    def delete(self, key: str) -> None:
        """Delete a key from cache."""
        with self._lock:
            self._memory_cache.pop(key, None)
        
        if self.persistent:
            try:
                file_path = self._get_file_path(key)
                file_path.unlink(missing_ok=True)
            except Exception:
                pass
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._memory_cache.clear()
        
        if self.persistent:
            try:
                for file_path in self.cache_dir.glob("*.json"):
                    file_path.unlink(missing_ok=True)
            except Exception:
                pass
    
    def cleanup_expired(self) -> int:
        """
        Remove expired entries from cache.
        
        Returns:
            Number of entries removed
        """
        now = time.time()
        removed = 0
        
        # Clean memory cache
        with self._lock:
            expired_keys = [
                k for k, v in self._memory_cache.items()
                if v.get("expires_at", 0) <= now
            ]
            for k in expired_keys:
                self._memory_cache.pop(k, None)
                removed += 1
        
        # Clean disk cache
        if self.persistent:
            try:
                for file_path in self.cache_dir.glob("*.json"):
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                        if data.get("expires_at", 0) <= now:
                            file_path.unlink(missing_ok=True)
                            removed += 1
                    except Exception:
                        pass
            except Exception:
                pass
        
        return removed


# Global cache instances with different TTLs
vnindex_cache = CacheManager(
    default_ttl=int(os.getenv("VNINDEX_CACHE_TTL_SECONDS", "300")),  # 5 minutes
    persistent=os.getenv("ENABLE_PERSISTENT_CACHE", "0") == "1"
)

vn30_breadth_cache = CacheManager(
    default_ttl=int(os.getenv("VN30_BREADTH_CACHE_TTL_SECONDS", "600")),  # 10 minutes
    persistent=os.getenv("ENABLE_PERSISTENT_CACHE", "0") == "1"
)

fundamentals_cache = CacheManager(
    default_ttl=int(os.getenv("FUNDAMENTALS_CACHE_TTL_SECONDS", "300")),  # 5 minutes
    persistent=os.getenv("ENABLE_PERSISTENT_CACHE", "0") == "1"
)
