"""
Rate limiter and request throttling for vnstock API calls.
Prevents hitting the 20 requests/minute limit on Guest tier.
"""
import time
from collections import deque
from threading import Lock
from typing import Callable, TypeVar, Optional, Any

T = TypeVar('T')


class RateLimiter:
    """
    Token bucket rate limiter to prevent exceeding vnstock API limits.
    
    Default: 10 requests per minute (50% of Guest tier limit for safety margin).
    """
    
    def __init__(self, max_calls: int = 10, time_window: int = 60):
        """
        Initialize rate limiter.
        
        Args:
            max_calls: Maximum number of calls allowed in time_window
            time_window: Time window in seconds
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls: deque[float] = deque()
        self.lock = Lock()
        self._rate_limited_until = 0.0
        
    def set_rate_limited(self, seconds: int = 60):
        """Mark as rate limited for a duration."""
        with self.lock:
            self._rate_limited_until = time.time() + seconds
    
    def is_rate_limited(self) -> bool:
        """Check if currently in rate-limited cooldown."""
        with self.lock:
            return time.time() < self._rate_limited_until
    
    def can_proceed(self) -> tuple[bool, float]:
        """
        Check if a call can proceed.
        
        Returns:
            (can_proceed, wait_time_seconds)
        """
        if self.is_rate_limited():
            wait_time = self._rate_limited_until - time.time()
            return False, max(0, wait_time)
        
        with self.lock:
            now = time.time()
            
            # Remove calls outside the time window
            while self.calls and self.calls[0] < now - self.time_window:
                self.calls.popleft()
            
            if len(self.calls) < self.max_calls:
                return True, 0.0
            
            # Calculate wait time until oldest call expires
            wait_time = self.calls[0] + self.time_window - now
            return False, max(0, wait_time)
    
    def record_call(self):
        """Record a successful API call."""
        with self.lock:
            self.calls.append(time.time())
    
    def wait_if_needed(self) -> bool:
        """
        Wait if rate limit would be exceeded.
        
        Returns:
            True if proceeded, False if in cooldown period (> 30s wait)
        """
        can_proceed, wait_time = self.can_proceed()
        
        if can_proceed:
            return True
        
        # If wait time is too long, return False (caller should use cache)
        if wait_time > 30:
            return False
        
        # Wait for a reasonable time
        time.sleep(wait_time + 0.1)  # Add small buffer
        return True
    
    def execute_with_limit(
        self, 
        func: Callable[[], T],
        fallback: Optional[Callable[[], T]] = None,
        max_wait: float = 30.0
    ) -> Optional[T]:
        """
        Execute a function with rate limiting.
        
        Args:
            func: Function to execute
            fallback: Optional fallback function if rate limited
            max_wait: Maximum seconds to wait (default 30)
            
        Returns:
            Result of func or fallback, or None if both fail
        """
        can_proceed, wait_time = self.can_proceed()
        
        if not can_proceed:
            if wait_time > max_wait:
                # Too long to wait, use fallback
                if fallback:
                    return fallback()
                return None
            
            # Wait briefly
            time.sleep(wait_time + 0.1)
        
        try:
            result = func()
            self.record_call()
            return result
        except Exception as e:
            # Check if it's a rate limit error
            error_msg = str(e).lower()
            if "rate limit" in error_msg or "too many" in error_msg:
                self.set_rate_limited(60)
                if fallback:
                    return fallback()
            raise


# Global rate limiter instance for vnstock API calls
vnstock_rate_limiter = RateLimiter(max_calls=10, time_window=60)


def with_rate_limit(fallback: Optional[Callable[[], T]] = None):
    """
    Decorator to add rate limiting to vnstock API calls.
    
    Usage:
        @with_rate_limit(fallback=lambda: cached_value)
        def fetch_data():
            return vnstock_api_call()
    """
    def decorator(func: Callable[[], T]) -> Callable[[], Optional[T]]:
        def wrapper(*args, **kwargs) -> Optional[T]:
            def call():
                return func(*args, **kwargs)
            
            return vnstock_rate_limiter.execute_with_limit(
                call, 
                fallback=fallback,
                max_wait=30.0
            )
        return wrapper
    return decorator
