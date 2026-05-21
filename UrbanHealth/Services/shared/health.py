import time
import threading
from typing import Dict, Any

class HealthMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.request_count = 0
        self.error_count = 0
        self.lock = threading.Lock()

    def increment_request(self):
        with self.lock:
            self.request_count += 1

    def increment_error(self):
        with self.lock:
            self.error_count += 1

    def get_status(self) -> Dict[str, Any]:
        with self.lock:
            uptime_seconds = int(time.time() - self.start_time)
            error_rate = (self.error_count / self.request_count * 100) if self.request_count > 0 else 0
            return {
                "status": "healthy" if error_rate < 10 else "degraded" if error_rate < 20 else "unhealthy",
                "uptime_seconds": uptime_seconds,
                "requests_processed": self.request_count,
                "errors": self.error_count,
                "error_rate_percent": round(error_rate, 2)
            }
