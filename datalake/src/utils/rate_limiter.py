import time 
import logging

class RateLimiter:
    def __init__(self, max_requests: int, period: int):
        self.max_requests = max_requests
        self.period = period
        self.requests = []
        
        
    def wait(self):
        now = time.time()
        
        self.requests = [t for t in self.requests if now - t < self.period]
        
        if len(self.requests) > self.max_requests:
            sleep_time = self.period - (now - self.requests[0])
            
            logging.info(f"Rate limit rechead. Waiting {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.requests.append(time.time())
        
        