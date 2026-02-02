import sqlite3
from datetime import date
import threading
import redis
import atexit

class AnalyticsController:
    
    def __init__(self, location, redisHost, redisPort, visitsFlushThreshold = 50):
        
        self.location = location
        self.analytics_flush_threshold = visitsFlushThreshold
        
        # Init Redis
        self.r = redis.Redis(host=redisHost, port=redisPort, db=0, decode_responses=True)
        
        # if the server is stopped, ensure we write the buffer to the DB
        atexit.register(self._flushPageVisits)
        
        
        
        # Init Database
        with sqlite3.connect(self.location) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pagevisits (
                    date TEXT DEFAULT CURRENT_DATE, 
                    page TEXT, 
                    hits INTEGER DEFAULT 1, 
                    UNIQUE(date, page))
            ''')
        with sqlite3.connect(self.location) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_pagevisits_report_today 
                ON pagevisits (date, hits DESC);
            ''')
        
    def logPageVisit(self, page_path):
        # Incrament the page count
        visit_date = date.today().isoformat()
        hash_key = f"data:analytics:{visit_date}"
        self.r.hincrby(hash_key, page_path, 1)
        
        # Incrament the global counter
        total_hits = self.r.incr("meta:analytics:counter")
        
        # Flush to db if ready
        if total_hits >= self.analytics_flush_threshold:
            t1 = threading.Thread(target=self._flushPageVisits,daemon=True)
            t1.start()
        
    def _flushPageVisits(self):
        # Scan for everything starting with analytics
        cursor, keys = self.r.scan(match="data:analytics:*", count=100)
        if not keys:
            return

        conn = sqlite3.connect(self.location, timeout=30)
        try:
            for key in keys:
                # Atomic Get + Delete
                pipe = self.r.pipeline()
                pipe.hgetall(key)
                pipe.delete(key)
                pipe.set("meta:analytics:counter", 0)
                results = pipe.execute()
                
                data = results[0]
                if not data:
                    continue

                target_date = key.split(":")[-1]
                params = [(target_date, page, count) for page, count in data.items()]
                conn.executemany("""
                    INSERT INTO pagevisits (date, page, hits) VALUES (?, ?, ?)
                    ON CONFLICT(date, page) DO UPDATE SET hits = hits + excluded.hits
                """, params)

            conn.commit()
        finally:
            conn.close()