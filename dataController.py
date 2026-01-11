import sqlite3
import threading
import queue
import atexit
from datetime import date

class DataController:
    
    def __init__(self, location, visitsFlushThreshold = 20):
        self.location = location
        
        # Class scoped vars to deal with analytics
        # analytics are designed to be written to the DB in async batches for max performance and minimal db writes 
        self.analytics_buffer = {}
        self.total_hits = 0
        self.analytics_task_queue = queue.Queue()
        self.analytics_flush_threshold = visitsFlushThreshold
        
        # Start ONE permanent background worker for analytics
        self.worker = threading.Thread(target=self._backgroundWorker, daemon=True)
        self.worker.start()
        
        # if the server is stopped, ensure we write the buffer to the DB
        atexit.register(self._forceAnalyticsFlush)
        
        conn = sqlite3.connect(self.location, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS blogpost (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                body TEXT,
                url TEXT NOT NULL UNIQUE,
                previewText TEXT,
                mainImageUrl TEXT,
                metaTags TEXT,
                publishedTime INTEGER
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contactMessage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time TEXT DEFAULT CURRENT_TIMESTAMP,
                name TEXT,
                email TEXT,
                message TEXT,
                ip TEXT,
                metaData BLOB
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pagevisits (
                date TEXT DEFAULT CURRENT_DATE,
                page TEXT,
                hits INTEGER DEFAULT 1,
                UNIQUE(date, page)
            );
        ''')
        conn.commit()
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_report_today 
            ON pagevisits (date, hits DESC);
        ''')
        conn.commit()
        conn.close()
        
    def getBlogPosts(self):
        conn = sqlite3.connect(self.location, timeout=30)
        
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM blogpost ORDER BY publishedTime DESC")
        rows = cursor.fetchall()
        
        posts = [dict(row) for row in rows]
        conn.close()
        return posts
    
    def getBlogPost(self, url):
        conn = sqlite3.connect(self.location, timeout=30)
        
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM blogpost WHERE url = ?",(url,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def insertMessage(self, name, email, message, ip, metaData):
        conn = sqlite3.connect(self.location, timeout=30)
        cursor = conn.cursor()
        params = (name, email, message, ip, metaData)
        cursor.execute("INSERT INTO contactMessage (name, email, message, ip, metaData) VALUES (?,?,?,?,?);", params)
        conn.commit()
        conn.close()
        
    def logPageVisit(self, page_path):
        visit_date = date.today().isoformat()
        self.analytics_task_queue.put((visit_date, page_path))
        
    def _backgroundWorker(self):
        while True:
            
            try:
                # Wait for an item from the queue
                visit_date, page_path = self.analytics_task_queue.get(timeout=60)
                
                # Update local buffer
                key = (visit_date, page_path)
                self.analytics_buffer[key] = self.analytics_buffer.get(key, 0) + 1
                self.total_hits += 1
                
                # Flush if threshold reached
                if self.total_hits >= self.analytics_flush_threshold:
                    self._writePageVisits(self.analytics_buffer)
                    self.analytics_buffer.clear()
                    self.total_hits = 0
                
                self.analytics_task_queue.task_done()
                
            except queue.Empty:
                if len(self.analytics_buffer) > 0:
                    self._writePageVisits(self.analytics_buffer)
                    self.analytics_buffer.clear()
                    self.total_hits = 0
    
    def _writePageVisits(self, data):
        conn = sqlite3.connect(self.location, timeout=30)
        try:
            params = [(d, p, h) for (d, p), h in data.items()]
            conn.executemany("""
                INSERT INTO pagevisits (date, page, hits) VALUES (?, ?, ?)
                ON CONFLICT(date, page) DO UPDATE SET hits = hits + excluded.hits
            """, params)
            conn.commit()
        finally:
            conn.close()
            
    def _forceAnalyticsFlush(self):
        self._writePageVisits(self.analytics_buffer)
        self.analytics_buffer.clear()
        self.total_hits = 0