import sqlite3
from datetime import date

class DataController:
    
    def __init__(self, location):
        self.location = location
        
        conn = sqlite3.connect(self.location, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS blogpost (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                body TEXT,
                url TEXT UNIQUE,
                previewText TEXT,
                mainImageUrl TEXT,
                mainImageAlt TEXT,
                metaTags TEXT,
                publishedTime TEXT,
                active INTEGER
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
        conn.commit()
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_blogpost_publishedTime 
            ON blogpost (publishedTime DESC);
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_contactMessage_time 
            ON contactMessage (time DESC);
        ''')
        conn.commit()
        conn.close()
        
    def getBlogPosts(self):
        conn = sqlite3.connect(self.location, timeout=30)
        
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM blogpost WHERE active = true ORDER BY publishedTime DESC")
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