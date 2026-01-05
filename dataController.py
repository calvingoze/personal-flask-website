import sqlite3

class DataController:
    
    def __init__(self, location):
        self.location = location
        conn = sqlite3.connect(self.location)
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
        conn.commit()
        conn.close()
        
    def getBlogPosts(self):
        conn = sqlite3.connect(self.location)
        
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT * FROM blogpost ORDER BY publishedTime DESC")
        rows = cursor.fetchall()
        
        posts = [dict(row) for row in rows]
        conn.close()
        return posts
    
    def getBlogPost(self, url):
        conn = sqlite3.connect(self.location)
        
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM blogpost WHERE url = ?",(url,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def insertMessage(self, name, email, message, ip, metaData):
        conn = sqlite3.connect(self.location)
        cursor = conn.cursor()
        params = (name, email, message, ip, metaData)
        cursor.execute("INSERT INTO contactMessage (name, email, message, ip, metaData) VALUES (?,?,?,?,?);", params)
        conn.commit()
        conn.close()