import sqlite3

def init_db():
    conn = sqlite3.connect('subsets.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS subsets
                 (sum INTEGER, subset BLOB)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sum ON subsets(sum)')
    conn.commit()
    conn.close()
    print("Database initialized.")

def get_db_connection():
    return sqlite3.connect('subsets.db')