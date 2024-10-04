import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import torch
import os
import sqlite3
from tqdm import tqdm

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

DB_FILE = 'sum_indices.db'

def create_database():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sum_indices
                 (sum REAL, indices BLOB)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sum ON sum_indices(sum)')
    conn.commit()
    return conn

def insert_or_update(conn, sum_val, index):
    c = conn.cursor()
    c.execute('SELECT indices FROM sum_indices WHERE sum = ?', (sum_val,))
    result = c.fetchone()
    if result:
        indices = result[0] + index.to_bytes(4, 'big')
        c.execute('UPDATE sum_indices SET indices = ? WHERE sum = ?', (indices, sum_val))
    else:
        c.execute('INSERT INTO sum_indices (sum, indices) VALUES (?, ?)', 
                  (sum_val, index.to_bytes(4, 'big')))

def process_chunk(chunk_sums, chunk_index, interval, conn):
    for i, sum_val in tqdm(enumerate(chunk_sums), total=len(chunk_sums), 
                           desc=f"Chunk {chunk_index+1}/63", 
                           unit="sum"):
        global_index = chunk_index * interval + i
        insert_or_update(conn, sum_val.item(), global_index)
    conn.commit()

print("Processing sum tensors...")
conn = create_database()
interval = 17043521

for i in range(63):
    print(f"\nLoading chunk {i+1}/63...")
    chunk_sums = torch.load(f"chunks/{i+1}_sums.pt").to(device)
    process_chunk(chunk_sums, i, interval, conn)
    
    # Optionally, clear CUDA cache to free up memory
    if device.type == 'cuda':
        torch.cuda.empty_cache()

conn.close()
print("\nProcessing complete. Data stored in", DB_FILE)