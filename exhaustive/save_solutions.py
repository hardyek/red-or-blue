import sqlite3
import numpy as np
import torch
from tqdm import tqdm
import os

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

numbers = torch.tensor([
    9469287, 1271334, 6712289, 8381842, 2455118,
    5992875, 4890524, 1425045, 4681945, 8022348,
    6483223, 8081143, 6192770, 1779814, 4443234,
    8546532, 8224679, 9222657, 9323386, 5363473,
    8541011, 9356134, 7956588, 3696206, 8534543,
    4446344, 1728483, 7451212, 7496930, 1966247
], device=device)

DB_FILE = 'optimised_sum_indices.db'
CHUNK_SIZE = 17043521
BATCH_SIZE = 1000
RESULTS_DIR = 'disjoint_subsets_results'

def index_to_binary_mask(index):
    return np.unpackbits(np.array([index % CHUNK_SIZE], dtype=np.uint32).view(np.uint8))[-30:][::-1]

def are_disjoint(mask1, mask2):
    return not np.any(mask1 & mask2)

def process_batch(batch_data):
    disjoint_pairs = []
    for sum_val, indices_blob in batch_data:
        indices = np.frombuffer(indices_blob, dtype=np.uint32)
        masks = [index_to_binary_mask(idx) for idx in indices]
        
        #Terrible efficiency
        for i in range(len(masks)):
            for j in range(i+1, len(masks)):
                if are_disjoint(masks[i], masks[j]):
                    mask1 = torch.from_numpy(masks[i].copy()).to(device)
                    mask2 = torch.from_numpy(masks[j].copy()).to(device)
                    subset1 = numbers[mask1.bool()].cpu().numpy()
                    subset2 = numbers[mask2.bool()].cpu().numpy()
                    disjoint_pairs.append((sum_val, subset1.tolist(), subset2.tolist()))
    
    return disjoint_pairs

def find_disjoint_subsets():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM sum_indices")
    total_rows = cursor.fetchone()[0]

    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)

    batch_num = 0
    total_pairs = 0

    for i in tqdm(range(0, total_rows, BATCH_SIZE), desc="Processing batches"):
        cursor.execute(f"SELECT sum, indices FROM sum_indices LIMIT {BATCH_SIZE} OFFSET {i}")
        batch_data = cursor.fetchall()
        
        disjoint_pairs = process_batch(batch_data)
        total_pairs += len(disjoint_pairs)

        with open(f'{RESULTS_DIR}/batch_{batch_num}.txt', 'w') as f:
            for sum_val, subset1, subset2 in disjoint_pairs:
                f.write(f"Sum: {sum_val}\n")
                f.write(f"Subset 1: {subset1}\n")
                f.write(f"Subset 2: {subset2}\n")
                f.write("\n")
        
        batch_num += 1

    conn.close()
    return total_pairs

total_disjoint_pairs = find_disjoint_subsets()

print(f"\nFound a total of {total_disjoint_pairs} disjoint subset pairs")
print(f"Results saved in batches in the '{RESULTS_DIR}' directory")