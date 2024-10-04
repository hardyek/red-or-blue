import sqlite3
import os
from tqdm import tqdm

# Original and new database file names
ORIGINAL_DB = 'sum_indices.db'
OPTIMIZED_DB = 'optimised_sum_indices.db'

def optimize_database():
    # Connect to the original database
    conn_original = sqlite3.connect(ORIGINAL_DB)
    cursor_original = conn_original.cursor()

    # Create a new database
    if os.path.exists(OPTIMIZED_DB):
        os.remove(OPTIMIZED_DB)  # Remove if it already exists
    conn_optimized = sqlite3.connect(OPTIMIZED_DB)
    cursor_optimized = conn_optimized.cursor()

    # Create the table in the new database
    cursor_optimized.execute('''
        CREATE TABLE sum_indices (
            sum REAL,
            indices BLOB
        )
    ''')
    cursor_optimized.execute('CREATE INDEX idx_sum ON sum_indices(sum)')

    # Get the total count of rows for the progress bar
    cursor_original.execute("SELECT COUNT(*) FROM sum_indices")
    total_rows = cursor_original.fetchone()[0]

    # Copy data with multiple subsets
    cursor_original.execute("SELECT sum, indices FROM sum_indices")
    
    batch_size = 10000
    inserted_count = 0

    with tqdm(total=total_rows, desc="Optimizing database") as pbar:
        while True:
            rows = cursor_original.fetchmany(batch_size)
            if not rows:
                break

            multi_subset_rows = [(sum_val, indices) for sum_val, indices in rows if len(indices) > 4]
            if multi_subset_rows:
                cursor_optimized.executemany("INSERT INTO sum_indices (sum, indices) VALUES (?, ?)", multi_subset_rows)
                inserted_count += len(multi_subset_rows)

            pbar.update(len(rows))

    conn_optimized.commit()

    # Print statistics
    print(f"Original database rows: {total_rows}")
    print(f"Optimized database rows: {inserted_count}")
    print(f"Reduction: {(1 - inserted_count/total_rows)*100:.2f}%")

    # Close connections
    conn_original.close()
    conn_optimized.close()

    print(f"Optimized database created: {OPTIMIZED_DB}")

if __name__ == "__main__":
    optimize_database()