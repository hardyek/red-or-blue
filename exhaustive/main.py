import multiprocessing as mp
from itertools import combinations
from bitarray import bitarray
from database import get_db_connection
from utils import bit_array_to_subset

def generate_and_store_subsets(numbers, start, end, progress_queue):
    conn = get_db_connection()
    c = conn.cursor()
    batch = []
    batch_size = 10000
    total_processed = 0

    for size in range(1, 20): # The maximum subset size is 19 calculated in combinatorics.ipynb
        for combo in combinations(range(start, end), size):
            bit_array = bitarray('0' * len(numbers))
            for index in combo:
                bit_array[index] = 1
            
            subset = [numbers[i] for i in combo]
            subset_sum = sum(subset)
            batch.append((subset_sum, bit_array.tobytes()))
            
            if len(batch) >= batch_size:
                c.executemany('INSERT INTO subsets VALUES (?, ?)', batch)
                conn.commit()
                progress_queue.put(len(batch))
                total_processed += len(batch)
                batch = []

    if batch:
        c.executemany('INSERT INTO subsets VALUES (?, ?)', batch)
        conn.commit()
        progress_queue.put(len(batch))
        total_processed += len(batch)

    conn.close()
    return total_processed

def parallel_generate_subsets(numbers, num_processes, progress_tracker):
    chunk_size = len(numbers) // num_processes
    progress_queue = mp.Queue()
    pool = mp.Pool(processes=num_processes)
    
    jobs = []
    for i in range(num_processes):
        start = i * chunk_size
        end = len(numbers) if i == num_processes - 1 else (i + 1) * chunk_size
        job = pool.apply_async(generate_and_store_subsets, (numbers, start, end, progress_queue))
        jobs.append(job)

    total_subsets = sum(math.comb(len(numbers), r) for r in range(1, min(MAX_SUBSET_SIZE + 1, len(numbers) + 1)))
    progress_tracker.set_total(total_subsets)

    completed = 0
    while completed < total_subsets:
        if not progress_queue.empty():
            completed += progress_queue.get()
            progress_tracker.update(completed)

    pool.close()
    pool.join()

    print(f"\nTotal subsets generated: {completed}")