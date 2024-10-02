import multiprocessing as mp
import math
from bitarray import bitarray
from database import get_db_connection
from utils import bit_array_to_subset

def generate_and_store_subsets(numbers, start, end, progress_queue):
    conn = get_db_connection()
    c = conn.cursor()
    batch = []
    batch_size = 10000

    # Iterate through the range assigned to this process
    for i in range(start, end):
        # Convert the number asigned to this process to a binary representation of length 30 (length of numbers)
        bit_array = bitarray(bin(i)[2:].zfill(30))
        # Convert the bit array to an array of the numbers
        subset = bit_array_to_subset(bit_array, numbers)
        if len(subset) > 0: 
            # Calculate the sum of the subset
            subset_sum = sum(subset)
            # Add the sum along with the binary representation of the subset to batch.
            batch.append((subset_sum, bit_array.tobytes()))

        # When the batch is full insert it into database
        if len(batch) >= batch_size:
            c.executemany('INSERT INTO subsets VALUES (?, ?)', batch)
            conn.commit()
            progress_queue.put(len(batch))
            batch = []

    # Insert any remaining subsets into database
    if batch:
        c.executemany('INSERT INTO subsets VALUES (?, ?)', batch)
        conn.commit()
        progress_queue.put(len(batch))

    conn.close()

def parallel_generate_subsets(numbers, num_processes, progress_tracker):
    # Calculate size of work for each process
    chunk_size = math.ceil((2**len(numbers)) / num_processes)
    
    # Queue for progress tracker
    progress_queue = mp.Queue()
    # Pool of worker processes
    pool = mp.Pool(processes=num_processes)
    
    jobs = []
    for i in range(num_processes):
        # Calculate start and end for each chunk
        start = i * chunk_size
        end = min((i + 1) * chunk_size, 2**len(numbers))
        # Start each job
        job = pool.apply_async(generate_and_store_subsets, (numbers, start, end, progress_queue))
        jobs.append(job)

    # Monitor progress
    completed = 0
    while completed < (2**len(numbers) - 1):  # Exclude empty set
        if not progress_queue.empty():
            completed += progress_queue.get()
            progress_tracker.update(completed)

    pool.close()
    pool.join()