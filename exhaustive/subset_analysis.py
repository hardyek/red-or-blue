from bitarray import bitarray
from database import get_db_connection
from utils import bit_array_to_subset

def find_equal_sum_subsets(numbers, batch_size=1000000):
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute('SELECT MIN(sum), MAX(sum) FROM subsets')
    min_sum, max_sum = c.fetchone()
    total_sums = max_sum - min_sum + 1
    
    solutions = []
    for current_sum in range(min_sum, max_sum + 1):
        c.execute('SELECT subset FROM subsets WHERE sum = ?', (current_sum,))
        matching_subsets = c.fetchall()
        
        if len(matching_subsets) > 1:
            for i, subset1 in enumerate(matching_subsets):
                for subset2 in matching_subsets[i+1:]:
                    bit_array1 = bitarray()
                    bit_array2 = bitarray()
                    bit_array1.frombytes(subset1[0])
                    bit_array2.frombytes(subset2[0])
                    if not (bit_array1 & bit_array2).any():
                        red_subset = bit_array_to_subset(bit_array1, numbers)
                        blue_subset = bit_array_to_subset(bit_array2, numbers)
                        solutions.append((red_subset, blue_subset, current_sum))
                        
        if len(solutions) >= batch_size:
            yield solutions
            solutions = []
        
        if current_sum % 1000 == 0:
            progress = (current_sum - min_sum + 1) / total_sums * 100
            print(f"Analysis progress: {progress:.2f}%")
    
    if solutions:
        yield solutions
    
    conn.close()