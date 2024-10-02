import torch
import time
import csv
import os

def save_solution_to_csv(red_subset, blue_subset, filename='solutions.csv'):
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(['Red Subset', 'Blue Subset', 'Sum','TimeFound'])
        writer.writerow([str(red_subset), str(blue_subset), sum(red_subset), time.time()])

def probabilistic_solver(numbers, max_iterations=100000000, batch_size=32768, max_solutions=10):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    numbers_tensor = torch.tensor(numbers, device=device, dtype=torch.float32)
    n = len(numbers)
    solutions_found = 0

    for iteration in range(0, max_iterations):
        # Generate 2 binary vectors of length n to represent the two subsets (red/blue)
        # 0 : Do not include item at this index in the subset
        # 1 : Include item at this index in the subset
        subsets = torch.randint(0, 2, (batch_size, n), dtype=torch.float32, device=device)

        # Matrix multiply subsets with the numbers tensor to go from binary vectors to summed up subsets
        # (n,1) vector matrix multiplied with a (1,n) vector will result in the sum of multipled elements.
        subset_sums = torch.mm(subsets, numbers_tensor.unsqueeze(1)).squeeze()
        
        # Calculate the difference between the subsets' sums
        sum_diff = subset_sums.unsqueeze(1) - subset_sums.unsqueeze(0)
        # Check if they are equal (difference is 0)
        equal_sum_pairs = torch.nonzero(torch.abs(sum_diff) < 1.0, as_tuple=False)
        
        # Verify that the solution is correct
        for i, j in equal_sum_pairs:
            if i >= j:
                continue
            
            subset1 = subsets[i]
            subset2 = subsets[j]
            
            if torch.sum(subset1 * subset2) == 0:
                red_subset = tuple(numbers[k] for k in range(n) if subset1[k] == 1)
                blue_subset = tuple(numbers[k] for k in range(n) if subset2[k] == 1)
                
                if verify_solution(numbers, red_subset, blue_subset):
                    save_solution_to_csv(red_subset, blue_subset)
                    solutions_found += 1
                    print(f"Found solution {solutions_found}:")
                    print("Red subset:", red_subset)
                    print("Blue subset:", blue_subset)
                    print("Sum:", sum(red_subset))
                    print()

                    if solutions_found >= max_solutions:
                        return

        if iteration % 100 == 0:
            print(f"Processed {iteration} iterations...")

    if solutions_found == 0:
        print("No solutions found.")

def verify_solution(numbers, red_subset, blue_subset):
    if not red_subset or not blue_subset: # Not empty subset (this is a solution though)
        return False
    if abs(sum(red_subset) - sum(blue_subset)) > 1: # Subsets' sums are equal
        return False
    if set(red_subset) & set(blue_subset): # There is no overlap in the sets (no double colouring)
        return False 
    if not (set(red_subset) | set(blue_subset)).issubset(set(numbers)): # All the numbers come from the original set
        return False
    return True

if __name__ == "__main__":
    numbers = [
        9469287, 1271334, 6712289, 8381842, 2455118,
        5992875, 4890524, 1425045, 4681945, 8022348,
        6483223, 8081143, 6192770, 1779814, 4443234,
        8546532, 8224679, 9222657, 9323386, 5363473,
        8541011, 9356134, 7956588, 3696206, 8534543,
        4446344, 1728483, 7451212, 7496930, 1966247
    ]

    start_time = time.time()
    probabilistic_solver(numbers, max_solutions=1000000)
    end_time = time.time()

    print(f"Time taken: {end_time - start_time:.2f} seconds")
    print("Solutions have been saved to 'solutions.csv'")