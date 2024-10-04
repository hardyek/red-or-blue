import torch

# Define the numbers set as well as the interval (chunk size)

numbers = [
        9469287, 1271334, 6712289, 8381842, 2455118,
        5992875, 4890524, 1425045, 4681945, 8022348,
        6483223, 8081143, 6192770, 1779814, 4443234,
        8546532, 8224679, 9222657, 9323386, 5363473,
        8541011, 9356134, 7956588, 3696206, 8534543,
        4446344, 1728483, 7451212, 7496930, 1966247
    ]
numbers_tensor = torch.tensor(numbers)

interval = 17043521

# Converts a range of numbers to a matrix containing all of these numbers represented in binary
def range_to_binmatrix(start, end):
    numbers = torch.arange(start, end, dtype=torch.int64)
    powers_of_two = 2 ** torch.arange(29, -1, -1, dtype=torch.int64)
    binary_matrix = (numbers.unsqueeze(1) & powers_of_two).ne(0).int()
    return binary_matrix

for i in range(0, 63):
    # Calculate the starting and ending numbers for the range in this chunk
    start = i*interval
    end = (i+1)*interval

    binary_matrix = range_to_binmatrix(start, end)
    # Use matrix multiplication to quickly calculate the sums for each subset
    subset_sums = torch.mm(binary_matrix.float(),numbers_tensor.unsqueeze(1).float()).squeeze()

    # Save the chunk and log
    torch.save(binary_matrix, f"chunks/{i+1}_binmatrix.pt") 
    torch.save(subset_sums, f"chunks/{i+1}_sums.pt")
    print(f"Completed chunk {i+1}/63 ||| {(i+1)/63 * 100:.2f}%")