class ProgressTracker:
    def __init__(self, total, description):
        self.total = total
        self.description = description
        self.current = 0

    def update(self, value):
        self.current = value
        percentage = (self.current / self.total) * 100
        print(f"\r{self.description} Progress: {percentage:.2f}% ({self.current}/{self.total})", end="")

def bit_array_to_subset(bit_array, numbers):
    return [num for i, num in enumerate(numbers) if bit_array[i]]