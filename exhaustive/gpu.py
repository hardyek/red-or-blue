import torch

def gpu_calculate_sums(subsets, numbers,device):
    numbers_tensor = torch.tensor(numbers, dtype=torch.int64, device=device)
    subsets_tensor = torch.tensor(subsets, dtype=torch.int64, device=device)
    return torch.sum(subsets_tensor * numbers_tensor, dim=1).cpu().numpy()