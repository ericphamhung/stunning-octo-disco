from torch.utils.data import Dataset, DataLoader
import argparse
import torch
import torch.utils.data
from torch import nn, optim
from torch.nn import functional as F
import pandas as pd


class VAE(nn.Module):
    def __init__(self, x_in):
        super(VAE, self).__init__()

        self.fc1 = nn.Linear(x_in, 400)
        self.fc21 = nn.Linear(400, 20)
        self.fc22 = nn.Linear(400, 20)
        self.fc3 = nn.Linear(20, 400)
        self.fc4 = nn.Linear(400, x_in)

    def encode(self, x):
        h1 = F.relu(self.fc1(x))
        return self.fc21(h1), self.fc22(h1)

    def reparameterize(self, mu, logvar):
        if self.training:
            std = torch.exp(0.5*logvar)
            eps = torch.randn_like(std)
            return eps.mul(std).add_(mu)
        else:
            return mu

    def decode(self, z):
        h3 = F.relu(self.fc3(z))
        return F.sigmoid(self.fc4(h3))

    def forward(self, x):
        mu, logvar = self.encode(x)
        z = self.reparameterize(mu, logvar)
        return self.decode(z), mu, logvar

class DataLoader_Bad(Dataset):
    """Face Landmarks dataset."""

    def __init__(self, csv_file, cols, transform=None):
        
        self.data = pd.read_csv(csv_file)[cols]
        

    def __len__(self):
        return self.data.shape[0]

    def __getitem__(self, idx):
        return copy(self.data.ix[idx])

