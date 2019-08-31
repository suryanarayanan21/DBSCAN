import pandas as pd
from sklearn.cluster import DBSCAN
import numpy as np
from python_data import *
eps=0.3
min_samples=10


db = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
print('labels',db.labels_)