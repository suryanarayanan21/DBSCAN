import pdb
import pandas as pd
from sklearn.cluster import DBSCAN
import numpy as np


#test dataset
test_dataset='test_1.csv'
#hpcc output csv
hpcc_in = 'test_1_debug.csv'
#columns in main dataset
#columns=['A','B','C']



columns=['0','1']

eps=0.3
min_samples=10

test_set=pd.read_csv(test_dataset)
X=np.array(test_set[columns])


db = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
test_set['labels']=db.labels_

ref=test_set
out=pd.read_csv(hpcc_in)
out=out.sort_values('id').reset_index()
pdb.set_trace()
outliers=0
v={}
for i in range(len(out)):
    if out.iloc[i]['id']==out.iloc[i]['parentid'] and out.iloc[i]['parentid'] not in v :
        outliers+=1
    else:
        p=out.iloc[i]['parentid']
        if p in v:
            continue
        v[p]=1
        t=ref.iloc[i]['labels']
        cnt_df=out[out['parentid']==p]
        actualdf=ref[ref['labels']==t]
        print('For parent',p,'id in actual',t,'accuracy (hpcc/python)  of ',len(cnt_df)/len(actualdf))
print('Outliers accuracy (hpcc/python)',outliers/len(test_set[test_set['labels']==-1]))

import matplotlib.pyplot as plt

for i in range(len(out)):
    if out.iloc[i]['id']==out.iloc[i]['parentid'] and out.iloc[i]['parentid'] not in v :
        out.at[i,'parentid']=-1

for i in columns:
    out[i]=ref[i]
pdb.set_trace()
def plot(df,label_col):
    labels=np.array(df[label_col])
    core_samples_mask = np.zeros_like(labels, dtype=bool)
    unique_labels = set(df[label_col])
    colors = [plt.cm.Spectral(each) for each in np.linspace(0, 1, len(unique_labels))]
    X=np.array(df[columns])

    for k, col in zip(unique_labels, colors):
        if k == -1:
        # Black used for noise.
            col = [0, 0, 0, 1]

        class_member_mask = (labels == k)

        xy = X[class_member_mask & core_samples_mask]
        plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col),markeredgecolor='k', markersize=20)

        xy = X[class_member_mask & ~core_samples_mask]
        plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=tuple(col),markeredgecolor='k', markersize=6)
    plt.title('Estimated number of clusters:'+str(len(unique_labels))+' for the metric min_samples:'+ str(min_samples)+' eps:'+str(eps))
    plt.show()
    

plot(out,'parentid')
plot(ref,'labels')

