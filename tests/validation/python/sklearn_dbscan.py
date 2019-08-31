import pdb
import pandas as pd
from sklearn.cluster import DBSCAN
import numpy as np
import matplotlib.pyplot as plt


columns=['MFCCs_ 1', 'MFCCs_ 2', 'MFCCs_ 3', 'MFCCs_ 4', 'MFCCs_ 5', 'MFCCs_ 6','MFCCs_ 7', 'MFCCs_ 8', 'MFCCs_ 9', 'MFCCs_10', 'MFCCs_11', 'MFCCs_12','MFCCs_13', 'MFCCs_14', 'MFCCs_15', 'MFCCs_16', 'MFCCs_17', 'MFCCs_18','MFCCs_19', 'MFCCs_20', 'MFCCs_21', 'MFCCs_22']



# columns=['0','1']

# eps=0.5
# min_samples=6

eps=0.3
min_samples=10

test_set=pd.read_csv(test_dataset)
X=np.array(test_set[columns])


db = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
test_set['labels']=db.labels_
ref=test_set
out=pd.read_csv(hpcc_in)
out=out.sort_values('id').reset_index()
outliers=0
v={}
noc=0
noo=0
acc=0
for i in range(len(out)):
    if out.iloc[i]['id']==out.iloc[i]['clusterid'] and out.iloc[i]['clusterid'] not in v :
        outliers+=1
        noo=1
    else:
        p=out.iloc[i]['clusterid']
        if p in v:
            continue
        v[p]=1
        noc+=1
        t=ref.iloc[i]['labels']
        cnt_df=out[out['clusterid']==p]
        actualdf=ref[ref['labels']==t]
        print('For parent',p,'id in actual',t,'accuracy (hpcc/python)  of ',(len(cnt_df)/len(actualdf))*100,' %')
        acc=acc+len(cnt_df)/len(actualdf)
acc=acc+outliers/len(test_set[test_set['labels']==-1])
print('Outliers accuracy (hpcc/python)',outliers/len(test_set[test_set['labels']==-1]))
print('Number of clusters in hpcc - ',noo+noc,'VS Number of clusters in sklearn - ',len(set(db.labels_)))
print('Average accuracy - ',acc/(noo+noc))
########################## PLOTTING CODE BELOW ####################################

for i in range(len(out)):
    if out.iloc[i]['id']==out.iloc[i]['clusterid'] and out.iloc[i]['clusterid'] not in v :
        out.at[i,'clusterid']=-1

for i in columns:
    out[i]=ref[i]


#label col is the column name having cluster id's
#Utilty function to plot 2-D scatter plot
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
    

# plot(out,'clusterid')
# plot(ref,'labels')

