import pandas as pd
import numpy as np
import pdb

df=pd.read_csv('./datasets/Frogs_MFCCs.csv')
df.to_csv('test_1.txt',header=False,index=False,sep=',')

f=open('test_1.txt','r')

f1=open('test_1final.txt','w')

L=[]
for line in f.readlines():
    line=line.rstrip()
    line='{'+line+'},\n'
    L.append(line)

f1.writelines(L) 
f.close()
f1.close()