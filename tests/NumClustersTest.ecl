IMPORT ML_Core;
IMPORT $.^ AS DBSCAN;
IMPORT $.datasets.frog_dataset AS frog_data;

// Test to check the Num_clusters function

ds := frog_data.ds;

ML_Core.AppendSeqID(ds,id,dsID);
ML_Core.ToField(dsID,dsNF);

OUTPUT(DBSCAN.DBSCAN(0.3,10).Num_Clusters(dsNF));
OUTPUT(DBSCAN.DBSCAN(0.3,10).Num_Outliers(dsNF));