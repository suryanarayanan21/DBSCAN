IMPORT ML_Core;
IMPORT $ AS DBSCAN;
IMPORT frog_data;

ds := frog_data.numeric_dataset;

ML_Core.AppendSeqID(ds,id,dsID);
ML_Core.ToField(dsID,dsNF);

// Test to confirm that multiple work items are evaluated separately and
// independently

wi1 := PROJECT(dsNF, TRANSFORM(ML_Core.Types.NumericField,SELF.wi := 1, SELF := LEFT));
wi2 := PROJECT(dsNF, TRANSFORM(ML_Core.Types.NumericField,SELF.wi := 2, SELF := LEFT));
test := wi1 + wi2;

clustering := DBSCAN.DBSCAN(0.3,10).fit(test);

OUTPUT(clustering,NAMED('Final'));

// Extract records which have not produced the same results for different wi
// eval must have zero records

eval := JOIN(clustering(wi=1),clustering(wi=2),
             LEFT.id=RIGHT.id and LEFT.clusterId <> RIGHT.clusterId,
             TRANSFORM(RECORDOF(clustering),
                       SELF.wi := LEFT.wi,
                       SELF.id := LEFT.id,
                       SELF.clusterId := LEFT.clusterID));

OUTPUT(eval,NAMED('eval'));
