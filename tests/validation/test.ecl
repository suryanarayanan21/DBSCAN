IMPORT Files;
IMPORT ML_Core;
IMPORT $ AS DBSCAN;
IMPORT $.frogs;
ds := frogs.ds;
ML_Core.AppendSeqID(ds, id, recs);
ML_Core.ToField(recs, recsNF);

OUTPUT(DBSCAN.DBSCAN(0.3,10).fit(recsNF),NAMED('Final'));