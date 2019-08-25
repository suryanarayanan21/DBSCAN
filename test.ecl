IMPORT Files;
IMPORT ML_Core;
IMPORT $ AS DBSCAN;

ds := Files.trainRecs;
ML_Core.AppendSeqID(ds, id, recs);
ML_Core.ToField(recs, recsNF);

OUTPUT(DBSCAN.DBSCAN(0.5,6).fit(recsNF),NAMED('Final'));