// IMPORT Module_DBSCAN;
IMPORT $ AS DBSCAN;
IMPORT Files;
IMPORT ML_Core;


ds := Files.testset0;
//Add ID field and transform
//the raw data to NumericField type
ML_Core.AppendSeqID(ds, id, recs);
ML_Core.ToField(recs, recsNF);

indpset := recsNF(number < 3);
OUTPUT(indpset, NAMED('indpset'));
dpset := recsNF(number = 3);
OUTPUT(dpset, NAMED('dpset'));

result := DBSCAN.DBSCAN(1,5).fit(indpset);
OUTPUT(result);
