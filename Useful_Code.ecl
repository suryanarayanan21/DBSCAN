IMPORT Files;
IMPORT ML_Core;
IMPORT ML_Core.Types AS Types;
IMPORT STD.system.Thorlib;

/**
  * Test data distribution
  */
//Load raw data
//**make sure the id of the data is sequential starting from 1
ds := Files.trainRecs;
//Add ID field and transform
//the raw data to NumericField type
ML_Core.AppendSeqID(ds, id, recs);
ML_Core.ToField(recs, recsNF);
OUTPUT(recsNF, NAMED('recsNF'));
//Evenly distribute the data
Xnf1 := DISTRIBUTE(recsNF, id);

a := PROJECT(Xnf1, TRANSFORM({UNSIGNED4 rid, RECORDOF(LEFT)}, SELF.rid := thorlib.node(), SELF := LEFT));
b := TABLE(a, {rid, cnt := COUNT(GROUP)}, rid);
OUTPUT(b);