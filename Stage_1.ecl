IMPORT Files;
IMPORT ML_Core;
IMPORT ML_Core.Types AS Types;
IMPORT STD.system.Thorlib;

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
//Transform to 1_stage1
X0 := PROJECT(Xnf1, TRANSFORM(
                              Files.l_stage1,
                              SELF.fields := [LEFT.value],
                              SELF.nodeId := Thorlib.node(),
                              SELF := LEFT),
                              LOCAL);
X1 := SORT(X0, wi, id, number, LOCAL);
X2 := ROLLUP(X1, TRANSFORM(
                            Files.l_stage1,
                            SELF.fields := LEFT.fields + RIGHT.fields,
                            SELF := LEFT),
                            wi, id,
                            LOCAL);
//Transform to l_stage2
X3 := PROJECT(X2, TRANSFORM(
                            Files.l_stage2,
                            SELF.parentID := LEFT.id,
                            SELF := LEFT),
                            LOCAL);
OUTPUT(X2, NAMED('X2'));
OUTPUT(X3, NAMED('X3'));
// o := TABLE(X2, {nodeid, INTEGER cnt := COUNT(GROUP)}, nodeid);
// OUTPUT(o);

//Braodcast for local clustering.
X := DISTRIBUTE(X3, ALL);