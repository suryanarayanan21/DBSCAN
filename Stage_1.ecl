IMPORT Files;
IMPORT ML_Core;
IMPORT ML_Core.Types AS Types;
IMPORT STD.system.Thorlib;

ds := Files.trainRecs;
ML_Core.AppendSeqID(ds, id, recs);
ML_Core.ToField(recs, recsNF);
OUTPUT(recsNF, NAMED('recsNF'));
l_stage1 := RECORD
    Types.NumericField;
    UNSIGNED nodeId;
    SET OF REAL4 fields;
END;

l_stage2 := RECORD
    Types.NumericField.wi;
    Types.NumericField.id;
    Types.t_RecordID parentID;
    UNSIGNED nodeId;
    SET OF REAL4 fields;
END;
Xnf1 := DISTRIBUTE(recsNF, id);
X0 := PROJECT(Xnf1, TRANSFORM(
                              l_stage1,
                              SELF.fields := [LEFT.value],
                              SELF.nodeId := Thorlib.node(),
                              SELF := LEFT),
                              LOCAL);
X1 := SORT(X0, wi, id, number, LOCAL);
X2 := ROLLUP(X1, TRANSFORM(
                            l_stage1,
                            SELF.fields := LEFT.fields + RIGHT.fields,
                            SELF := LEFT),
                            wi, id,
                            LOCAL);
X3 := PROJECT(X2, TRANSFORM(
                            l_stage2,
                            SELF.parentID := LEFT.id,
                            SELF := LEFT),
                            LOCAL);
OUTPUT(X2, NAMED('X2'));
OUTPUT(X3, NAMED('X3'));
o := TABLE(X2, {nodeid, INTEGER cnt := COUNT(GROUP)}, nodeid);
OUTPUT(o);

// X := DISTRIBUTE(X3, ALL);