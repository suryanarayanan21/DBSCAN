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

REAL8 distance(SET OF REAL8 d1, SET OF REAL8 d2) := EMBED(C++)
  #include<math.h>
  double sum = 0;
  for(size32_t i=0; i<lenD1/sizeof(double); ++i){
    sum += (*((double*)(d1) + i) - *((double*)(d2) + i))*(*((double*)(d1) + i) - *((double*)(d2) + i));
  }
  return sqrt(sum);
  return lenD1;
ENDEMBED;

REAL8 eps := 0.5;
INTEGER minPts := 6;

neighbours := JOIN(X, X,
                   LEFT.nodeId = Thorlib.node() and
                   LEFT.id <> RIGHT.id and
                   LEFT.wi = RIGHT.wi,
                   TRANSFORM({RECORDOF(Files.l_stage2), SET OF INTEGER neighbour},
                             SELF.neighbour := IF(distance(LEFT.fields,RIGHT.fields) <= eps, [RIGHT.id], []),
                             SELF.if_local := true;
                             SELF := LEFT), LOCAL);
                               
neighbours1 := ROLLUP(SORT(neighbours,wi,id),TRANSFORM(RECORDOF(neighbours),
                                                       SELF.neighbour := LEFT.neighbour + RIGHT.neighbour,
                                                       SELF.if_core := IF(COUNT(SELF.neighbour) >= minPts, true, false),
                                                       SELF := LEFT),
                                                       wi, id, LOCAL);
                                                       
OUTPUT(neighbours1, NAMED('neighbours'));
