IMPORT ML_Core;
IMPORT Files;
IMPORT Std.system.Thorlib;

//Stage2 : local DBSCAN
//At beginning initialize every record with:

// Helper function to find distance between two points
REAL8 distance(SET OF REAL8 d1, SET OF REAL8 d2) := EMBED(C++)
  #include<math.h>
  double sum = 0;
  for(size32_t i=0; i<lenD1/sizeof(double); ++i){
    sum += (*((double*)(d1) + i) - *((double*)(d2) + i))*(*((double*)(d1) + i) - *((double*)(d2) + i));
  }
  return sqrt(sum);
  return lenD1;
ENDEMBED;



//pseudo code for local DBSCAN
STREAMED DATASET(Files.l_stage3) locDBSCAN(STREAMED DATASET(Files.l_stage2) dsIn, //distributed data from stage 1
                                  REAL8 eps,   //distance threshold
                                  UNSIGNED minPts, //the minimum number of points required to form a cluster,
                                  UNSIGNED localNode = Thorlib.node()
                                  ) := FUNCTION
                                  


// Definitions
//remotePoints := dsIn(nodeid <> localNode); //set if_local = FALSE
//localPoints := dsIn(nodeid = localNode);   //set if_local = TRUE
//
// For x in localPoints:
//   N = GetNeighbors(x);
//   If N > minPt:
//     mark x as core point (if_core = TRUE)
//     for y in N:
//       if y is local point
//         if y is core point
//            Union(x, y)
//         else if y is not yet member of any cluster then
//            Union(x, y)
//       if y is remote point:
//         m = GetNeighbors(y);
//         If m > minPt:
//           mark y as core point (if_core = TRUE)
//         Union(x, y)

  neighbours := JOIN(dsIn, dsIn,
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
  
  RETURN DATASET([{1,1,1,1,true,true}],Files.l_stage3);
END;

OUTPUT('');
