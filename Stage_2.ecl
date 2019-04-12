IMPORT Files;
IMPORT Std.system.Thorlib;

//Stage2 : local DBSCAN
//At beginning initialize every record with:

//pseudo code for local DBSCAN
STREAMED DATASET(Files.l_stage3) locDBSCAN(STREAMED DATASET(Files.l_stage2) dsIn, //distributed data from stage 1
                                  REAL8 eps,   //distance threshold
                                  UNSIGNED minPts, //the minimum number of points required to form a cluster,
                                  UNSIGNED localNode = Thorlib.node()
                                  ) := EMBED(C++ : activity)
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
ENDEMBED;

OUTPUT('');