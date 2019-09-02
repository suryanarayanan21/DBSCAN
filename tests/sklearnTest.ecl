IMPORT ML_Core;
IMPORT $.^ AS DBSCAN;
IMPORT $.datasets.frog_dataset AS frog_data;

/*
 * This test compares the results of this implementation against
 * sklearn's DBSCAN implementation.
 */
 
// Load frog_data
ds := frog_data.ds;
// Convert to NumericField
ML_Core.AppendSeqID(ds,id,dsID);
ML_Core.ToField(dsID,dsNF);
// Produce clustering result
clustering := DBSCAN.DBSCAN(0.3,10).fit(dsNF);
// Output clustering result
OUTPUT(clustering, NAMED('FIT'));
// Compare with sklearn results
sk_res := frog_data.sklearn_results;
ML_Core.AppendSeqID(sk_res,id,sk_res_id);
// Find rows that do not match the sklearn result
no_match := JOIN(clustering, sk_res_id,
                 LEFT.id=RIGHT.id and LEFT.label <> RIGHT.a+1,
                 TRANSFORM({ML_Core.Types.ClusterLabels,INTEGER sklabel},
                           SELF.sklabel := RIGHT.a,
                           SELF := LEFT));
// Ideally, the result must be as close to zero rows as possible,
// however, some mismatch is inevitable due to difference in traversal
// orders during parallel computation
OUTPUT(no_match, NAMED('no_match'));