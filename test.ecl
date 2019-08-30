IMPORT STD;
IMPORT $ as DBSCAN;
IMPORT ML_Core;
IMPORT LearningTrees;
IMPORT Files;
IMPORT Python;
IMPORT blobsDS;


trainDat := blobsDS.trainRec;
ML_Core.ToField(trainDat, trainNF);

labelfield := 4;
indpset := trainNF(number < labelfield);
OUTPUT(indpset, NAMED('indpset'));
dpset := trainNF(number = labelfield);

mod := DBSCAN.DBSCANv0(0.3,10).fit(indpset);
OUTPUT(mod);

l_result := Files.l_result;
NumericField := ML_Core.Types.NumericField;

// Compare with Scikit Learn
DATASET(l_result) sklearn() := EMBED(Python)
  from sklearn.cluster import DBSCAN
  from sklearn import metrics
  from sklearn.datasets.samples_generator import make_blobs
  from sklearn.preprocessing import StandardScaler

  centers = [[1, 1], [-1, -1], [1, -1]]
  X, labels_true = make_blobs(n_samples=750, centers=centers, cluster_std=0.4,
                              random_state=0)
  X = StandardScaler().fit_transform(X)
  db = DBSCAN(eps=0.3, min_samples=10).fit(X)
  labels = db.labels_
  n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
  n_noise_ = list(labels).count(-1)

  result = []
  for i in range(len(labels)):
    rec = [1] + [(i+1)] + list(label[i])
    result.append(rec)
  return result
ENDEMBED;

sk_rst := sklearn();

comparison := JOIN(mod, sk_rst,
                   LEFT.wi = RIGHT.wi,
                   TRANSFORM({NumericField.wi, REAL8 ML_Core, REAL8 sklearn},
                             SELF.wi := LEFT.wi,
                             SELF.ML_Core := LEFT.clusterID,
                             SELF.sklearn := RIGHT.clusterID));

OUTPUT(comparison);