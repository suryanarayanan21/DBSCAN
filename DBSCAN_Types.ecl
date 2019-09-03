IMPORT ML_Core;
IMPORT ML_Core.Types as Types;

/*
 * Contains Types required for the internal calculations performed
 * within the DBSCAN module.
 */

EXPORT DBSCAN_Types := MODULE

EXPORT l_stage1 := RECORD
    Types.NumericField;
    UNSIGNED nodeId;
    SET OF REAL4 fields;
END;

EXPORT l_stage2 := RECORD
    Types.NumericField.wi;
    Types.NumericField.id;
    Types.t_RecordID parentID;
    UNSIGNED nodeId;
    SET OF REAL4 fields;
    BOOLEAN if_local := FALSE;
    BOOLEAN if_core := FALSE;
END;
EXPORT l_stage3 := RECORD
UNSIGNED4 wi;
UNSIGNED4 nodeid;
UNSIGNED4 id;
UNSIGNED4 parentID;
BOOLEAN   if_local := FALSE;
BOOLEAN   if_core := FALSE;
END;


EXPORT l_result := RECORD
    UNSIGNED4 wi;
    UNSIGNED4 id;
    UNSIGNED4 clusterID;
END;

EXPORT l_num_clusters := RECORD
    UNSIGNED4 wi;
    UNSIGNED4 num;
END;

END;
