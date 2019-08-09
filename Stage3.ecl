IMPORT ML_Core;
IMPORT ML_Core.Types AS Types;
IMPORT STD.system.Thorlib;
IMPORT Files;
IMPORT Stage2;

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
//Braodcast for local clustering.
X := DISTRIBUTE(X3, ALL);

//Sample code for stage 3 Global Merge
//New layout for stage 3
l_stage3 := Files.l_stage3;

raw := stage2.locDBSCAN(X,0.5,5);

w1 := PROJECT(raw, TRANSFORM(l_stage3, SELF.wi := 1, SELF := LEFT));
OUTPUT(w1, NAMED('w1'));
w2 := PROJECT(raw, TRANSFORM(l_stage3, SELF.wi := 2, SELF := LEFT));
OUTPUT(w2, NAMED('w2'));
rDS := w1 + w2;
OUTPUT(rDS, NAMED('rDS'));

//Layout for Ultimate() and Loop_Func()
layout := RECORD
  UNSIGNED4 wi;
  UNSIGNED4 id;
  UNSIGNED4 parentID;
  UNSIGNED4 largestID := 0;
  UNSIGNED4 ultimateID := 0;
END;

Layout1 := RECORD(layout)
  UNSIGNED4 nodeid;
END;

STREAMED DATASET(layout) ultimate(STREAMED DATASET(layout) dsin, UNSIGNED4 pointcount) := EMBED(C++:activity)
  #include <stdio.h>
  struct upt
  {
    uint32_t wi;
    uint32_t id;
    uint32_t pid;
  };

  class MyStreamInlineDataset : public RtlCInterface, implements IRowStream
    {
      public:
          MyStreamInlineDataset(IEngineRowAllocator * _resultAllocator, IRowStream * _ds, uint32_t _pc)
            :resultAllocator(_resultAllocator), ds(_ds), pc(_pc)
            {
              uptable = (upt*) rtlMalloc(pc * sizeof(upt));
              for(uint32_t i = 0; i < pc; i++)
              {
                uptable[i].wi = 0;
                uptable[i].id = 0;
                uptable[i].pid = 0;
              };
              calculated = false;
              rc = 0;
              lastgroupend = 0;
              curWi = 0;
              lastid = 0;
            }
            ~MyStreamInlineDataset(){
              // rtlFree(uptable);
            }

          RTLIMPLEMENT_IINTERFACE
  //calculate the ultimate id
          virtual const void *nextRow() override
          {
              // uint32_t lastid;
              if(!calculated){
                  // lastid = 0;
                  while(true)
                  {
                      const byte * next = (const byte *)ds->nextRow();
                      if (!next) break;
                      const byte * pos = next;
                      uint32_t wi = *(uint32_t*)pos;
                      pos += sizeof(uint32_t);
                      uint32_t id = *(uint32_t*)pos;
                      pos += sizeof(uint32_t);
                      uint32_t pid = *(uint32_t *) pos;
                      if(curWi == 0){
                        curWi = wi;
                      }
                      if(curWi != wi){
                        curWi = wi;
                        lastgroupend = lastid;
                      }
                      id += lastgroupend;
                      pid += lastgroupend;
                      if (id > 0 && id <= pc)
                      {
                      uptable[id -1].wi = wi;
                      uptable[id -1].id = id;
                      uptable[id -1].pid = pid;
                      }
                      lastid = id;
                      // lastid++;
                      rtlReleaseRow(next);
                  }// End while()

                  for(uint32_t i = 0; i < pc; i++)
                  {
                    uint32_t wi = uptable[i].wi;
                    uint32_t id = uptable[i].id;
                    uint32_t pid = uptable[i].pid;
                    if(id == 0) continue;
                    while(id != pid)
                    {
                      id = pid;
                      if(pid - 1  >= pc){
                        break;
                      }
                      if(uptable[pid -1].pid == 0 || uptable[pid -1].wi != wi){
                        break;
                      }else{
                        pid = uptable[pid -1].pid;
                      }
                    }
                    uptable[i].pid = pid;
                  };// end for()

                calculated = true;
                lastgroupend = 0;
                curWi = 0;
                lastid = 0;
              }//end if(!calculated)

              byte* row;
              RtlDynamicRowBuilder rowBuilder(resultAllocator);
              uint32_t returnsize = 5*sizeof(uint32_t);
              while(rc < pc && uptable[rc].id == 0){ rc++;}
              if(rc < pc)
              {
                row = rowBuilder.ensureCapacity(returnsize, NULL);
                void * pos = row;
                uint32_t id = uptable[rc].id;
                uint32_t pid = uptable[rc].pid;
                uint32_t wi = uptable[rc].wi;

                if(curWi == 0){
                  curWi = wi;
                }
                if(curWi != wi){
                  curWi = wi;
                  lastgroupend = lastid;
                }
                id = id - lastgroupend;
                pid = pid - lastgroupend;
                *(uint32_t *)pos = wi;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = id;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = pid;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = rc;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = lastgroupend;
                lastid = id;
                rc++;
                return rowBuilder.finalizeRowClear(returnsize);
              }else{
                return NULL;
              }// end if()

          }// end nextRow()

          virtual void stop() override
          {
              // ds->stop();
          }

          protected:
              Linked<IEngineRowAllocator> resultAllocator;
              IRowStream * ds;
              uint32_t pc;
              upt * uptable;
              bool calculated;
              uint32_t rc;// row counter
              uint32_t lastgroupend;
              uint32_t curWi;
              uint32_t lastid;
    };

  #body

        return new MyStreamInlineDataset(_resultAllocator, dsin, pointcount);

ENDEMBED;

//get non_outliers and its largest parentID
rds1 := rds( NOT( if_core = FALSE AND id = parentID));
OUTPUT(rds1, NAMED('rds1'));
non_outliers := DEDUP(SORT(rds1,wi, id,-parentID),wi,id );
OUTPUT(non_outliers, NAMED('non_outliers'));

//Get outliers
outliers := PROJECT(JOIN(rDS, non_outliers, LEFT.wi = RIGHT.wi AND LEFT.id = RIGHT.id, LEFT ONLY), TRANSFORM(Layout,
                                                                              SELF.ultimateid := LEFT.parentid,
                                                                              SELF := LEFT));
OUTPUT(outliers, NAMED('outliers'));

unfiltered := rDS(if_local = TRUE);
cntunfiltered := COUNT(unfiltered );
OUTPUT(cntunfiltered, NAMED('cntunfiltered'));

dds := DISTRIBUTE(unfiltered, wi); //
f0 := PROJECT(NOCOMBINE(dds), TRANSFORM(layout1, SELF.nodeid := Thorlib.node(), SELF := LEFT));
OUTPUT(f0, NAMED('f0'));
t := TABLE(f0, { nodeid , cnt := COUNT(GROUP)}, nodeid, LOCAL);
OUTPUT(t, NAMED('t'));
c := t(nodeid = thorlib.node())[1].cnt;


//get local core points
f1 := rDS(if_local = TRUE AND if_core=TRUE);
OUTPUT(f1, NAMED('f1'));
f2 := DISTRIBUTE(f1, wi);
localCores := SORT(PROJECT(NOCOMBINE(f2),TRANSFORM(layout, SELF := LEFT), LOCAL), wi, id, LOCAL);
OUTPUT(localCores, NAMED('localCores'));

locals_ultimate:=  ultimate(localCores, c);// all the ultimates for locals
OUTPUT(locals_ultimate, NAMED('locals_ultimate'));


//get largestID for the core points
largest := DISTRIBUTE(non_outliers(if_core = TRUE), wi);
OUTPUT(largest, NAMED('largest'));

//Prepare the input dataset 'initial' for Loop_Func()
//Join largest and locals_ultimate
initial0 := JOIN(largest, locals_ultimate,
                LEFT.wi = RIGHT.wi
                AND
                LEFT.id = RIGHT.id,
                TRANSFORM(Layout,
                          SELF.ultimateID := RIGHT.parentID,
                          SELF.largestID := LEFT.parentID,
                          SELF := LEFT), LOCAL);
OUTPUT(initial0, NAMED('initial0'));

//Join locals
initial := JOIN(initial0, localCores,
                LEFT.wi = RIGHT.wi
                AND
                LEFT.id = RIGHT.id,
                TRANSFORM(layout,
                        SELF.parentID := RIGHT.parentID,
                        SELF := LEFT), LOCAL);
OUTPUT(initial, NAMED('initial'));


//LOOP to get the final result/ultimateID
Loop_Func(DATASET(Layout) ds) := FUNCTION
    tempLayout := RECORD
      UNSIGNED4 wi;
      UNSIGNED4 id;
      UNSIGNED4 newParentID;
    END;
    tempChanges := PROJECT(ds, TRANSFORM(tempLayout,
                                  SELF.wi := LEFT.wi,
                                  SELF.id := LEFT.ultimateID,
                                  SELF.newParentID := LEFT.largestID), LOCAL);
    changes := DEDUP(SORT(tempChanges, wi, id, -newParentID, LOCAL), wi, id, LOCAL);
    newParent := JOIN(ds, changes, LEFT.wi = RIGHT.wi AND LEFT.id = RIGHT.id, TRANSFORM(RECORDOF(LEFT),
                                                          SELF.parentID := IF(right.id > 0, RIGHT.newParentID, LEFT.parentID),
                                                          SELF := LEFT), LEFT OUTER, LOCAL);

    newUltimate :=  Ultimate(newParent, c);
    rst := JOIN(newParent, newUltimate, LEFT.wi = RIGHT.wi AND LEFT.id = RIGHT.id, TRANSFORM(layout,
                                                      SELF.ultimateID := RIGHT.parentID,
                                                      SELF := LEFT));
    RETURN rst;
END;
l := LOOP(initial, LEFT.id > 0, EXISTS(ROWS(LEFT)(ultimateID < largestID)), LOOP_Func(ROWS(LEFT)) );
OUTPUT(SORT(l, wi, id), NAMED('l'));

//Update the parentID of all non_outliers from the result
update_non_outliers := JOIN(non_outliers, l, LEFT.wi = RIGHT.wi AND LEFT.parentid = RIGHT.id, TRANSFORM(Layout,
                                                                SELF.ultimateID := IF(right.id =0, LEFT.parentid, RIGHT.ultimateID),
                                                                SELF:= LEFT),
                                                                LEFT OUTER);
OUTPUT(SORT(update_non_outliers, wi, id), NAMED('update_non_outliers'));

//combine outlier to get the final complete result
result := outliers + update_non_outliers ;
OUTPUT(SORT(result, wi, id), NAMED('result'));

//Final result with simpiflied format: id and cluster id only
final := PROJECT(result , TRANSFORM({UNSIGNED4 wi, UNSIGNED4 id, UNSIGNED4 clusterID}, SELF.clusterID := LEFT.ultimateID, SELF := LEFT));

OUTPUT(SORT(final, wi, id), NAMED('final'));