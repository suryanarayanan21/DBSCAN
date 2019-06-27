IMPORT STD.system.Thorlib;
IMPORT Files;

//Sample code for stage 3 Global Merge

old_layout := RECORD
  UNSIGNED4 nodeid;
  UNSIGNED4 id;
  UNSIGNED4 parentID;
  UNSIGNED4 largestID := 0;
  UNSIGNED4 ultimateID := 0;
  BOOLEAN   if_local := FALSE;
  BOOLEAN   if_core := FALSE;
END;

raw := DATASET([
//node 1 1-5
{1,1,1,0,0,TRUE,TRUE},
{1,3,1,0,0,TRUE,FALSE},
{1,8,1,0,0,FALSE,FALSE},
{1,14,1,0,0,FALSE,FALSE},
{1,4,4,0,0,TRUE,FALSE},
{1,5,5,0,0,TRUE,TRUE},
{1,6,5,0,0,FALSE,FALSE},
{1,18,5,0,0,FALSE,FALSE},
{1,19,5,0,0,FALSE,FALSE},
{1,2,5,0,0,TRUE,FALSE},
//node 2 6-10
{2,6,10,0,0,TRUE,FALSE},
{2,7,7,0,0,TRUE,FALSE},
{2,8,10,0,0,TRUE,FALSE},
{2,9,9,0,0,TRUE,TRUE},
{2,11,9,0,0,FALSE,FALSE},
{2,14,9,0,0,FALSE,FALSE},
{2,17,9,0,0,FALSE,FALSE},
{2,10,10,0,0,TRUE,TRUE},
{2,20,10,0,0,FALSE,FALSE},
//node 3  11-15
{3,11,11,0,0,TRUE,FALSE},
{3,12,12,0,0,TRUE,TRUE},
{3,4,12,0,0,FALSE,FALSE},
{3,15,12,0,0,TRUE,FALSE},
{3,16,12,0,0,FALSE,FALSE},
{3,7,12,0,0,FALSE,FALSE},
{3,13,13,0,0,TRUE,FALSE},
{3,14,14,0,0,TRUE,FALSE},
//node 4  16-20
{4,16,16,0,0,TRUE,FALSE},
{4,17,17,0,0,TRUE,FALSE},
{4,18,18,0,0,TRUE,FALSE},
{4,19,19,0,0,TRUE,FALSE},
{4,20,20,0,0,TRUE,FALSE}
], old_layout);

//New layout for stage 3
l_stage3 := RECORD
UNSIGNED4 wi;
UNSIGNED4 nodeid;
UNSIGNED4 id;
UNSIGNED4 parentID;
BOOLEAN   if_local := FALSE;
BOOLEAN   if_core := FALSE;
END;
rDS := PROJECT(raw, TRANSFORM(l_stage3, SELF.wi := 1, SELF := LEFT));
OUTPUT(rDS, NAMED('rDS'));

//Layout for Ultimate() and Loop_Func()****to-do: add wi?
layout := RECORD
  UNSIGNED4 id;
  UNSIGNED4 parentID;
  UNSIGNED4 largestID := 0;
  UNSIGNED4 ultimateID := 0;
END;

//Helper Function: get the ultimateID.
//For example, sample 1 has parentID 2 and 2 has parentID 3 and 3's parentID is 3.
//Then the UltimateID for sample 1 is 3.
STREAMED DATASET(layout) ultimate(STREAMED DATASET(layout) dsin, UNSIGNED4 pointcount) := EMBED(C++:activity)

  #include <stdio.h>
  struct upt
  {
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
                uptable[i].id =0;
                uptable[i].pid =0;
              };
              calculated = false;
              rc = 0;
            }
            ~MyStreamInlineDataset(){
              // rtlFree(uptable);
            }

          RTLIMPLEMENT_IINTERFACE
  //calculate the ultimate id
          virtual const void *nextRow() override
          {
              if(!calculated){
                  while(true)
                  {
                      const byte * next = (const byte *)ds->nextRow();
                      if (!next) break;
                      const byte * pos = next;
                      uint32_t id = *(uint32_t*)pos;
                      pos += sizeof(uint32_t);
                      uint32_t pid = *(uint32_t *) pos;
                      if (id > 0 && id <=pc)
                      {
                      uptable[id-1].id = id;
                      uptable[id-1].pid = pid;
                      }
                      rtlReleaseRow(next);
                  }// End while()

                for(uint32_t i = 0; i < pc; i++)
                {
                  uint32_t id = uptable[i].id;
                  uint32_t pid = uptable[i].pid;
                  if(id == 0) continue;
                  while(id != pid)
                  {
                    id = pid;
                    if(pid -1  >= pc ){
                      break;
                    }
                    pid = uptable[pid -1].pid;
                  }
                  uptable[i].pid = pid;
                };// end for()

                calculated = true;

              }//end if(!calculated)



              byte* row;
              RtlDynamicRowBuilder rowBuilder(resultAllocator);
              uint32_t returnsize = 3*sizeof(uint32_t) + 2*sizeof(bool);
              while(rc < pc && uptable[rc].id == 0) rc++;
              if(rc < pc)
              {
                row = rowBuilder.ensureCapacity(returnsize, NULL);
                void * pos = row;
                // *(uint32_t *)pos = (uint32_t) 0;
                // pos += sizeof(uint32_t);
                *(uint32_t *)pos = uptable[rc].id;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = uptable[rc].pid;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = 0;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = 0;
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
      };

  #body

        return new MyStreamInlineDataset(_resultAllocator, dsin, pointcount);

ENDEMBED;

//get non_outliers
rds1 := rds( NOT( if_core = FALSE AND id = parentID)) ;
//OUTPUT(rds1, NAMED('rds1'));
non_outliers := DEDUP(SORT(rds1,id,-parentID),id );
// OUTPUT(non_outliers, NAMED('non_outliers'));


//Get outliers
outliers := PROJECT(JOIN(rDS, non_outliers, LEFT.id = RIGHT.id, LEFT ONLY), TRANSFORM(Layout,
                                                                              SELF.ultimateid := LEFT.parentid,
                                                                              SELF := LEFT));
// OUTPUT(outliers, NAMED('outliers'));

//get local core points
locals := PROJECT(rds(if_local = TRUE AND if_core = TRUE),TRANSFORM(layout, SELF := LEFT));
//OUTPUT(SORT(locals, id), NAMED('locals'));

//get the ultimateID for local core points
c :=COUNT(rds);
pc := IF(thorlib.node() = 0, c , 0);
locals_ultimate:=  ultimate(locals, pc);
//OUTPUT(locals_ultimate, NAMED('locals_ultimate'));

//get largestID for the core points
largest := DISTRIBUTE(non_outliers(if_core = TRUE), 0);
//OUTPUT(largest, NAMED('largest'));

//Prepare the input dataset 'initial' for Loop_Func()
//Join largest and locals_ultimate
initial0 := JOIN(largest, locals_ultimate,
                LEFT.id = RIGHT.id,
                TRANSFORM(Layout,
                          SELF.ultimateID := RIGHT.parentID,
                          SELF.largestID := LEFT.parentID,
                          SELF := LEFT), LOCAL);
//OUTPUT(initial1, NAMED('initial1'));

//Join locals
initial := JOIN(initial0, locals, LEFT.id = RIGHT.id, TRANSFORM(layout,
                                                                SELF.parentID := RIGHT.parentID,
                                                                SELF := LEFT));
//OUTPUT(initial, NAMED('initial'));

//LOOP to get the final result/ultimateID
Loop_Func(DATASET(Layout) ds) := FUNCTION
    tempLayout := RECORD
      UNSIGNED4 id;
      UNSIGNED4 newParentID;
    END;
    tempChanges := PROJECT(ds, TRANSFORM(tempLayout,
                                  SELF.id := LEFT.ultimateID,
                                  SELF.newParentID := LEFT.largestID), LOCAL);
    changes := DEDUP(SORT(tempChanges, id, -newParentID, LOCAL), id, LOCAL);
    newParent := JOIN(ds, changes, LEFT.id = RIGHT.id, TRANSFORM(RECORDOF(LEFT),
                                                          SELF.parentID := IF(right.id > 0, RIGHT.newParentID, LEFT.parentID),
                                                          SELF := LEFT), LEFT OUTER, LOCAL);
    n := COUNT(newParent);
    toNode0 := IF(thorlib.node() = 0, n , 0);
    newUltimate :=  Ultimate(newParent, toNode0);
    rst := JOIN(newParent, newUltimate, LEFT.id = RIGHT.id, TRANSFORM(layout,
                                                      SELF.ultimateID := RIGHT.parentID,
                                                      SELF := LEFT));
    RETURN rst;
END;

l := LOOP(initial, LEFT.id > 0, EXISTS(ROWS(LEFT)(ultimateID < largestID)), LOOP_Func(ROWS(LEFT)) );
//OUTPUT(result0, NAMED('result0'));

//Update the parentID of all non_outliers from the result
update_non_outliers := JOIN(non_outliers, l, LEFT.parentid = RIGHT.id, TRANSFORM(Layout,
                                                                SELF.ultimateID := IF(right.id =0, LEFT.parentid, RIGHT.ultimateID),
                                                                SELF:= LEFT),
                                                                LEFT OUTER);
//OUTPUT(SORT(result1, id), NAMED('result1'));

//combine outlier to get the final complete result
result := outliers + update_non_outliers ;

//Final result with simpiflied format: id and cluster id only
final := PROJECT(update_non_outliers , TRANSFORM({UNSIGNED4 id, UNSIGNED4 clusterID}, SELF.clusterID := LEFT.ultimateID, SELF := LEFT));
//OUTPUT(SORT(result, id), NAMED('result'));

//evaluate the result: if the result shows 0 rows, it proves the result is correct.
l_result := RECORD
  UNSIGNED4 id;
  UNSIGNED4 clusterID;
END;

answers := DATASET([
  {1,1},
  {2,5},
  {3,1},
  {4,12},
  {5,5},
  {6,10},
  {7,12},
  {8,10},
  {9,9},
  {10,10},
  {11,9},
  {12,12},
  {13,13},
  {14,9},
  {15,12},
  {16,12},
  {17,9},
  {18,5},
  {19,5},
  {20,10}
], l_result);

evl := JOIN(final, answers, LEFT.id = RIGHT.id AND LEFT.clusterID <> RIGHT.clusterID);
OUTPUT(evl, NAMED('evaluation'));