IMPORT STD.system.Thorlib;

layout := RECORD
  UNSIGNED4 id;
  UNSIGNED4 parentID;
  UNSIGNED4 largestID := -1;
  UNSIGNED4 ultimateID := -1;
END;

layout1 := RECORD
  UNSIGNED4 id;
  UNSIGNED4 parentID;
  UNSIGNED4 largestID := -1;
  UNSIGNED4 ultimateID := -1;
  BOOLEAN   if_local := FALSE;
END;

rDS :=  DATASET([
{1,5,-1,-1,TRUE},
{2,5,-1,-1,TRUE},
{3,5,-1,-1,TRUE},
{4,5,-1,-1,TRUE},
{5,5,-1,-1,FALSE},
{6,6,-1,-1,FALSE},
{7,7,-1,-1,FALSE},
{8,8,-1,-1,FALSE},
{9,9,-1,-1,TRUE},
{10,10,-1,-1,FALSE},
{11,11,-1,-1,FALSE},
{12,12,-1,-1,FALSE},
{13,13,-1,-1,FALSE},
{14,14,-1,-1,FALSE},
{15,15,-1,-1,FALSE},
{16,16,-1,-1,FALSE},
{17,17,-1,-1,FALSE},
{18,18,-1,-1,FALSE},
{19,19,-1,-1,FALSE},
{20,20,-1,-1,FALSE},
{21,21,-1,-1,FALSE},
{1,1,-1,-1,FALSE},
{2,21,-1,-1,FALSE},
{3,3,-1,-1,FALSE},
{4,4,-1,-1,FALSE},
{5,21,-1,-1,TRUE},
{6,21,-1,-1,TRUE},
{7,21,-1,-1,TRUE},
{8,8,-1,-1,TRUE},
{9,21,-1,-1,FALSE},
{10,10,-1,-1,FALSE},
{11,11,-1,-1,FALSE},
{12,21,-1,-1,TRUE},
{13,13,-1,-1,FALSE},
{14,14,-1,-1,FALSE},
{15,15,-1,-1,FALSE},
{16,16,-1,-1,FALSE},
{17,17,-1,-1,FALSE},
{18,18,-1,-1,FALSE},
{19,19,-1,-1,FALSE},
{20,20,-1,-1,FALSE},
{21,21,-1,-1,TRUE},
{1,1,-1,-1,FALSE},
{2,2,-1,-1,FALSE},
{3,3,-1,-1,FALSE},
{4,4,-1,-1,FALSE},
{5,5,-1,-1,FALSE},
{6,6,-1,-1,FALSE},
{7,7,-1,-1,FALSE},
{8,8,-1,-1,FALSE},
{9,9,-1,-1,FALSE},
{10,10,-1,-1,TRUE},
{11,20,-1,-1,TRUE},
{12,12,-1,-1,FALSE},
{13,13,-1,-1,TRUE},
{14,20,-1,-1,TRUE},
{15,15,-1,-1,FALSE},
{16,20,-1,-1,FALSE},
{17,17,-1,-1,FALSE},
{18,18,-1,-1,TRUE},
{19,20,-1,-1,FALSE},
{20,20,-1,-1,FALSE},
{21,21,-1,-1,FALSE},
{1,1,-1,-1,FALSE},
{2,2,-1,-1,FALSE},
{3,3,-1,-1,FALSE},
{4,4,-1,-1,FALSE},
{5,5,-1,-1,FALSE},
{6,6,-1,-1,FALSE},
{7,7,-1,-1,FALSE},
{8,8,-1,-1,FALSE},
{9,9,-1,-1,FALSE},
{10,10,-1,-1,FALSE},
{11,16,-1,-1,FALSE},
{12,12,-1,-1,FALSE},
{13,13,-1,-1,FALSE},
{14,16,-1,-1,FALSE},
{15,16,-1,-1,TRUE},
{16,16,-1,-1,TRUE},
{17,17,-1,-1,TRUE},
{18,18,-1,-1,FALSE},
{19,19,-1,-1,TRUE},
{20,20,-1,-1,TRUE},
{21,21,-1,-1,FALSE}
], layout1);

OUTPUT(rDS, NAMED('rDS'));
//---To Do
//change the input format to Layout
//try another example with more than one loop
STREAMED DATASET(layout) ultimate(STREAMED DATASET(layout) dsin, UNSIGNED4 pointcount) := EMBED(C++:activity)


  #include <stdio.h>
  //   if(thorlib.node() <>0)
  // {
  //   return NULL;
  // }
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
                uptable[i].id =1;
                uptable[i].pid =1;
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


largest0 := DEDUP(SORT(rDS,id,-parentID),id );
OUTPUT(largest0, NAMED('largest0'));
largest := DISTRIBUTE(largest0, 0);

locals := DISTRIBUTE(PROJECT(rDS(if_local = TRUE),TRANSFORM(layout, SELF := LEFT)), 0);
OUTPUT(SORT(locals, id), NAMED('locals'));

c :=COUNT(locals);
pc := IF(thorlib.node() = 0, c , 0);
initial0:=  ultimate(locals, pc);
OUTPUT(initial0, NAMED('initial0'));


initial1 := JOIN(largest, initial0,
                LEFT.id = RIGHT.id,
                TRANSFORM(Layout,
                          SELF.ultimateID := RIGHT.parentID,
                          SELF.largestID := LEFT.parentID,
                          SELF := LEFT), LOCAL);

initial := JOIN(initial1, locals, LEFT.id = RIGHT.id, TRANSFORM(layout,
                                                                SELF.parentID := RIGHT.parentID,
                                                                SELF := LEFT));
OUTPUT(initial, NAMED('initial'));

Loop_Func(DATASET(Layout) ds) := FUNCTION //ECL
// if ultimateID = laregestID, DONE
// else parentID(ultimateID) = largestID

tempLayout := RECORD
  UNSIGNED4 id;
  UNSIGNED4 newParentID;
END;
changes0 := PROJECT(ds, TRANSFORM(tempLayout,
                              SELF.id := LEFT.ultimateID,
                              SELF.newParentID := LEFT.largestID), LOCAL);
changes := DEDUP(SORT(changes0, id, -newParentID, LOCAL), id, LOCAL);
ds1 := JOIN(ds, changes, LEFT.id = RIGHT.id, TRANSFORM(RECORDOF(LEFT),
                                                        SELF.parentID := IF(right.id > 0, RIGHT.newParentID, LEFT.parentID),
                                                       SELF := LEFT), LEFT OUTER, LOCAL);
n := COUNT(ds1);
pcc := IF(thorlib.node() = 0, c , 0);
u :=  Ultimate(ds1, pcc);
ds3 := JOIN(ds1, u, LEFT.id = RIGHT.id, TRANSFORM(layout,
                                                  SELF.ultimateID := RIGHT.parentID,
                                                  SELF := LEFT));
RETURN ds3;
END;
result0 := LOOP(initial, LEFT.id > 0, EXISTS(ROWS(LEFT)(ultimateID < largestID)), LOOP_Func(ROWS(LEFT)) );
OUTPUT(result0);
result := PROJECT(result0, TRANSFORM({UNSIGNED4 id, UNSIGNED4 clusterID}, SELF.clusterID := LEFT.ultimateID, SELF := LEFT));
OUTPUT(SORT(result, id));

// result0 := LOOP(initial, 1 ,  LOOP_Func(ROWS(LEFT)) );
// OUTPUT(result0);
// result := PROJECT(result0, TRANSFORM({UNSIGNED4 id, UNSIGNED4 clusterID}, SELF.clusterID := LEFT.ultimateID, SELF := LEFT));
// OUTPUT(SORT(result, id));
