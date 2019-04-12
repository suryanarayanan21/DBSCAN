IMPORT STD.system.Thorlib;
IMPORT Files;

layout := RECORD
  UNSIGNED4 id;
  UNSIGNED4 parentID;
  UNSIGNED4 largestID := 0;
  UNSIGNED4 ultimateID := 0;
END;

layout1 := RECORD
  UNSIGNED4 id;
  UNSIGNED4 parentID;
  UNSIGNED4 largestID := 0;
  UNSIGNED4 ultimateID := 0;
  BOOLEAN   if_local := FALSE;
END;


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


rDS :=  Files.rDS;
rDS1 := Files.rDS1;
rDS3 := Files.rDS3;
OUTPUT(rDS, NAMED('rDS'));
OUTPUT(rDS1, NAMED('rDS1'));

rDS2 := rDS3(if_local = TRUE or if_core = TRUE or id <> parentid);
OUTPUT(rDS2, NAMED('rDS2'));

// largest0 := DEDUP(SORT(rDS1(if_local = TRUE or if_core = TRUE),id,-parentID),id );
largest0 := DEDUP(SORT(rDS2,id,-parentID),id );
OUTPUT(largest0, NAMED('largest0'));
largest := DISTRIBUTE(largest0, 0);

locals := DISTRIBUTE(PROJECT(rDS3(if_local = TRUE),TRANSFORM(layout, SELF := LEFT)), 0);
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
