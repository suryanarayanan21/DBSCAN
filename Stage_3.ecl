IMPORT STD.system.Thorlib;
IMPORT Files;


rDS :=  DATASET([
{1,1,5,TRUE,FALSE},
{1,2,5,TRUE,FALSE},
{1,3,5,TRUE,TRUE},
{1,4,5,TRUE,FALSE},
{1,5,5,FALSE,TRUE},
{1,6,6,FALSE,FALSE},
{1,7,7,FALSE,TRUE},
{1,8,8,FALSE,FALSE},
{1,9,9,TRUE,FALSE},
{1,10,10,FALSE,FALSE},
{1,11,11,FALSE,FALSE},
{1,12,12,FALSE,FALSE},
{1,13,13,FALSE,FALSE},
{1,14,14,FALSE,TRUE},
{1,15,15,FALSE,FALSE},
{1,16,16,FALSE,TRUE},
{1,17,17,FALSE,FALSE},
{1,18,18,FALSE,FALSE},
{1,19,19,FALSE,FALSE},
{1,20,20,FALSE,FALSE},
{1,21,21,FALSE,FALSE},
{2,1,1,FALSE,FALSE},
{2,2,21,FALSE,FALSE},
{2,3,3,FALSE,TRUE},
{2,4,4,FALSE,FALSE},
{2,5,21,TRUE,TRUE},
{2,6,21,TRUE,FALSE},
{2,7,21,TRUE,TRUE},
{2,8,8,TRUE,FALSE},
{2,9,21,FALSE,FALSE},
{2,10,10,FALSE,FALSE},
{2,11,11,FALSE,FALSE},
{2,12,21,TRUE,FALSE},
{2,13,13,FALSE,FALSE},
{2,14,14,FALSE,TRUE},
{2,15,15,FALSE,FALSE},
{2,16,16,FALSE,TRUE},
{2,17,17,FALSE,FALSE},
{2,18,18,FALSE,FALSE},
{2,19,19,FALSE,FALSE},
{2,20,20,FALSE,FALSE},
{2,21,21,TRUE,FALSE},
{3,1,1,FALSE,FALSE},
{3,2,2,FALSE,FALSE},
{3,3,3,FALSE,TRUE},
{3,4,4,FALSE,FALSE},
{3,5,5,FALSE,TRUE},
{3,6,6,FALSE,FALSE},
{3,7,7,FALSE,TRUE},
{3,8,8,FALSE,FALSE},
{3,9,9,FALSE,FALSE},
{3,10,10,TRUE,FALSE},
{3,11,20,TRUE,FALSE},
{3,12,12,FALSE,FALSE},
{3,13,13,TRUE,FALSE},
{3,14,20,TRUE,TRUE},
{3,15,15,FALSE,FALSE},
{3,16,20,FALSE,TRUE},
{3,17,17,FALSE,FALSE},
{3,18,18,TRUE,FALSE},
{3,19,20,FALSE,FALSE},
{3,20,20,FALSE,FALSE},
{3,21,21,FALSE,FALSE},
{4,1,1,FALSE,FALSE},
{4,2,2,FALSE,FALSE},
{4,3,3,FALSE,TRUE},
{4,4,4,FALSE,FALSE},
{4,5,5,FALSE,TRUE},
{4,6,6,FALSE,FALSE},
{4,7,7,FALSE,TRUE},
{4,8,8,FALSE,FALSE},
{4,9,9,FALSE,FALSE},
{4,10,10,FALSE,FALSE},
{4,11,16,FALSE,FALSE},
{4,12,12,FALSE,FALSE},
{4,13,13,FALSE,FALSE},
{4,14,16,FALSE,TRUE},
{4,15,16,TRUE,FALSE},
{4,16,16,TRUE,TRUE},
{4,17,17,TRUE,FALSE},
{4,18,18,FALSE,FALSE},
{4,19,19,TRUE,FALSE},
{4,20,20,TRUE,FALSE},
{4,21,21,FALSE,FALSE}
], Files.l_stage3);

OUTPUT(rDS, NAMED('rDS'));


STREAMED DATASET(Files.l_stage3) ultimate(STREAMED DATASET(Files.l_stage3) dsin, UNSIGNED4 pointcount) := EMBED(C++:activity)


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
                  // for(uint32_t i = 0; i < pc ; i++)
                  while(true)
                  {
                      const byte * next = (const byte *)ds->nextRow();
                      if (!next) break;
                      const byte * pos = next + sizeof(uint32_t);
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
                *(uint32_t *)pos = (uint32_t) 0;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = uptable[rc].id;
                pos += sizeof(uint32_t);
                *(uint32_t *)pos = uptable[rc].pid;
                pos += sizeof(uint32_t);
                *(bool *)pos = false;
                pos += sizeof(bool);
                *(bool *)pos = false;
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

locals := DISTRIBUTE(rDS(if_local = TRUE), 0);
OUTPUT(SORT(locals, id), NAMED('locals'));

c :=COUNT(locals);
pc := IF(thorlib.node() = 0, c , 0);
initial0:=  ultimate(locals, pc);
OUTPUT(initial0, NAMED('initial0'));


layout := RECORD
  INTEGER id;
  INTEGER parentID;
  INTEGER largestID;
  INTEGER ultimateID := -1;
END;

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
ds2 := PROJECT(ds1, TRANSFORM(Files.l_stage3,
                        SELF.nodeid := 0;
                        SELF.if_local := FALSE,
                        SELF.if_core := FALSE,
                        SELF := LEFT), LOCAL);
n := COUNT(ds1);
pcc := IF(thorlib.node() = 0, c , 0);
u :=  Ultimate(ds2, pcc);
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
