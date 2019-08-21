IMPORT ML_Core;
IMPORT ML_Core.Types AS Types;
IMPORT Files;
IMPORT Std.system.Thorlib;


EXPORT DBSCAN( REAL8 eps = 0, UNSIGNED4 minPts = 2, STRING8 dist = 'Euclidian' ):= MODULE

  EXPORT STREAMED DATASET(Files.l_stage3) locDBSCAN(STREAMED DATASET(Files.l_stage2) dsIn, //distributed data from stage 1
                                                    REAL8 eps = eps,   //distance threshold
                                                    UNSIGNED minPts = minPts, //the minimum number of points required to form a cluster,
                                                    STRING distance_func = dist,
                                                    SET OF REAL8 params = [],
                                                    UNSIGNED4 localNode = Thorlib.node()
                                                    ) := EMBED(C++ : activity)

    #include <iostream>
    #include <bits/stdc++.h>
    #include <cmath>

    using namespace std;

    struct dataRecord{
      uint16_t wi;
      unsigned long long id;
      unsigned long long parentId;
      unsigned long long nodeId;
      bool isAllFields;
      uint32_t lenFields;
      vector<double> fields;
      bool if_local;
      bool if_core;
    };

    struct retRecord{
      uint32_t wi;
      uint32_t id;
      uint32_t parentId;
      uint32_t nodeId;
      bool if_local;
      bool if_core;
    };

    struct node
    {
      uint32_t data;
      node* parent=NULL;
      vector<node *> child;
    };

    struct row
    {
        vector<double> fields;
        struct node id;
        int actual_id;
    };

    typedef struct node* Node;
    typedef struct row* Row;

    Node newNode(uint32_t data){
      Node n=new struct node;
      n->data=data;
        return n;
    }

    Node find(Node y){
      if(y==NULL){
        return NULL;
      }
      return (y->parent)==NULL?y:find(y->parent);
    }

    // returning the root of the tree
    Node unionOp(Node x,Node y)
    {
      if(find(y)==y && find(x)==x)
      {
        if(x->data>y->data){
        (x->child).push_back(y);
        y->parent=x;
        }
        else
        {
          (y->child).push_back(x);
        x->parent=y;
        }
        return find(x);
      }

      else if(find(x)==find(y)){
            return find(x);
      }
        else {
          if(find(x)->data>find(y)->data){
            (find(x)->child).push_back(find(y));
            (find(y)->parent)=find(x);
          return find(x);
        } else {
            (find(y)->child).push_back(find(x));
            (find(x)->parent)=find(y);
            return find(y);
        }
      }
    }

    double euclidean(Row row1,Row row2){
        double ans=0;
        int M=row1->fields.size();

        for(int i=0;i<M;i++)
        ans+=((row1->fields[i])-(row2->fields[i]))*((row1->fields[i])-(row2->fields[i]));

        return sqrt(ans);
    }

    double haversine(Row row1,Row row2){
        int M=row1->fields.size();
        if(M!=2){
            cout<<"Haversine can be applied only for 2 dimensions"<<endl;
            exit(-1);
        }
        double lat1=row1->fields[0];
        double lat2=row2->fields[0];
        double lon1=row1->fields[1];
        double lon2=row2->fields[1];

        double sin_0 = sin(0.5 * (lat1 - lat2));
        double sin_1 = sin(0.5 * (lon1 - lon2));
        return (sin_0 * sin_0 + cos(lat1) * cos(lat2) * sin_1 * sin_1);
    }


    double manhattan(Row row1,Row row2){
        double ans=0;
        int M=row1->fields.size();

        for(int i=0;i<M;i++)
        ans=ans+(abs((row1->fields[i])-(row2->fields[i])));

        return ans;
    }

    double minkowski(Row row1,Row row2,int p)
    {
            // sum(|x - y|^p)^(1/p)

            int m=row1->fields.size();
            double ans=0;
            for(int i=0;i<m;i++){
                    ans+=pow(abs(row1->fields[i]-row2->fields[i]),p);
            }

            return pow(ans,(double)1/p);

    }

    double cosine(Row row1,Row row2){
    double ans=0, a1=0,a2=0;

            int m=row1->fields.size();
            for(int i=0;i<m;i++){
                    ans+=row1->fields[i]*row2->fields[i];
                    a1=a1+pow(row1->fields[i],2);
                    a2=a2+pow(row2->fields[i],2);
            }

            return ans/(sqrt(a1)*sqrt(a2));

    }

    double chebyshev(Row row1,Row row2)
    {
            // max(|x - y|)

            int m=row1->fields.size();
            double ans=0;
            for(int i=0;i<m;i++){
                    ans=max(abs(row1->fields[i]-row2->fields[i]),ans);
            }

            return ans;

    }

    vector<int> visited;
    vector<int> core;
    string distanceFunc = "euclidian";
    vector<double> dist_params;

    Row newRow( int id){
        Row newrow=new struct row;
        newrow->id.data=id;
        return newrow;
    }

    vector<Row> initialise(vector<vector<double>> dataset, vector<uint32_t> ids){
        int N = dataset.size();
        int M = dataset[0].size();
        vector<Row> data;
        visited.resize(N,0);
        core.resize(N,0);

        // for(int i=0;i<N;i++){
        // visited.push_back(0);
        // core.push_back(0);
        // }

        for(int i=0;i<N;i++){

            //initially each node is pointing to itself

            Row r= newRow(ids[i]);
            r->fields.resize(M);

            for(int j=0;j<M;j++){
                r->fields[j]=dataset[i][j];
            }
            r->actual_id=i;
            data.push_back(r);
        }
        return data;
    }

    vector<Row> getNeighrestNeighbours(vector<Row> dataset, Row row, double eps, vector<uint16_t> wis, uint16_t wi){
        vector<Row> neighbours;
        for(int i=0;i<dataset.size();i++){
            if(dataset[i]==row)
            continue;

            if(wis[i] != wi)
            continue;

            double dist;

            if(distanceFunc.compare("manhattan") == 0)
              dist = manhattan(dataset[i],row);
            else if(distanceFunc.compare("haversine") == 0)
              dist = haversine(dataset[i],row);
            else if(distanceFunc.compare("minkowski") == 0)
              dist = minkowski(dataset[i],row,(int)dist_params[0]);
            else if(distanceFunc.compare("cosine") == 0)
              dist = cosine(dataset[i],row);
            else if(distanceFunc.compare("chebyshev") == 0)
              dist = chebyshev(dataset[i],row);
            else
              dist = euclidean(dataset[i],row);

            if(dist<=eps){
                neighbours.push_back(dataset[i]);
            }
        }
        return neighbours;
    }

    vector<Row> dbscan(vector<vector<double>> dataset,int minpoints,double eps,vector<bool> ifLocal, vector<uint16_t> wis, vector<bool> &isModified, vector<uint32_t> ids){
        vector<Row> transdataset=initialise(dataset,ids);
        vector<Row> neighs;
        Node temp;
        int temp1;
        for(int ro=0;ro<transdataset.size();ro++){
            // cout<<"Processing"<<ro<<endl;

            if(!ifLocal[ro]) continue;

            neighs=getNeighrestNeighbours(transdataset,transdataset[ro],eps,wis,wis[ro]);

            //Here 1 indicates the point 'trandataset[ro]' itself. Refer https://en.wikipedia.org/wiki/DBSCAN#Original_Query-based_Algorithm

            if(neighs.size()+1>=minpoints){
                core[ro]=1;

                for(int neigh=0;neigh<neighs.size();neigh++){
                    int neighId = neighs[neigh]->actual_id;
                    isModified[neighId] = true;
                    if(ifLocal[neighId]){
                        // Local neighbour
                        temp1=core[neighId];
                        if(temp1)
                        {

                            //modify parent id's
                            temp=unionOp(&transdataset[ro]->id,&neighs[neigh]->id);
                            cout<<"\nThe parent is "<<temp->data<<endl;
                        }
                        else
                        {
                            if(!visited[neighId]){
                                visited[neighId]=1;
                                // cout<<" Trying union for "<<transdataset[ro]->id.data<<" and "<<neighs[neigh]->id.data<<endl;
                            temp=unionOp(&transdataset[ro]->id,&neighs[neigh]->id);

                            // cout<<"\nThe parent is "<<temp->data<<endl;
                            }
                        }
                    } else {
                        //Remote neighbour
                        vector<Row> tempNeighs = getNeighrestNeighbours(transdataset,transdataset[neighId],eps,wis,wis[neighId]);
                        if(tempNeighs.size()+1>= minpoints)
                            core[neighId] = 1;
                        unionOp(&transdataset[ro]->id,&neighs[neigh]->id);
                    }
                }
            }
        }
        return transdataset;
    }

    class ResultStream : public RtlCInterface, implements IRowStream {
      public:
      ResultStream(IEngineRowAllocator *_ra, IRowStream *_ds, int minpts, double eps, unsigned long long lnode)
      : ra(_ra), ds(_ds){
        byte* p;
        count = 0;
        while((p=(byte*)ds->nextRow())){
          dataRecord temp;
          temp.wi = *((uint16_t*)p); p += sizeof(uint16_t);
          temp.id = *((unsigned long long*)p); p += sizeof(unsigned long long);
          temp.parentId = *((unsigned long long*)p); p += sizeof(unsigned long long);
          temp.nodeId = *((unsigned long long*)p); p += sizeof(unsigned long long);
          temp.isAllFields = *((bool*)p); p += sizeof(bool);
          temp.lenFields = *((uint32_t*)p); p += sizeof(uint32_t);
          for(int i=0; i<temp.lenFields/sizeof(float); ++i){
            double f = (double)(*((float*)p)); p += sizeof(float);
            temp.fields.push_back(f);
          }
          temp.if_local = *((bool*)p); p += sizeof(bool);
          temp.if_core = *((bool*)p);
          items.push_back(temp);
        }

        vector<vector<double>> dataset;
        vector<bool> ifLocal;
        vector<uint16_t> wis;
        vector<bool> isModified;
        vector<uint32_t> ids;

        for(uint i=0; i<items.size(); ++i){
          dataset.push_back(items[i].fields);
          ifLocal.push_back(lnode == items[i].nodeId);
          isModified.push_back(lnode == items[i].nodeId);
          wis.push_back(items[i].wi);
          ids.push_back(items[i].id);
        }

        vector<Row> out_data = dbscan(dataset,minpts,eps,ifLocal,wis,isModified,ids);

        for(uint i=0;i<out_data.size();i++){
          if(!isModified[i]) continue;
          Node dat=find(&out_data[i]->id);
          retRecord temp;
          temp.wi = items[i].wi;
          temp.id = items[i].id;
          temp.parentId = dat->data;
          temp.nodeId = lnode;
          temp.if_local = ifLocal[i];
          temp.if_core = core[i];
          retDs.push_back(temp);
        }
      }

      RTLIMPLEMENT_IINTERFACE
      virtual const void* nextRow() override {
        RtlDynamicRowBuilder rowBuilder(ra);
        if(count < retDs.size()){

          uint32_t lenRec = 4*sizeof(uint32_t) + 2*sizeof(bool);
          byte* p = (byte*)rowBuilder.ensureCapacity(lenRec, NULL);

          int i = count;
          *((uint32_t*)p) = retDs[i].wi; p += sizeof(uint32_t);
          *((uint32_t*)p) = retDs[i].nodeId; p += sizeof(uint32_t);
          *((uint32_t*)p) = retDs[i].id; p += sizeof(uint32_t);
          *((uint32_t*)p) = retDs[i].parentId; p += sizeof(uint32_t);
          *((bool*)p) = retDs[i].if_local; p += sizeof(bool);
          *((bool*)p) = retDs[i].if_core;

          count++;
          return rowBuilder.finalizeRowClear(lenRec);
        } else {
          return NULL;
        }
      }

      virtual void stop() override{}

      protected:
      Linked<IEngineRowAllocator> ra;
      unsigned count;
      vector<dataRecord> items;
      vector<Row> out_data;
      vector<retRecord> retDs;
      IRowStream *ds;
    };

    #body

    distanceFunc = distance_func;
    double* p = (double*)params;

    for(uint32_t i=0; i<lenParams/sizeof(double); ++i)
      dist_params.push_back(*p);
      p += sizeof(double);

    transform(distanceFunc.begin(),distanceFunc.end(),distanceFunc.begin(),::tolower);
    return new ResultStream(_resultAllocator, dsin, minpts, eps, localnode);


  ENDEMBED;//end locDBSCAN

  //Layout for Ultimate() and Loop_Func()
  EXPORT l_ultimate := RECORD
    UNSIGNED4 wi;
    UNSIGNED4 id;
    UNSIGNED4 parentID;
    UNSIGNED4 largestID := 0;
    UNSIGNED4 ultimateID := 0;
  END;

  EXPORT STREAMED DATASET(l_ultimate) ultimate(STREAMED DATASET(l_ultimate) dsin, UNSIGNED4 pointcount) := EMBED(C++:activity)
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
                if(!calculated){
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

  ENDEMBED;//end ultimate()

  //LOOP to get the final result/ultimateID
  EXPORT Loop_Func(DATASET(l_ultimate) ds, UNSIGNED c) := FUNCTION
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
        rst := JOIN(newParent, newUltimate, LEFT.wi = RIGHT.wi AND LEFT.id = RIGHT.id, TRANSFORM(l_ultimate,
                                                          SELF.ultimateID := RIGHT.parentID,
                                                          SELF := LEFT));
        RETURN rst;
  END;//end loop_func()

  /**
    * fit() function i
    */
  EXPORT DATASET(Files.l_result) fit(DATASET(Types.NumericField) ds) := FUNCTION

    /**
      * Stage 1: Transform and distribute input dataset ds for local clustering in stage 2.
      */

    //Evenly distribute the data
    Xnf1 := DISTRIBUTE(ds, id);
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

    /**
      * Stage 2: local clustering on each node
      */

    raw := locDBSCAN(X,1,2);

    intermediate := SORT(raw,wi,nodeId,parentId,LOCAL);

    mapping := TABLE(intermediate(if_core=TRUE),{wi,nodeId,parentId,maxCore:=MAX(GROUP,id)},wi,nodeId,parentId,LOCAL);

    rDS := PROJECT(intermediate,
                      TRANSFORM(RECORDOF(intermediate),
                                SELF.parentID := IF(EXISTS(mapping(wi=LEFT.wi and nodeID=LEFT.nodeID and parentID=LEFT.parentID)),
                                                    mapping(wi=LEFT.wi and nodeID=LEFT.nodeID and parentID=LEFT.parentID)[1].maxCore,
                                                    LEFT.id),
                                SELF := LEFT));

    /**
      * Stage 3: global merge the local clustering results to the final clustering result
      */


    //get non_outliers and its largest parentID
    rds1 := rds( NOT( if_core = FALSE AND id = parentID));
    non_outliers := DEDUP(SORT(rds1,wi, id,-parentID),wi,id );

    //Get outliers
    outliers := PROJECT(JOIN(rDS, non_outliers, LEFT.wi = RIGHT.wi AND LEFT.id = RIGHT.id, LEFT ONLY), TRANSFORM(l_ultimate,
                                                                                  SELF.ultimateid := LEFT.parentid,
                                                                                  SELF := LEFT));
    unfiltered := rDS(if_local = TRUE);
    ntunfiltered := COUNT(unfiltered );
    dds := DISTRIBUTE(unfiltered, wi); //
    f0 := PROJECT(NOCOMBINE(dds), TRANSFORM({l_Ultimate, UNSIGNED4 nodeid}, SELF.nodeid := Thorlib.node(), SELF := LEFT));
    t := TABLE(f0, { nodeid , cnt := COUNT(GROUP)}, nodeid, LOCAL);
    c := t(nodeid = thorlib.node())[1].cnt;

    //get local core points
    f1 := rDS(if_local = TRUE AND if_core=TRUE);
    f2 := DISTRIBUTE(f1, wi);
    localCores := SORT(PROJECT(NOCOMBINE(f2),TRANSFORM(l_ultimate, SELF := LEFT), LOCAL), wi, id, LOCAL);

    locals_ultimate:=  ultimate(localCores, c);// all the ultimates for locals

    //get largestID for the core points
    largest := DISTRIBUTE(non_outliers(if_core = TRUE), wi);

    //Prepare the input dataset 'initial' for Loop_Func()
    //Join largest and locals_ultimate
    initial0 := JOIN(largest, locals_ultimate,
                    LEFT.wi = RIGHT.wi
                    AND
                    LEFT.id = RIGHT.id,
                    TRANSFORM(l_ultimate,
                              SELF.ultimateID := RIGHT.parentID,
                              SELF.largestID := LEFT.parentID,
                              SELF := LEFT), LOCAL);

    //Join locals
    initial := JOIN(initial0, localCores,
                    LEFT.wi = RIGHT.wi
                    AND
                    LEFT.id = RIGHT.id,
                    TRANSFORM(l_ultimate,
                            SELF.parentID := RIGHT.parentID,
                            SELF := LEFT), LOCAL);

    l := LOOP(initial, LEFT.id > 0, EXISTS(ROWS(LEFT)(ultimateID < largestID)), LOOP_Func(ROWS(LEFT), COUNTER) );
    //Update the parentID of all non_outliers from the result
    update_non_outliers := JOIN(non_outliers, l, LEFT.wi = RIGHT.wi AND LEFT.parentid = RIGHT.id, TRANSFORM(l_ultimate,
                                                                    SELF.ultimateID := IF(right.id =0, LEFT.parentid, RIGHT.ultimateID),
                                                                    SELF:= LEFT),
                                                                    LEFT OUTER);
    //combine outlier to get the final complete result
    result0 := outliers + update_non_outliers;
    //Final result with simpiflied format: id and cluster id only
    result := PROJECT(result0 , TRANSFORM(Files.l_result, SELF.clusterID := LEFT.ultimateID, SELF := LEFT));
    RETURN result;
  END;//end fit()

END;//end DBSCAN