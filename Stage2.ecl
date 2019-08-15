IMPORT STD.system.Thorlib;
IMPORT Files;

EXPORT Stage2 := MODULE
EXPORT STREAMED DATASET(Files.l_stage3) locDBSCAN(STREAMED DATASET(Files.l_stage2) dsIn, //distributed data from stage 1
                                  REAL8 eps,   //distance threshold
                                  UNSIGNED minPts, //the minimum number of points required to form a cluster,
                                  STRING distance_func = 'Euclidian',
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

  // cout<<"INSIDE "<<x->data<<"INSDIE";
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
    visited.resize(N);
    core.resize(N);

    for(int i=0;i<N;i++){
    visited.push_back(0);
    core.push_back(0);
    }

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
        cout<<"Processing"<<ro<<endl;
        
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

                        cout<<"\nThe parent is "<<temp->data<<endl;
                        }
                    }
                } else {
                    // Remote neighbour
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


ENDEMBED;
END;
