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

#include<iostream>
#include<bits/stdc++.h>

using namespace std;

struct dataIn{
    uint16_t wi;
    uint64_t id;
    uint64_t nodeId;
    vector<float> fields;
    bool ifLocal = false;
    bool ifCore = false;
    dataIn *parent = NULL;
    bool isModified = false;
    bool isVisited = false;
};

double euclidian(vector<float> a, vector<float> b){
    double sum=0;
    for(int i=0; i<a.size(); ++i){
        sum += (a[i] - b[i])*(a[i] - b[i]);
    }
    return sqrt(sum);
}

vector<dataIn*> getNeighbors(vector<dataIn*> ds, dataIn *p, double eps){
    vector<dataIn*> ret;
    for(uint64_t i=0; i<ds.size(); ++i){
        if(p->wi != ds[i]->wi) continue;
        double dist = euclidian(p->fields, ds[i]->fields);
        if(dist <= eps) ret.push_back(ds[i]);
    }
    return ret;
}

dataIn* find(dataIn *p){
    if(p->parent == NULL || p->parent == p){
        return p;
    } else {
        return find(p->parent);
    }
}

void Union(vector<dataIn*> ds, dataIn* a, dataIn* b){
    dataIn* x = find(a);
    dataIn* y = find(b);
    if(x->ifCore && !y->ifCore){
        y->parent = x;
        return;
    } else if (!x->ifCore && y->ifCore){
        x->parent = y;
        return;
    }
    if(x == y) return;
    else if(x->id > y->id) y->parent = x;
    else x->parent = y;
}

void dbscan(vector<dataIn*> ds, double eps, uint64_t minpts) {
    for(uint64_t i=0; i<ds.size(); ++i){

        if(!ds[i]->ifLocal) continue;

        ds[i]->isModified = true;
        vector<dataIn*> neighs = getNeighbors(ds,ds[i],eps);

        if(neighs.size() >= minpts){
            ds[i]->ifCore = true;
            ds[i]->parent = NULL;
            for(uint64_t n=0; n < neighs.size(); ++n){
                neighs[n]->isModified = true;
                if(neighs[n]->ifLocal){
                    if(neighs[n]->ifCore){
                        Union(ds,ds[i],neighs[n]);
                    } else {
                        if(neighs[n]->isVisited) continue;
                        neighs[n]->isVisited = true;
                        Union(ds,ds[i],neighs[n]);
                    }
                } else {
                    if(!neighs[n]->ifCore){
                        if(getNeighbors(ds,neighs[n],eps).size()>=minpts){
                            neighs[n]->ifCore = true;
                            Union(ds,ds[i],neighs[n]);
                        } else {
                            if(neighs[n]->isVisited) continue;
                            neighs[n]->isVisited = true;
                            Union(ds,ds[i],neighs[n]);
                        }
                    } else {
                        Union(ds,ds[i],neighs[n]);
                    }
                }
            }
        }
    }
}

struct retRecord{
  uint32_t wi;
  uint32_t id;
  uint32_t parentId;
  uint32_t nodeId;
  bool if_local;
  bool if_core;
};

class ResultStream : public RtlCInterface, implements IRowStream {
public:
    ResultStream(IEngineRowAllocator *_ra, IRowStream *_ds, uint64_t minpts, double eps, unsigned long long lnode) : ra(_ra), ds(_ds), Lnode(lnode){
        byte* p;
        count = 0;
        while((p=(byte*)ds->nextRow())){
            dataIn temp;
            temp.wi = *((uint16_t*)p); p += sizeof(uint16_t);
            temp.id = *((unsigned long long*)p); p += 2*sizeof(unsigned long long);
            temp.nodeId = *((unsigned long long*)p); p += sizeof(unsigned long long);
            p += sizeof(bool);
            uint32_t lenFields = *((uint32_t*)p); 
            p += sizeof(uint32_t);
            for(uint32_t i=0; i<lenFields/sizeof(float); ++i){
                float f = *((float*)p); p += sizeof(float);
                temp.fields.push_back(f);
            }
            temp.ifLocal = (lnode == temp.nodeId); p += sizeof(bool);
            temp.ifCore = false;
            dataset.push_back(temp);
        }

        for(uint64_t i=0; i<dataset.size(); ++i){
            results.push_back(&dataset[i]);
        }

        dbscan(results, eps, minpts);
    }

    RTLIMPLEMENT_IINTERFACE
    virtual const void* nextRow() override {
        RtlDynamicRowBuilder rowBuilder(ra);
        if(count < results.size()){
            uint32_t lenRec = 4*sizeof(uint32_t) + 2*sizeof(bool);
            byte* p = (byte*)rowBuilder.ensureCapacity(lenRec, NULL);

            while(!dataset[count].isModified && count < dataset.size())
                count++;
            
            if(count >= results.size()) return NULL;
            
            *((uint32_t*)p) = dataset[count].wi; p += sizeof(uint32_t);
            *((uint32_t*)p) = Lnode; p += sizeof(uint32_t);
            *((uint32_t*)p) = dataset[count].id; p += sizeof(uint32_t);
            *((uint32_t*)p) = find(&dataset[count])->id; p += sizeof(uint32_t);
            *((bool*)p) = dataset[count].ifLocal; p += sizeof(bool);
            *((bool*)p) = dataset[count].ifCore;

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
    vector<dataIn> dataset;
    vector<dataIn*> results;
    IRowStream *ds;
    uint64_t Lnode;
};

#body

//distanceFunc = distance_func;
//double* p = (double*)params;
/*
for(uint32_t i=0; i<lenParams/sizeof(double); ++i)
  dist_params.push_back(*p);
  p += sizeof(double);
*/
//transform(distanceFunc.begin(),distanceFunc.end(),distanceFunc.begin(),::tolower);

return new ResultStream(_resultAllocator, dsin, minpts, eps, localnode);

ENDEMBED;
END;
