/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/*
 * Description: Example User Defined Analytic Function Lag.
 *
 * Create Date: Nov 22, 2011
 */

#include <stdlib.h>
#include <string.h>
#include <chrono>
#include <unordered_map>
#include "tsl/hopscotch_map.h"
#include "tsl/hopscotch_set.h"
#include <array>
#include <vector>
#include <mutex>
#include "safe_ptr.h"
#include "Vertica.h"
//#include <chrono>

#define MAXHASHSIZE 32
#define METRIC_BLOCK_CAPACITY 16000
#define MAXMETRICCOUNT 128
#define MAXMASKCOUNT 2048


using namespace Vertica;
using namespace std;


struct Metric
{
    string name;
    int input_ind;
    bool is_additive;
};

struct Dimension
{
    int pos;
    int size;
};

struct DimHash
{
    DimHash(char sz = 0, char *d = 0) : print(0), size(sz), hash(d) {}

    size_t print;
    char size;
    char *hash;
};

struct HashFunctor
{
    size_t operator()(const DimHash &d) const noexcept
    {
        return d.print ? d.print : std::_Hash_impl::hash(d.hash, d.size);
    }
};

struct EqualFunctor
{
    bool operator()(const DimHash& x, const DimHash& y) const
    {
        return /*x.print == y.print &&*/ memcmp(x.hash, y.hash, x.size) == 0;
    }
};

struct LessFunctor
{
    bool operator()(const DimHash& x, const DimHash& y) const
    {
        if (x.print < y.print)
        {
            return true;
        }
        else if (x.print > y.print)
        {
            return false;
        }
        else
        {
            return memcmp(x.hash, y.hash, x.size) == -1;
        }
    }
};


struct ResultTuple
{
    ResultTuple(char *hash = 0, int *data = 0, int metricCount = 0)
        : hash(hash),
          final_metrics(data),
          local_metrics((char*)(data + metricCount)),
          counted((char*)(data + metricCount) + metricCount) {}

    char *hash;
    int *final_metrics;
    char *local_metrics;
    char *counted;
};


struct i8
{
    union
    {
        uint64 i;
        char b[8];
    };

    int size;
};


struct ArgMin
{
    ArgMin(uint64 v, int a) : min(v), arg(a) {}

    uint64 min;
    int arg;
};


class MinMutation
{
public:
    MinMutation(const int hashSize, vector<Metric> metrics)
        : HASHSIZE(hashSize), METRICCOUNT(metrics.size()), metrics(metrics) {}

    void apply(vector<ResultTuple> &local_hashes, char *blank_hash, i8 mutated_bytes)
    {
        map<int, ArgMin> *m = NULL;

        for (int k = 0; k < METRICCOUNT; ++k)
        {
            if (mutated_bytes.i == 0xffffffff && mutation_dups.size() == 0) continue;
            if (metrics[k].is_additive) continue;
            //if (local_hashes.back().local_metrics[k] == 0) continue;

            if (mutation_dups.size() == 0)
            {
                for (size_t i = 0; i < local_hashes.size() - 1; ++i)
                {
                    DimHash deferred(HASHSIZE, local_hashes[i].hash + HASHSIZE);
                    deferred.print = HashFunctor()(deferred);
                    map<int, ArgMin> &hash_mutations = mutation_dups[deferred];

                    for (int j = 0; j < METRICCOUNT; ++j)
                    {
                        if (!metrics[j].is_additive /*&& local_hashes[i].local_metrics[j] != 0*/)
                        {
                            hash_mutations.insert(make_pair(j, ArgMin(0xffffffff, i)));
                        }
                    }
                }
            }

            if (m == NULL)
            {
                DimHash mutation(HASHSIZE, blank_hash);
                mutation.print = HashFunctor()(mutation);
                m = &mutation_dups[mutation]; // metric_no -> (min,argmin)
            }

            auto p = m->find(k);
            if (p == m->end())
            {
                m->insert(make_pair(k, ArgMin(mutated_bytes.i, local_hashes.size() - 1)));
            }
            else if (p->second.min < mutated_bytes.i)
            {
                local_hashes[p->second.arg].local_metrics[k] = max(
                    local_hashes.back().local_metrics[k],
                    local_hashes[p->second.arg].local_metrics[k]);
                local_hashes.back().local_metrics[k] = 0;
            }
            else if (p->second.min > mutated_bytes.i)
            {
                local_hashes.back().local_metrics[k] = max(
                    local_hashes.back().local_metrics[k],
                    local_hashes[p->second.arg].local_metrics[k]);
                local_hashes[p->second.arg].local_metrics[k] = 0;
                p->second = ArgMin(mutated_bytes.i, local_hashes.size() - 1);
            }
        }
    }

    void reset()
    {
        mutation_dups.clear();
    }

private:
    const int HASHSIZE;
    const int METRICCOUNT;
    vector<Metric> metrics;
    map<DimHash, map<int, ArgMin>, LessFunctor> mutation_dups; //hash -> metric_no -> (min,argmin)
};


static vector<string>
splitString(string s, char delim)
{
    vector<string> res;
    istringstream f(s);
    string p;
    while (getline(f, p, delim))
    {
        res.push_back(p);
    }
    return res;
}


template<class Type, int size>
struct DataBlock
{
    DataBlock() : d(0) {}

    DataBlock(bool) : d(0)
    {
        d = new Type[size];
        memset(d, 0, size * sizeof(Type));
    }

    DataBlock(DataBlock &&other) : d(0)
    {
        d = other.d;
        other.d = 0;
    }

    DataBlock & operator = (DataBlock &&other)
    {
        delete[] d;
        d = other.d;
        other.d = 0;
        return *this;
    }

    ~DataBlock()
    {
        delete[] d;
    }

    Type *d;
};


inline int create_value_mask(char *metrics, int metric_count)
{
    if (metric_count == 1)
    {
        return metrics[0] ? 1 : 0;
    }
    else if (metric_count == 2)
    {
        return (metrics[0] ? 1 : 0) | (metrics[1] ? 2 : 0);
    }

    int m = 0;
    for (int i = 0; i < metric_count; ++i)
    {
        m |= (metrics[i] ? 1 << i : 0);
    }
    return m;
}


class DistinctHashCounter : public TransformFunction
{
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
    {
        mutex.lock();
        if (threadCount == 0)
        {
            boundCookies.clear();
            globalHashTable.clear();
            blocks.clear();
            blocks.push_back(DataBlock<int, METRIC_BLOCK_CAPACITY>(true));
            blockCursor = 0;
            srvInterface.log("cache cleaned up");
        }
        threadNo = threadCount++;
        mutex.unlock();

        srvInterface.log("thread %d setup", threadNo);
    }

    virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
    {
        mutex.lock();
        threadCount--;
        if (threadCount == 0)
        {
            boundCookies.clear();
            globalHashTable.clear();
            blocks.clear();
            srvInterface.log("cache cleaned up");
        }
        mutex.unlock();

        srvInterface.log("thread %d destroy", threadNo);
    }

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter)
    {
        try
        {

            // check semantics
            const SizedColumnTypes &inputColumns = inputReader.getTypeMetaData();

            if (inputColumns.getColumnCount() < 3)
            {
                vt_report_error(0, "Too few columns");
            }

            for (size_t c = 0; c < inputColumns.getColumnCount(); ++c)
            {
                if (c == 0 && !inputColumns.getColumnType(c).isInt())
                {
                    vt_report_error(0, "First column expected to be identifier of type integer");
                }
                else if (c == 1 && !inputColumns.getColumnType(c).isVarbinary())
                {
                    vt_report_error(0, "Second column expected to be hash of type varbinary");
                }
                else if (c > 1 && !inputColumns.getColumnType(c).isInt())
                {
                    vt_report_error(0, "Metric column expected to be integer");
                }
            }

            // parse params
            vector<Metric> metrics = DistinctHashCounter::parseCounters(srvInterface.getParamReader(), inputReader.getTypeMetaData());
            int METRICCOUNT = min(MAXMETRICCOUNT, (int)metrics.size());
            int METRIC_TUPLE_SIZE = METRICCOUNT + METRICCOUNT / sizeof(int) + 1;

            ParamReader paramReader = srvInterface.getParamReader();
            VString header = paramReader.getStringRef("mask_header");
            VString blob = paramReader.getStringRef("mask_blob");

            int HASHSIZE = min(MAXHASHSIZE, (int) header.length());
            int MASKCOUNT = min(MAXMASKCOUNT, (int) (blob.length() / header.length()));

            char mask_blob[65000];
            memcpy(mask_blob, blob.data(), HASHSIZE * MASKCOUNT); //memset(mask_blob, 0, 65000);

            char mask_params[MAXHASHSIZE];
            memcpy(mask_params, header.data(), HASHSIZE); //memset(mask_params, 0, MAXHASHSIZE);

            char *masks[MAXMASKCOUNT];
            for (int c = 0; c < MASKCOUNT; ++c)
            {
                masks[c] = mask_blob + c * HASHSIZE;
            }

            vector<Dimension> mutatedDimensions = DistinctHashCounter::parseMutations(srvInterface.getParamReader(), HASHSIZE, mask_params);
            bool hasMutations = mutatedDimensions.size() > 0;

            char mutation_mask[MAXHASHSIZE];
            memset(mutation_mask, 0xff, MAXHASHSIZE);
            initMutationMask(mutatedDimensions, mutation_mask);

            MinMutation minMutation(HASHSIZE, metrics);

            // buffer variables
            char fact_hash[MAXHASHSIZE * 2];
            char *blank_hash = fact_hash + MAXHASHSIZE;
            int hash_factor = hasMutations ? 2 : 1;

            DataBlock<int,METRIC_BLOCK_CAPACITY> nextBlock(true);
            vector<ResultTuple> local_hashes;
            local_hashes.reserve(HASHSIZE * MASKCOUNT);

            srvInterface.log("thread %d started", threadNo);

            bool isMultithreaded = threadCount > 1;

            int rowCount = 0;
            int blockSize = inputReader.getNumRows();
            bool blockEdge = true;

            bool hasMore = false;

            // process blocks
            do
            {
                vint cookie = inputReader.getIntRef(0);
                VString hash = inputReader.getStringRef(1);
                vint value;
                vint distinct_value;

                if ((int)hash.length() < HASHSIZE)
                {
                    vt_report_error(0, "Hash is too short");
                    break;
                }

                char *d = hash.data();

                for (int j = 0; j < MASKCOUNT; ++j)
                {
                    // apply mask
                    char *mask = masks[j];

                    int pno = 1;
                    int m_params = 0;
                    int d_params = 0;

                    i8 mutated_bytes;
                    mutated_bytes.i = 0xffffffff;
                    mutated_bytes.size = 0;

                    for (int i = 0; i < HASHSIZE; ++i)
                    {
                        fact_hash[i] = d[i] & mask[i];
                        blank_hash[i] = fact_hash[i] & mutation_mask[i];

                        if (mask_params[i] != pno)
                        {
                            d_params <<= 1;
                            m_params <<= 1;
                            pno = mask_params[i];
                        }
                        d_params |= (fact_hash[i] != 0 ? 1 : 0);
                        m_params |= (mask[i] != 0 ? 1 : 0);

                        if (hasMutations && mutation_mask[i] == 0)
                        {
                            mutated_bytes.b[mutated_bytes.size++] = fact_hash[i];
                        }
                    }

                    // check if mask fulfilled
                    if (d_params != m_params)
                    {
                        continue;
                    }

                    // put in hash table
                    DimHash probe(HASHSIZE, fact_hash);
                    probe.print = HashFunctor()(probe);

                    bool isExclusive = false;
                    isMultithreaded && sharedMutex.lock_shared();

                    auto gh = globalHashTable.find(probe, probe.print);
                    if (gh == globalHashTable.end())
                    {
                        isExclusive = true;
                        isMultithreaded && sharedMutex.unlock_shared();
                        isMultithreaded && sharedMutex.lock();

                        int *data = allocate<int>(METRIC_TUPLE_SIZE * threadCount, nextBlock);
                        char *hash = allocate<char>(HASHSIZE * hash_factor, nextBlock);
                        memcpy(hash, fact_hash, HASHSIZE * hash_factor);

                        DimHash h(HASHSIZE, hash);
                        h.print = probe.print;
                        gh = globalHashTable.emplace(make_pair(h, data)).first;
                    }

                    ResultTuple t(gh->first.hash, gh.value() + METRIC_TUPLE_SIZE * threadNo, METRICCOUNT);
                    isMultithreaded && (isExclusive ? sharedMutex.unlock() : sharedMutex.unlock_shared());
                    local_hashes.push_back(t);

                    if (nextBlock.d == 0)
                    {
                        nextBlock = std::move(DataBlock<int, METRIC_BLOCK_CAPACITY>(true));
                    }

                    // advance local counters
                    *t.counted = 0;
                    for (int k = 0; k < METRICCOUNT; ++k)
                    {
                        if (inputReader.isNull(metrics[k].input_ind)) continue;

                        value = inputReader.getIntRef(metrics[k].input_ind);
                        distinct_value = t.local_metrics[k] ? 0 : max(0, min((int)value, 1));
                        t.local_metrics[k] += metrics[k].is_additive ? value : distinct_value;
                    }

                    // apply mutation by picking right local hash and moving counters
                    if (hasMutations)
                    {
                        minMutation.apply(local_hashes, blank_hash, mutated_bytes);
                    }
                }

                // check if sorted block of data ended
                rowCount += 1;
                blockSize = inputReader.getNumRows();
                if (rowCount % blockSize == 0)
                {
                    blockEdge = true;
                    rowCount = 0;
                }

                // flush local counters on cookie change
                hasMore = inputReader.next();
                if (!hasMore || inputReader.getIntRef(0) != cookie || (isMultithreaded && rowCount % blockSize == 0))
                {
                    for (size_t i = 0; i < local_hashes.size(); ++i)
                    {
                        if (*local_hashes[i].counted) continue;
                        *local_hashes[i].counted = 1;

                        // check if cookie on block edge already counted in another thread
                        int cookieAlreadySeen = 0;
                        if (isMultithreaded && (!hasMore || blockEdge))
                        {
                            DimHash probe(HASHSIZE, local_hashes[i].hash);
                            probe.print = HashFunctor()(probe);
                            int value_mask = create_value_mask(local_hashes[i].local_metrics, METRICCOUNT);

                            mutex.lock();
                            tsl::hopscotch_map<DimHash, int, HashFunctor, EqualFunctor> &knownHashes = boundCookies[cookie];
                            int &m = knownHashes[probe];
                            cookieAlreadySeen = m;
                            m |= value_mask;
                            mutex.unlock();
                        }

                        // flush counters
                        int *final_metrics = local_hashes[i].final_metrics;
                        char *local_metrics = local_hashes[i].local_metrics;
                        for (int k = 0; k < METRICCOUNT; ++k)
                        {
                            bool is_duplicate = !metrics[k].is_additive && ((cookieAlreadySeen >> k) & 1);
                            final_metrics[k] += is_duplicate ? 0 : local_metrics[k];
                            local_metrics[k] = 0;
                        }
                    }
                    local_hashes.clear();
                    minMutation.reset();

                    if (rowCount % blockSize > 0)
                    {
                        blockEdge = false;
                    }
                }

            } while (hasMore);

            // print resulting hash table
            srvInterface.log("thread %d dump result", threadNo);
            for (auto it = globalHashTable.begin(); it != globalHashTable.end(); ++it)
            {
                VString &res = outputWriter.getStringRefNoClear(0);
                res.copy(it->first.hash, HASHSIZE);

                int *final_metrics = it.value() + METRIC_TUPLE_SIZE * threadNo;
                char *counted = (char*)(final_metrics + METRICCOUNT) + METRICCOUNT;
                if (*counted == 0) continue;

                for (int i = 0; i < METRICCOUNT; ++i)
                {
                    outputWriter.setInt(1 + i, final_metrics[i]);
                }
                outputWriter.next();
            }

            srvInterface.log("thread %d ended", threadNo);
        }
        catch(std::exception& e)
        {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }

    template<class type>
    static type *
    allocate(int count, DataBlock<int, METRIC_BLOCK_CAPACITY> &nextBlock)
    {
        int intCount = (count * sizeof(type) + sizeof(int) - 1) / sizeof(int);
        if (blockCursor + intCount >= METRIC_BLOCK_CAPACITY)
        {
            blocks.emplace_back(std::move(nextBlock));
            blockCursor = 0;
        }
        int *res = blocks.back().d + blockCursor;
        blockCursor += intCount;
        return reinterpret_cast<type*>(res);
    }

    void initMutationMask(const vector<Dimension> &mutatedDimensions, char *mutationMask)
    {
        for (size_t m = 0; m < mutatedDimensions.size(); ++m)
        {
            for (int i = 0; i < mutatedDimensions[m].size; ++i)
            {
                mutationMask[mutatedDimensions[m].pos + i] = 0;
            }
        }
    }

public:

    static vector<Dimension>
    parseMutations(ParamReader params, int hashSize, char *hashParams)
    {
        vector<Dimension> dimMutations;

        if (params.containsParameter("min_mutations"))
        {
            string minMutations = params.getStringRef("min_mutations").str();
            vector<string> mutations = splitString(minMutations, ',');

            for (auto m = mutations.begin(); m!= mutations.end(); ++m)
            {
                Dimension dim;
                dim.pos = -1;
                dim.size = -1;

                int dim_no = strtol(m->data(), 0, 16);
                for (int p = 0; p < hashSize; ++p)
                {
                    if (hashParams[p] == dim_no && dim.pos == -1)
                    {
                        dim.pos = p;
                        dim.size = 1;
                    }
                    else if(hashParams[p] == dim_no && dim.pos != -1)
                    {
                        dim.size += 1;
                    }
                }

                if (dim.pos != -1)
                {
                    dim.size = min(dim.size, 4);
                    dimMutations.push_back(dim);
                }
            }
        }

        return dimMutations;
    }

    static vector<Metric>
    parseCounters(ParamReader params, const SizedColumnTypes &inputColumns)
    {
        vector<Metric> metrics;

        if (params.containsParameter("additive_counters"))
        {
            vector<string> metric_names = splitString(params.getStringRef("additive_counters").str(), ',');
            for (unsigned i = 0; i < metric_names.size(); ++i)
            {
                int metric_col = -1;
                for (unsigned j = 0; j < inputColumns.getColumnCount(); ++j)
                {
                    if (inputColumns.getColumnName(j) == metric_names[i] && inputColumns.getColumnType(j).isInt())
                    {
                        metric_col = j;
                        break;
                    }
                }

                if (metric_col != -1)
                {
                    Metric m;
                    m.name = metric_names[i];
                    m.input_ind = metric_col;
                    m.is_additive = true;
                    metrics.push_back(m);
                }
            }
        }

        if (params.containsParameter("uniq_counters"))
        {
            vector<string> metric_names = splitString(params.getStringRef("uniq_counters").str(), ',');
            for (unsigned i = 0; i < metric_names.size(); ++i)
            {
                int metric_col = -1;
                for (unsigned j = 0; j < inputColumns.getColumnCount(); ++j)
                {
                    if (inputColumns.getColumnName(j) == metric_names[i] && inputColumns.getColumnType(j).isInt())
                    {
                        metric_col = j;
                        break;
                    }
                }

                if (metric_col != -1)
                {
                    Metric m;
                    m.name = metric_names[i];
                    m.input_ind = metric_col;
                    m.is_additive = false;
                    metrics.push_back(m);
                }
            }
        }

        if (metrics.size() == 0)
        {
            Metric m1;
            m1.name = string("pv");
            m1.input_ind = 2;//count column
            m1.is_additive = true;
            metrics.push_back(m1);

            Metric m2;
            m2.name = string("uv");
            m2.input_ind = 2;//count column
            m2.is_additive = false;
            metrics.push_back(m2);
        }

        return metrics;
    }

private:
    static int threadCount;
    static std::mutex mutex;
    static sf::contention_free_shared_mutex< > sharedMutex;

    static tsl::hopscotch_map<DimHash, int*, HashFunctor, EqualFunctor> globalHashTable;
    static tsl::hopscotch_map<vint, tsl::hopscotch_map<DimHash, int, HashFunctor, EqualFunctor> > boundCookies;

    static list<DataBlock<int, METRIC_BLOCK_CAPACITY> > blocks;
    static int blockCursor;

    int threadNo;
};


int DistinctHashCounter::threadCount = 0;
std::mutex DistinctHashCounter::mutex;
sf::contention_free_shared_mutex< > DistinctHashCounter::sharedMutex;

tsl::hopscotch_map<DimHash, int*, HashFunctor, EqualFunctor> DistinctHashCounter::globalHashTable;
tsl::hopscotch_map<vint, tsl::hopscotch_map<DimHash, int, HashFunctor, EqualFunctor> > DistinctHashCounter::boundCookies;

list<DataBlock<int, METRIC_BLOCK_CAPACITY> > DistinctHashCounter::blocks;
int DistinctHashCounter::blockCursor = 0;



class DistinctHashCounterFactory : public TransformFunctionFactory
{

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addInt();//cookie
        argTypes.addVarbinary();//hash
        argTypes.addInt();//count

        returnType.addVarbinary();//hash
        returnType.addInt();//pv
        returnType.addInt();//uv
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addIntOrderColumn("cookie");
        parameterTypes.addVarbinary(MAXHASHSIZE, "mask_header", SizedColumnTypes::Properties(false, true, false, "mask params"));
        parameterTypes.addVarbinary(65000, "mask_blob", SizedColumnTypes::Properties(false, true, false, "mask array"));
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
        outputTypes.addVarbinary(MAXHASHSIZE, "hash");
        outputTypes.addInt("pv");
        outputTypes.addInt("uv");
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, DistinctHashCounter);
    }

};

RegisterFactory(DistinctHashCounterFactory);


class DistinctHashCounterFactory2 : public TransformFunctionFactory
{

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addAny();//cookie(int) + hash(varbinary) + metrics
        returnType.addAny();//hash(varbinary) + metrics
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addIntOrderColumn("cookie");
        parameterTypes.addVarchar(4096, "uniq_counters", SizedColumnTypes::Properties(false, true, false, "uniq metrics comma separated"));
        parameterTypes.addVarchar(4096, "additive_counters", SizedColumnTypes::Properties(false, false, false, "additive metrics comma separated"));
        parameterTypes.addVarchar(4096, "min_mutations", SizedColumnTypes::Properties(false, false, false, "dimension number (from mask_header) comma separated"));
        parameterTypes.addVarbinary(MAXHASHSIZE, "mask_header", SizedColumnTypes::Properties(false, true, false, "mask-to-dimension breakdown"));
        parameterTypes.addVarbinary(65000, "mask_blob", SizedColumnTypes::Properties(false, true, false, "mask array"));
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
        outputTypes.addVarbinary(MAXHASHSIZE, "hash");
        vector<Metric> metrics = DistinctHashCounter::parseCounters(srvInterface.getParamReader(), inputTypes);
        for (unsigned i = 0; i < metrics.size(); ++i)
        {
            outputTypes.addInt(metrics[i].name);
        }
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, DistinctHashCounter);
    }

};

RegisterFactory(DistinctHashCounterFactory2);
