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

//#include "backtrace.h"
//INSTALL_SIGSEGV_TRAP

using namespace Vertica;
using namespace std;


struct Metric
{
    string name;
    int input_ind;
    bool is_additive;
};

struct DimHash
{
    DimHash(char sz = 0, char *d = 0)
        : print(0), size(sz), hash(d) {}

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

struct ResultTuple
{   
    ResultTuple(char *hash = 0, int *data = 0, int metricCount = 0)
        : hash(hash),
          final_metrics(data),
          local_metrics((char*)(data + metricCount)),
          counted((char*)(data + metricCount) + metricCount) {}

    char *hash;
    int  *final_metrics;
    char *local_metrics;
    char *counted;

    static inline char is_counted(int *data, int metricCount)
    {
        return *((char*)(data + metricCount) + metricCount);
    }
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

void toHex(const char *b, char *s, int size = 16)
{
    static const char digits[513] =
        "000102030405060708090a0b0c0d0e0f"
        "101112131415161718191a1b1c1d1e1f"
        "202122232425262728292a2b2c2d2e2f"
        "303132333435363738393a3b3c3d3e3f"
        "404142434445464748494a4b4c4d4e4f"
        "505152535455565758595a5b5c5d5e5f"
        "606162636465666768696a6b6c6d6e6f"
        "707172737475767778797a7b7c7d7e7f"
        "808182838485868788898a8b8c8d8e8f"
        "909192939495969798999a9b9c9d9e9f"
        "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
        "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
        "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
        "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
        "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
        "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

    char *lut = (char *)(digits);
    for (int i = 0; i < size; ++i)
    {
        int pos = (b[i] & 0xFF) * 2;

        s[i * 2] = lut[pos];
        s[i * 2 + 1] = lut[pos + 1];
    }
}

class DistinctHashCounter : public TransformFunction
{
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
    {
        mutex.lock();
        if (threadCount == 0)
        {
            partitionBoundCookies.clear();
            threadPartition.clear();
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
            partitionBoundCookies.clear();
            threadPartition.clear();
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

            int payloadColumn = -1;
            if (inputColumns.getColumnType(0).isVarbinary())
            {
                payloadColumn = 0;
            }

            if (!inputColumns.getColumnType(payloadColumn + 1).isInt())
            {
                vt_report_error(0, "First column expected to be ordered identifier of type integer");
            }
            if (!inputColumns.getColumnType(payloadColumn + 2).isVarbinary())
            {
                vt_report_error(0, "Second column expected to be feature hash of type varbinary");
            }
            for (size_t c = payloadColumn + 3; c < inputColumns.getColumnCount(); ++c)
            {
                if (!inputColumns.getColumnType(c).isInt())
                {
                    vt_report_error(0, "Metric column expected to be integer");
                }
            }

            bool hasPartitions = inputColumns.getLastPartitionColumnIdx() > -1;

            // parse params
            vector<Metric> metrics = DistinctHashCounter::parseCounters(srvInterface.getParamReader(),
                                                                        inputReader.getTypeMetaData());
            int METRICCOUNT = min(MAXMETRICCOUNT, (int)metrics.size());
            int METRIC_TUPLE_SIZE = METRICCOUNT + METRICCOUNT / sizeof(int) + 1;

            ParamReader paramReader = srvInterface.getParamReader();
            VString header = paramReader.getStringRef("mask_header");
            VString blob = paramReader.getStringRef("mask_blob");

            int HASHSIZE = min(MAXHASHSIZE, (int) header.length());
            int MASKCOUNT = min(MAXMASKCOUNT, (int) (blob.length() / header.length()));

            char mask_blob[65000];
            memcpy(mask_blob, blob.data(), HASHSIZE * MASKCOUNT);

            char mask_params[MAXHASHSIZE];
            memcpy(mask_params, header.data(), HASHSIZE);

            char *masks[MAXMASKCOUNT];
            for (int c = 0; c < MASKCOUNT; ++c)
            {
                masks[c] = mask_blob + c * HASHSIZE;
            }

            // buffer variables
            char fact_hash[MAXHASHSIZE * 2];

            DataBlock<int, METRIC_BLOCK_CAPACITY> nextBlock(true);
            vector<ResultTuple> local_hashes;
            local_hashes.reserve(HASHSIZE * MASKCOUNT);

            srvInterface.log("thread %d started", threadNo);

            bool isMultithreaded = threadCount > 1;

            int blockSize = inputReader.getNumRows();
            int blockRow = 0;
            bool blockEdge = true;

            bool hasMore = false;

            // process blocks
            do
            {
                std::string payload = payloadColumn == 0 ? inputReader.getStringRef(0).str() : std::string();
                vint cookie = inputReader.getIntRef(payloadColumn + 1);
                VString hash = inputReader.getStringRef(payloadColumn + 2);
                vint value;

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

                    for (int i = 0; i < HASHSIZE; ++i)
                    {
                        fact_hash[i] = d[i] & mask[i];

                        if (mask_params[i] != pno)
                        {
                            d_params <<= 1;
                            m_params <<= 1;
                            pno = mask_params[i];
                        }
                        d_params |= (fact_hash[i] != 0 ? 1 : 0);
                        m_params |= (mask[i] != 0 ? 1 : 0);
                    }

                    // check if mask fulfilled to avoid double counting as we dont store cookie in a hash table
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
                        char *hash = allocate<char>(HASHSIZE, nextBlock);
                        memcpy(hash, fact_hash, HASHSIZE);

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
                        if (metrics[k].is_additive) // add value for additive
                            t.local_metrics[k] += value;
                        else if (t.local_metrics[k] == 0) // +1 if not seen yet
                            t.local_metrics[k] += max(0, min((int)value, 1));
                    }
                }

                blockSize = inputReader.getNumRows();
                hasMore = inputReader.next();

                // check if physical block ended
                blockRow = (blockRow + 1) % blockSize;
                bool blockChanged = (!hasMore || blockRow == 0);

                if (blockChanged)
                {
                    blockEdge = true;
                }

                // check if logical block ended
                bool payloadChanged = !hasMore || (payloadColumn == 0 && inputReader.getStringRef(0).str() != payload);
                bool cookieChanged = !hasMore || inputReader.getIntRef(payloadColumn + 1) != cookie;

                if (payloadChanged)
                {
                    blockEdge = true;
                }

                // flush local counters on cookie change
                if (cookieChanged || payloadChanged || (!hasPartitions && isMultithreaded && blockChanged))
                {
                    for (size_t i = 0; i < local_hashes.size(); ++i)
                    {
                        if (*local_hashes[i].counted) continue;
                        *local_hashes[i].counted = 1;

                        // check if cookie on block edge already counted in another thread
                        int cookieAlreadySeen = 0;
                        if (!hasPartitions && isMultithreaded && blockEdge)
                        {
                            int value_mask = create_value_mask(local_hashes[i].local_metrics, METRICCOUNT);

                            mutex.lock();
                            SharedPartition &p = partitionBoundCookies[payload];
                            tsl::hopscotch_map<char*, int> &knownHashes = p[cookie];
                            int &m = knownHashes[local_hashes[i].hash];
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

                    if (cookieChanged && !payloadChanged && !blockChanged)
                    {
                        blockEdge = false;
                    }
                }

                // print hash table on partition/payload change
                if (payloadChanged)
                {
                    srvInterface.log("thread %d dump partition", threadNo);

                    isMultithreaded && sharedMutex.lock_shared();
                    for (auto it = globalHashTable.begin(); it != globalHashTable.end(); ++it)
                    {
                        if (payloadColumn != -1)
                        {
                            VString &res = outputWriter.getStringRefNoClear(0);
                            res.copy(payload);
                        }
                        VString &res = outputWriter.getStringRefNoClear(payloadColumn + 1);
                        res.copy(it->first.hash, HASHSIZE);

                        int *final_metrics = it.value() + METRIC_TUPLE_SIZE * threadNo;
                        if (ResultTuple::is_counted(final_metrics, METRICCOUNT) == 0) continue;

                        for (int i = 0; i < METRICCOUNT; ++i)
                        {
                            outputWriter.setInt(payloadColumn + 2 + i, final_metrics[i]);
                        }
                        outputWriter.next();

                        memset(final_metrics, 0, METRIC_TUPLE_SIZE * sizeof(int));
                    }
                    isMultithreaded && sharedMutex.unlock_shared();

                    // clear early partitions
                    if (!hasPartitions && isMultithreaded)
                    {
                        mutex.lock();
                        threadPartition[threadNo] = payload;

                        std::string minPayload = payload;
                        for (auto it = threadPartition.begin(); it != threadPartition.end(); ++it)
                        {
                            minPayload = std::min(it.value(), minPayload);
                        }

                        for (auto it = partitionBoundCookies.begin(); it != partitionBoundCookies.end(); ++it)
                        {
                            if (it.key() < minPayload)
                            {
                                partitionBoundCookies.erase(it);
                            }
                        }

                        mutex.unlock();
                    }
                }

            } while (hasMore);            

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

public:

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
    int threadNo;

    static tsl::hopscotch_map<DimHash, int*, HashFunctor, EqualFunctor> globalHashTable;
    typedef tsl::hopscotch_map<vint, tsl::hopscotch_map<char*, int> > SharedPartition;
    static tsl::hopscotch_map<std::string, SharedPartition> partitionBoundCookies;
    static tsl::hopscotch_map<int, std::string> threadPartition;

    static list<DataBlock<int, METRIC_BLOCK_CAPACITY> > blocks;
    static int blockCursor;
};


int DistinctHashCounter::threadCount = 0;
std::mutex DistinctHashCounter::mutex;
sf::contention_free_shared_mutex< > DistinctHashCounter::sharedMutex;

tsl::hopscotch_map<DimHash, int*, HashFunctor, EqualFunctor> DistinctHashCounter::globalHashTable;
tsl::hopscotch_map<std::string, DistinctHashCounter::SharedPartition> DistinctHashCounter::partitionBoundCookies;
tsl::hopscotch_map<int, std::string> DistinctHashCounter::threadPartition;

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
        argTypes.addAny(); //payload(varbinary) + cookie(int) + features(varbinary) + metrics
        returnType.addAny(); //hash(varbinary) + features(varbinary) + metrics
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(4096, "uniq_counters", SizedColumnTypes::Properties(false, true, false, "uniq metrics comma separated"));
        parameterTypes.addVarchar(4096, "additive_counters", SizedColumnTypes::Properties(false, false, false, "additive metrics comma separated"));
        parameterTypes.addVarbinary(MAXHASHSIZE, "mask_header", SizedColumnTypes::Properties(false, true, false, "mask-to-dimension breakdown"));
        parameterTypes.addVarbinary(65000, "mask_blob", SizedColumnTypes::Properties(false, true, false, "mask array"));
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
        if (inputTypes.getColumnType(0).isVarbinary())
        {
            outputTypes.addVarbinary(inputTypes.getColumnType(0).getMaxSize(), "payload");
        }
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
