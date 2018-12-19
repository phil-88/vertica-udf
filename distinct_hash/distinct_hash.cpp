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
#include <array>
#include <vector>
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

struct Dimention
{
    int pos;
    int size;
};


struct DimTuple
{
    char *hash;
    size_t mutated_print;
    int *metrics;
    int *local_metrics;
    int *counted;
};

struct DimMetrics
{
    int event;
    int *metrics;
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


class DistinctHashCounter : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter)
    {
        try {

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

            Dimention *mutations[MAXHASHSIZE];
            memset(mutations, 0, MAXHASHSIZE * sizeof(void*));

            char mutation_mask[MAXHASHSIZE];
            memset(mutation_mask, 0xff, MAXHASHSIZE);

            vector<Dimention> mutatedDimentions = DistinctHashCounter::parseMutations(srvInterface.getParamReader(), HASHSIZE, mask_params);
            for (size_t m = 0; m < mutatedDimentions.size(); ++m)
            {
                for (int i = 0; i < mutatedDimentions[m].size; ++i)
                {
                    mutations[mutatedDimentions[m].pos + i] = &mutatedDimentions[m];
                    mutation_mask[mutatedDimentions[m].pos + i] = 0;
                }
            }

            bool hasMutations = mutatedDimentions.size() > 0;

            // buffer variables
            char fact_hash[MAXHASHSIZE];
            char mutated_hash[MAXHASHSIZE];

            vector<DimTuple> local_hashes;
            local_hashes.reserve(HASHSIZE * MASKCOUNT);

            map<DimHash, map<int, ArgMin>, LessFunctor> mutation_dups; //hash -> metric -> (min,argmin)

            tsl::hopscotch_map<DimHash, DimMetrics, HashFunctor, EqualFunctor> hashes;

            list<std::array<int, METRIC_BLOCK_CAPACITY> > metric_blocks;
            metric_blocks.push_back(std::array<int, METRIC_BLOCK_CAPACITY>());
            int metric_block_cursor = 0;

            int event = 0;
            bool hasMore = false;

            // process blocks
            do {
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
                        mutated_hash[i] = fact_hash[i] & mutation_mask[i];

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

                    auto gh = hashes.find(probe, probe.print);
                    if (gh == hashes.end())
                    {
                        DimMetrics m;
                        m.event = -1;
                        m.metrics = this->allocate<int>(METRICCOUNT * 2 + 1, &metric_blocks, &metric_block_cursor);

                        DimHash h(HASHSIZE);
                        h.hash = this->allocate<char>(HASHSIZE * 2, &metric_blocks, &metric_block_cursor);
                        h.print = probe.print;
                        memcpy(h.hash, probe.hash, HASHSIZE);
                        memcpy(h.hash + HASHSIZE, mutated_hash, HASHSIZE);

                        gh = hashes.emplace(make_pair(h, m)).first;
                    }

                    // skip dups
                    if (gh->second.event == event)
                    {
                        continue;
                    }

                    DimTuple t;
                    t.hash = gh->first.hash;
                    t.metrics = gh.value().metrics;
                    t.local_metrics = gh.value().metrics + METRICCOUNT;
                    t.counted = gh.value().metrics + 2 * METRICCOUNT;
                    local_hashes.push_back(t);

                    // advance local counters
                    gh.value().event = event;
                    *t.counted = 0;
                    for (int k = 0; k < METRICCOUNT; ++k)
                    {
                        if (inputReader.isNull(metrics[k].input_ind)) continue;

                        value = inputReader.getIntRef(metrics[k].input_ind);
                        distinct_value = t.local_metrics[k] ? 0 : max(0, min((int)value, 1));
                        t.local_metrics[k] += metrics[k].is_additive ? value : distinct_value;
                    }

                    // reduce duplicated uniqs
                    if (hasMutations)
                    {
                        map<int, ArgMin> *m = NULL;

                        for (int k = 0; k < METRICCOUNT; ++k)
                        {
                            if (mutated_bytes.i == 0xffffffff && mutation_dups.size() == 0) continue;
                            if (metrics[k].is_additive) continue;
                            if (local_hashes.back().local_metrics[k] == 0) continue;

                            if (mutation_dups.size() == 0)
                            {
                                for (size_t i = 0; i < local_hashes.size() - 1; ++i)
                                {
                                    DimHash deferred(HASHSIZE, local_hashes[i].hash + HASHSIZE);
                                    deferred.print = HashFunctor()(deferred);
                                    map<int, ArgMin> &hash_mutations = mutation_dups[deferred];

                                    for (int j = 0; j < METRICCOUNT; ++j)
                                    {
                                        if (!metrics[j].is_additive && local_hashes[i].local_metrics[j] != 0)
                                        {
                                            hash_mutations.insert(make_pair(j, ArgMin(0xffffffff, i)));
                                        }
                                    }
                                }
                            }

                            if (m == NULL)
                            {
                                DimHash mutation(HASHSIZE, t.hash + HASHSIZE);
                                mutation.print = HashFunctor()(mutation);
                                m = &mutation_dups[mutation]; // metric -> (min,argmin)
                            }

                            auto p = m->find(k);
                            if (p == m->end())
                            {
                                m->insert(make_pair(k, ArgMin(mutated_bytes.i, local_hashes.size() - 1)));
                            }
                            else if (p->second.min < mutated_bytes.i)
                            {
                                local_hashes.back().local_metrics[k] = 0;
                            }
                            else if (p->second.min > mutated_bytes.i)
                            {
                                local_hashes[p->second.arg].local_metrics[k] = 0;
                                p->second = ArgMin(mutated_bytes.i, local_hashes.size() - 1);
                            }
                        }
                    }
                }

                ++event;

                //flush local counters on cookie change
                hasMore = inputReader.next();
                if (!hasMore || inputReader.getIntRef(0) != cookie)
                {
                    for (size_t i = 0; i < local_hashes.size(); ++i)
                    {
                        if (*local_hashes[i].counted) continue;
                        *local_hashes[i].counted = 1;

                        int *global_metrics = local_hashes[i].metrics;
                        int *local_metrics = local_hashes[i].local_metrics;
                        for (int k = 0; k < METRICCOUNT; ++k)
                        {
                            global_metrics[k] += local_metrics[k];
                            local_metrics[k] = 0;
                        }
                    }
                    local_hashes.clear();
                    mutation_dups.clear();
                }

            } while (hasMore);

            for (auto it = hashes.begin(); it != hashes.end(); ++it)
            {
                VString &res = outputWriter.getStringRefNoClear(0);
                res.copy(it->first.hash, HASHSIZE);
                DimMetrics &s = it.value();
                for (int i = 0; i < METRICCOUNT; ++i)
                {
                    outputWriter.setInt(1 + i, s.metrics[i]);
                }
                outputWriter.next();
            }

        }
        catch(std::exception& e)
        {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }

    template<class type>
    type *
    allocate(int count, list<array<int, METRIC_BLOCK_CAPACITY> > *blocks, int *blockCursor)
    {
        int intCount = (count * sizeof(type) + sizeof(int) - 1) / sizeof(int);
        if (*blockCursor + intCount >= METRIC_BLOCK_CAPACITY)
        {
            blocks->push_back(std::array<int, METRIC_BLOCK_CAPACITY>());
            *blockCursor = 0;
        }
        int *res = blocks->back().data() + *blockCursor;
        *blockCursor += intCount;
        return reinterpret_cast<type*>(res);
    }

public:

    static vector<Dimention>
    parseMutations(ParamReader params, int hashSize, char *hashParams)
    {
        vector<Dimention> dimMutations;

        if (params.containsParameter("min_mutations"))
        {
            string minMutations = params.getStringRef("min_mutations").str();
            vector<string> mutations = splitString(minMutations, ',');

            for (auto m = mutations.begin(); m!= mutations.end(); ++m)
            {
                Dimention dim;
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

};


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
        parameterTypes.addVarchar(4096, "min_mutations", SizedColumnTypes::Properties(false, false, false, "dimention number (from mask_header) comma separated"));
        parameterTypes.addVarbinary(MAXHASHSIZE, "mask_header", SizedColumnTypes::Properties(false, true, false, "mask-to-dimention breakdown"));
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


vector<int>
getBinaryPartSizes(ParamReader params, size_t colCount = 0)
{
    vector<int> sizes;
    if (params.containsParameter("sizes"))
    {
        vector<string> size_param = splitString(params.getStringRef("sizes").str(), ',');
        if (colCount <= 0)
        {
            colCount = size_param.size();
        }
        for (unsigned i = 0; i < min(colCount, size_param.size()); ++i)
        {
            sizes.push_back(stoi(size_param[i]));
        }
    }
    else if (colCount <= 0)
    {
        colCount = 1;
    }

    unsigned left = colCount - sizes.size();
    for (unsigned i = 0; i < left; ++i)
    {
        sizes.push_back(8);
    }
    return sizes;
}


class Int2Binary : public ScalarFunction
{
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &arg_reader,
                              BlockWriter &res_writer)
    {
        try
        {
            size_t cnt = arg_reader.getTypeMetaData().getColumnCount();
            vector<int> sizes = getBinaryPartSizes(srvInterface.getParamReader(), cnt);
            int total_size = 0;
            for (unsigned i = 0; i < cnt; ++i)
            {
                total_size += sizes[i];
            }

            if (total_size > 128)
            {
                vt_report_error(0, "to many arguments");
            }

            char buffer[128];
            int offset;
            const char *null_buffer = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";

            do
            {
                memset(buffer, 0, total_size);
                offset = 0;

                for (unsigned i = 0; i < cnt; ++i)
                {
                    const char *b = arg_reader.isNull(i) ? null_buffer : (const char*)&arg_reader.getIntRef(i);
                    reverse_copy(b, b + sizes[i], buffer + offset);
                    offset += sizes[i];
                }

                res_writer.getStringRef().copy(buffer, total_size);
                res_writer.next();
            }
            while (arg_reader.next());
        }
        catch(std::exception& e)
        {
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};


class Int2BinaryFactory : public ScalarFunctionFactory
{
public:
    Int2BinaryFactory() : ScalarFunctionFactory()
    {
        this->vol = IMMUTABLE;
    }

private:
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    {
        return vt_createFuncObj(interface.allocator, Int2Binary);
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(256, "sizes");
    }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addVarbinary();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
        vector<int> sizes = getBinaryPartSizes(srvInterface.getParamReader(), inputTypes.getColumnCount());

        int total_size = 0;
        for (unsigned i = 0; i < inputTypes.getColumnCount(); ++i)
        {
            if (!inputTypes.getColumnType(i).isInt())
            {
                vt_report_error(0, "Only integer columns supported");
            }
            total_size += sizes[i];
        }

        outputTypes.addVarbinary(total_size, "bin");
    }
};

RegisterFactory(Int2BinaryFactory);
