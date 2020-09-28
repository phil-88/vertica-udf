
#include "Vertica.h"

#include <iostream>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <ctime>
#include <regex>
#include <iomanip>
#include "tsl/hopscotch_map.h"
#include "cppkafka/consumer.h"
#include "cppkafka/configuration.h"

#define JSMN_STATIC
#define JSMN_PARENT_LINKS
#include "jsmn.h"

#define NOTINT LONG_LONG_MAX
#define ALL_PARTITIONS -1
#define REBALANCE_PARTITIONS -2
#define REBALANCE_AUTOCOMMIT true

#define POLL_TIMEOUT 30000
#define CUMULATIVE_POLL_TIMEOUT 90000

//#include "backtrace.h"
//INSTALL_SIGSEGV_TRAP


using namespace Vertica;
using namespace std;
using namespace cppkafka;


std::vector<std::string> splitString(std::string s, char delim)
{
    std::vector<std::string> res;
    std::istringstream f(s);
    std::string p;
    while (getline(f, p, delim))
    {
        res.push_back(p);
    }
    return res;
}

int64 toInt(std::string s, int64 defultValue = NOTINT)
{
    std::size_t const sign = s.find_first_of("-");
    std::size_t const b = s.find_first_of("0123456789");
    if (b != std::string::npos)
    {
        std::size_t const e = s.find_first_not_of("0123456789", b);
        return (sign != std::string::npos ? -1 : 1) * stoll(s.substr(b, e != std::string::npos ? e - b : e));
    }
    return defultValue;
}

void intToHex(uint32 x, char *s, int size = 4)
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
    int i = size - 1;
    char *lut = (char *)(digits);
    while (i >= 0)
    {
        int pos = (x & 0xFF) * 2;

        s[i * 2] = lut[pos];
        s[i * 2 + 1] = lut[pos + 1];

        x >>= 8;
        i -= 1;
    }
}

char hex2byte(char *h)
{
    char a = (h[0] <= '9') ? h[0] - '0' : (h[0] & 0x7) + 9;
    char b = (h[1] <= '9') ? h[1] - '0' : (h[1] & 0x7) + 9;
    return (a << 4) + b;
}

int unescape(char *buf, int len)
{
    int in, out;
    for (in = 0, out = 0; in < len; ++in && ++out)
    {
        if (buf[in] == '\\' && in + 1 < len)
        {
            ++in;
            switch (buf[in])
            {
            case 't':
                buf[out] = '\t';
                break;
            case 'b':
                buf[out] = '\b';
                break;
            case 'f':
                buf[out] = '\f';
                break;
            case 'n':
                buf[out] = '\n';
                break;
            case 'r':
                buf[out] = '\r';
                break;
            case '\\':
                buf[out] = '\\';
                break;
            case '"':
                buf[out] = '"';
                break;
            case 'u':
                if (in + 4 < len
                    && buf[in + 1] == '0'
                    && buf[in + 2] == '0'
                    && buf[in + 3] >= '0' && buf[in + 3] < '8'
                    && ((buf[in + 4] >= '0' && buf[in + 4] <= '9') ||
                        (buf[in + 4] >= 'a' && buf[in + 4] <= 'f') ||
                        (buf[in + 4] >= 'A' && buf[in + 4] <= 'F')))
                {
                    buf[out] = hex2byte(buf + in + 3);
                    in += 4;
                    break;
                }
            default:
                buf[out++] = '\\';
                buf[out] = buf[in];
            }
        }
        else if (out < in)
        {
            buf[out] = buf[in];
        }
    }
    return out;
}


class Sink
{
public:
    virtual int put(Message &doc, char *buf, int size) = 0;
};


int align256(int v, int s, uint8 base)
{
    return (v + s) <= base ? v : ((v + base - 1) / base) * base;
}

int encode256(int v, uint8 base)
{
    if (v < base)
    {
        return v + 1;
    }
    else if (v <= 127 * base)
    {
        return 128 | (v / base);
    }
    return 0;
}

enum OutputFormat
{
    OUTPUT_JSON,
    OUTPUT_EAV,
    OUTPUT_ARRAY,
    OUTPUT_COLUMNS
};


enum HeaderFields
{
    HEADER_PARTITION,
    HEADER_OFFSET,
    HEADER_KEY,
    HEADER_TIMESTAMP
};


class CSVSink : public Sink
{
    const std::string delimiter, terminator;
    std::vector<int> headerFields;
    int formatType;

    tsl::hopscotch_map<std::string, int, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<std::string, int> >, 30, true, tsl::power_of_two_growth_policy> fieldIndex;
    std::vector<std::string> fieldName;
    std::vector<int> fieldGroup;

    std::vector<int> fieldReorderDirect;
    std::vector<int> fieldReorderReverse;

    int fieldGroupCount;
    std::vector<int> groupFieldCount;

    std::string lastLine;
public:

    CSVSink(std::string format, std::string delimiter, std::string terminator)
        : delimiter(delimiter), terminator(terminator)
    {
        formatType = OUTPUT_JSON;

        for (std::string header : splitString(format, ';'))
        {
            // kafka headers
            if (header == std::string("partition"))
            {
                headerFields.push_back(HEADER_PARTITION);
            }
            else if (header == std::string("offset"))
            {
                headerFields.push_back(HEADER_OFFSET);
            }
            else if (header == std::string("key"))
            {
                headerFields.push_back(HEADER_KEY);
            }
            else if (header == std::string("timestamp"))
            {
                headerFields.push_back(HEADER_TIMESTAMP);
            }
            // value format
            else if (header == std::string("json"))
            {
                formatType = OUTPUT_JSON;
            }
            else if (header == std::string("eav"))
            {
                formatType = OUTPUT_EAV;
            }
            else if (header.substr(0, 5) == std::string("array"))
            {
                formatType = OUTPUT_ARRAY;
                parseFields(header.substr(6), true);
            }
            else if (header.substr(0, 7) == std::string("columns"))
            {
                formatType = OUTPUT_COLUMNS;
                parseFields(header.substr(8), false);
            }
        }
    }

    void parseFields(const std::string &format, bool defaultGroupping)
    {
        std::vector<std::string> groups;
        std::unordered_map<std::string, std::vector<std::string> > groupFields;

        std::vector<std::string> keys = splitString(format, ',');
        for (size_t j = 0; j < keys.size(); ++j)
        {
            std::vector<std::string> keyParts = splitString(keys[j], '=');
            std::string key = keyParts[0];

            std::string group = keyParts.size() > 1 ? keyParts[1] : (defaultGroupping ? "" : key);
            auto it = groupFields.find(group);
            if (it == groupFields.end())
            {
                it = groupFields.insert(std::make_pair(group, std::vector<std::string>())).first;
                groups.push_back(group);
            }
            it->second.push_back(key);

            fieldIndex[key] = j;
        }

        fieldReorderDirect = std::vector<int>(keys.size(), -1);
        fieldReorderReverse = std::vector<int>(keys.size(), -1);

        for (size_t k = 0; k < groups.size(); ++k)
        {
            std::vector<std::string> fields = groupFields[groups[k]];
            for (size_t f = 0; f < fields.size(); ++f)
            {
                fieldReorderDirect[fieldIndex[fields[f]]] = fieldName.size();
                fieldReorderReverse[fieldName.size()] = fieldIndex[fields[f]];

                fieldIndex[fields[f]] = fieldName.size();
                fieldName.push_back(fields[f]);
                fieldGroup.push_back(k);
            }
            groupFieldCount.push_back(fields.size());
        }

        fieldGroupCount = groups.size();
    }

    int put(Message &doc, char *buf, int size)
    {
        if (lastLine.empty())
        {
            lastLine = toRecord(doc);
        }
        int lastLineSize = lastLine.size();
        if (lastLineSize > size)
        {
            return -1;
        }
        memcpy(buf, lastLine.data(), lastLineSize);
        lastLine.clear();
        return lastLineSize;
    }

    std::string toRecord(Message &doc)
    {
        if (formatType == OUTPUT_JSON)
        {
            return toJSONRecord(doc);
        }
        else if (formatType == OUTPUT_EAV)
        {
            return toEAVRecord(doc);
        }
        else if (formatType == OUTPUT_ARRAY)
        {
            return toArrayRecordComp(doc);
        }
        else if (formatType == OUTPUT_COLUMNS)
        {
            return toSparseRecord(doc);
        }
        return std::string();
    }

    inline std::string toRecordHeader(Message &doc)
    {
        std::ostringstream s;

        for (int headerField : headerFields)
        {
            if (headerField == HEADER_PARTITION)
            {
                s << doc.get_partition();
                s << delimiter;
            }
            else if (headerField == HEADER_OFFSET)
            {
                s << doc.get_offset();
                s << delimiter;
            }
            else if (headerField == HEADER_KEY)
            {
                s << doc.get_key();
                s << delimiter;
            }
            else if (headerField == HEADER_TIMESTAMP)
            {
                s << rd_kafka_message_timestamp(doc.get_handle(), NULL);
                s << delimiter;
            }
        }

        return s.str();
    }

    std::string toSparseRecord(Message &doc)
    {
        int totalSize = 0;
        const int fieldCount = fieldName.size();
        std::vector<std::pair<const char*, int> > values(fieldCount, make_pair("", 0));

        jsmn_parser p;
        jsmntok_t t[4098];

        int tokenLevel[4098];
        tokenLevel[0] = 0;
        const int maxLevel = 3;

        jsmn_init(&p);
        const char *src = (const char *)doc.get_payload().get_data();
        int len = doc.get_payload().get_size();
        int r = jsmn_parse(&p, src, len, t, 4098);
        for (int i = 1; i < r - 1; ++i)
        {
            tokenLevel[i] = tokenLevel[t[i].parent] + 1;

            if (t[i].type == JSMN_STRING && tokenLevel[i] <= maxLevel && t[i + 1].parent == i)
            {
                std::string key(src + t[i].start, t[i].end - t[i].start);
                int par = t[t[i].parent].parent;
                while (par > 0)
                {
                    key = std::string(src + t[par].start, t[par].end - t[par].start) + "." + key;
                    par = t[t[par].parent].parent;
                }

                auto found = fieldIndex.find(key);
                if (found != fieldIndex.end())
                {
                    int ind = found->second & 0xffff;
                    values[ind] = make_pair(src + t[i + 1].start, t[i + 1].end - t[i + 1].start);
                    totalSize += t[i + 1].end - t[i + 1].start;
                }

                i += 1;
                tokenLevel[i] = tokenLevel[t[i].parent] + 1;
            }
        }

        const char arrayDelim = ',';
        const char csvDelim = delimiter[0];
        const char defaultDelim = arrayDelim;

        static const int sizeMax = 65000;
        static char buf[sizeMax];
        memset(buf, defaultDelim, min(sizeMax, totalSize + fieldCount + fieldGroupCount * 2));
        int shift = 0;

        int group = -1;
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (fieldGroup[i] != group)
            {
                if (groupFieldCount[group] > 1)
                {
                    buf[shift - 1] = '}';
                    if (defaultDelim != csvDelim)
                    {
                        buf[shift] = csvDelim;
                    }
                    shift += 1;
                }

                if (groupFieldCount[fieldGroup[i]] > 1)
                {
                    buf[shift] = '{';
                    shift += 1;
                }
                group = fieldGroup[i];
            }


            if (values[i].second > 0 && values[i].second + (fieldCount - i + group * 2) < sizeMax)
            {
                memcpy(buf + shift, values[i].first, values[i].second);
                shift += values[i].second;
            }

            if (defaultDelim != arrayDelim && groupFieldCount[group] > 1)
            {
                buf[shift] = arrayDelim;
            }
            else if (defaultDelim != csvDelim && groupFieldCount[group] <= 1)
            {
                buf[shift] = csvDelim;
            }
            shift += 1;
        }
        if (groupFieldCount[group] > 1)
        {
            buf[shift - 1] = '}';
            shift += 1;
        }

        std::string res = toRecordHeader(doc);
        res += std::string(buf, max(0, shift - 1));
        return res + terminator;
    }

    std::string toArrayRecordComp(Message &doc)
    {
        static const int BUF_SIZE = 65000;
        static char buf[BUF_SIZE];
        int bufOffset = 0;
        const char delim = delimiter[0];
        const int fieldCount = fieldName.size();

        const char *src = (const char *)doc.get_payload().get_data();
        int len = doc.get_payload().get_size();

        std::string header(std::move(toRecordHeader(doc)));

        // rough size check
        int rowSize = header.size() + (2 * fieldCount) + (fieldGroupCount + len) + 1;
        if (rowSize > BUF_SIZE)
        {
            return std::string();
        }

        // parse json
        std::vector<std::pair<const char*, int> > values(fieldCount, make_pair("", 0));

        std::vector<int> presentIndexes;
        presentIndexes.reserve(fieldCount);

        jsmn_parser p;
        jsmntok_t t[4098];

        jsmn_init(&p);
        int r = jsmn_parse(&p, src, len, t, 4098);
        for (int i = 1; i < r - 1; ++i)
        {
            if (t[i].type == JSMN_STRING && t[i].parent == 0 && t[i + 1].parent == i)
            {
                std::string key(src + t[i].start, t[i].end - t[i].start);

                auto found = fieldIndex.find(key);
                if (found != fieldIndex.end())
                {
                    int ind = found->second & 0xffff;

                    char * s = (char*)src + t[i + 1].start;
                    int size = t[i + 1].end - t[i + 1].start;

                    if (t[i + 1].type == JSMN_STRING)
                    {
                        size = unescape(s, size);
                    }
                    if (size == 4 && strncmp(s, "null", 4) == 0)
                    {
                        size = 0;
                    }
                    if (size == 0)
                    {
                        continue;
                    }
                    values[ind] = make_pair(s, size);

                    presentIndexes.push_back(ind);
                    if (ind + 1 < (int)values.size() && fieldGroup[ind] == fieldGroup[ind + 1])
                    {
                        presentIndexes.push_back(ind + 1);
                    }
                }
            }
        }
        std::sort(presentIndexes.begin(), presentIndexes.end());
        presentIndexes.erase(std::unique(presentIndexes.begin(), presentIndexes.end()), presentIndexes.end());

        // format header
        memcpy(buf + bufOffset, header.data(), header.size());
        bufOffset += header.size();

        // format index
        int indexCapacity = int(values.size());
        memset(buf + bufOffset, '0', indexCapacity * 2);

        int valueSizeTotal = 0;
        std::vector<int> groupSize(fieldGroupCount, 0);
        for (int ind : presentIndexes)
        {
            int group = fieldGroup[ind];
            int size = values[ind].second;

            int offsetAligned = align256(groupSize[group], size, 128);
            valueSizeTotal += offsetAligned + size - groupSize[group];
            intToHex(encode256(offsetAligned, 128), buf + bufOffset + fieldReorderReverse[ind] * 2, 1);
            groupSize[group] = offsetAligned + size;
        }
        bufOffset += indexCapacity * 2;
        buf[bufOffset++] = delim;

        // format value groups
        memset(buf + bufOffset, ' ', valueSizeTotal + fieldGroupCount);

        int currentGroup = 0;
        int groupOffset = 0;
        for (int i : presentIndexes)
        {
            if (fieldGroup[i] != currentGroup)
            {
                bufOffset += groupOffset;
                groupOffset = 0;

                memset(buf + bufOffset, delim, fieldGroup[i] - currentGroup);
                bufOffset += fieldGroup[i] - currentGroup;
                currentGroup = fieldGroup[i];
            }

            int size = values[i].second;
            if (size == 0)
            {
                continue;
            }

            int offset = groupOffset;
            int offsetAligned = align256(offset, size, 128);

            memcpy(buf + bufOffset + offsetAligned, values[i].first, size);
            groupOffset = offsetAligned + size;
        }
        bufOffset += groupOffset;
        memset(buf + bufOffset, delim, fieldGroupCount - currentGroup);
        bufOffset += fieldGroupCount - currentGroup;

        buf[bufOffset++] = terminator[0];

        return std::string(buf, bufOffset);
    }

    std::string toJSONRecord(Message &doc)
    {
        std::string j(doc.get_payload());
        return toRecordHeader(doc) + j + terminator;
    }

    std::string toEAVRecord(Message &doc)
    {
        std::string line;
        std::string h = toRecordHeader(doc);

        jsmn_parser p;
        jsmntok_t t[4098];

        jsmn_init(&p);
        const char *src = (const char *)doc.get_payload().get_data();
        int len = doc.get_payload().get_size();
        int r = jsmn_parse(&p, src, len, t, 4098);
        for (int i = 1; i < r - 1; ++i)
        {
            if (t[i].type == JSMN_STRING && t[i].parent == 0 && t[i + 1].parent == i)
            {
                std::string key(src + t[i].start, t[i].end - t[i].start);
                std::string value(src + t[i + 1].start, t[i + 1].end - t[i + 1].start);
                line += h + key + delimiter + value + terminator;
            }
        }
        return line;
    }

};

struct PartitionTask
{
    PartitionTask() : partition(INT_MAX), offset(-1), limit(0), isNull(true), commit(false) {}

    int partition;
    int64 offset;
    int64 limit;
    bool isNull;
    bool commit;
};


class KafkaSource : public UDSource
{
    Consumer *consumer;
    std::list<Message> pendingDocuments;

    const std::string brokers, topic, group;
    std::vector<PartitionTask> partitions;
    int64 limit;

    Sink *sink;

    bool timedout;
    int duration;
    bool subscribe;

public:
    KafkaSource(std::string brokers, std::string topic, std::string partitions, std::string group, Sink *sink)
        : consumer(NULL), brokers(brokers), topic(topic), group(group), limit(0), sink(sink)
    {
        parsePartitions(partitions);
    }

    virtual std::string getUri() {
        std::vector<std::string> parts = splitString(brokers, ',');
        return parts.size() > 0 ? parts[0] : brokers;
    }

    void setup(ServerInterface &srvInterface)
    {
        this->subscribe = partitions.size() == 1 && partitions[0].partition == REBALANCE_PARTITIONS;
        const bool autocommit = subscribe && REBALANCE_AUTOCOMMIT;

        this->timedout = false;
        this->duration = 0;

        Configuration config = {
            { "metadata.broker.list", brokers },
            { "group.id", group },
            { "enable.auto.commit", autocommit },
            { "enable.auto.offset.store", autocommit },
            { "auto.offset.reset", subscribe ? "earliest" : "error" },
        };
        consumer = new Consumer(config);

        int topicPartitionCount = getPartitionCount(consumer->get_handle(), topic.c_str());
        if (partitions.size() == 1 && (partitions[0].partition == ALL_PARTITIONS ||
                                       partitions[0].partition == REBALANCE_PARTITIONS))
        {
            limit = partitions[0].limit;
            std::vector<PartitionTask> allPartitions;
            for (int p = 0; p < topicPartitionCount; ++p)
            {
                PartitionTask t;
                t.isNull = false;
                t.partition = p;
                t.offset = partitions[0].offset;
                t.limit = 0;
                t.commit = partitions[0].commit && !autocommit;
                allPartitions.push_back(t);
            }
            partitions = allPartitions;
        }

        if (subscribe)
        {
            consumer->set_assignment_callback([&](const TopicPartitionList& partitions) {

                std::vector<PartitionTask> assignedPartitions;
                int partitionCount = 1;

                for (TopicPartition p : partitions)
                {
                    PartitionTask t;
                    t.isNull = false;
                    t.partition = p.get_partition();
                    t.offset = -1000;
                    t.limit = 0;
                    t.commit = !REBALANCE_AUTOCOMMIT;
                    assignedPartitions.push_back(t);

                    partitionCount = max(partitionCount, t.partition + 1);
                    srvInterface.log("partition %d: rebalance assign %s", t.partition, t.commit ? "manual commit" : "autocommit");
                }

                this->partitions.clear();
                this->partitions.resize(partitionCount);
                for (PartitionTask p : assignedPartitions)
                {
                    this->partitions[p.partition] = p;
                }
            });

            consumer->set_revocation_callback([&](const TopicPartitionList& partitions) {
                this->commitOffsets(srvInterface);
            });

            consumer->subscribe({ topic });
        }
        else
        {
            TopicPartitionList offsets;
            for (PartitionTask &p : partitions)
            {
                p.isNull = p.isNull || p.partition < 0 || p.partition >= topicPartitionCount;
                if (!p.isNull)
                {
                    offsets.push_back(TopicPartition(topic, p.partition, p.offset));
                    srvInterface.log("partition %d: assign %lld limit %lld", p.partition, p.offset, p.limit);
                }
            }

            consumer->assign(offsets);
        }
    }

    void destroy(ServerInterface &srvInterface)
    {
        if (subscribe)
        {
            consumer->unsubscribe();
        }
    }

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output)
    {
        if (pendingDocuments.empty())
        {
            output.offset = 0;
        }

        int64 currentLimit = getCurrentLimit();
        int batch_size = max(0, (int) min(currentLimit, 100000LL));

        while ((getCurrentLimit() > 0 && !timedout) || pendingDocuments.size() > 0)
        {
            if (pendingDocuments.empty())
            {
                auto start = std::chrono::steady_clock::now();
                std::vector<Message> msgs = consumer->poll_batch(batch_size, std::chrono::milliseconds(POLL_TIMEOUT));
                auto end = std::chrono::steady_clock::now();

                auto d = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
                duration += d;
                timedout = (d >= POLL_TIMEOUT) || (duration > CUMULATIVE_POLL_TIMEOUT);

                srvInterface.log("%lu messages polled: %ld ms poll, %d ms total", msgs.size(), d, duration);
                for (Message &msg: msgs)
                {
                    if (!msg)
                    {
                        continue;
                    }

                    if (msg.get_error() && !msg.is_eof())
                    {
                        srvInterface.log("error recieived: %s", msg.get_error().to_string().c_str());
                        continue;
                    }

                    int part = msg.get_partition();
                    partitions[part].limit -= 1;
                    partitions[part].offset = msg.get_offset() + 1;
                    pendingDocuments.emplace_back(std::move(msg));
                }
            }

            if (pendingDocuments.empty())
            {
                srvInterface.log("no documents left");
                commitOffsets(srvInterface);
                return DONE;
            }

            int delta = sink->put(pendingDocuments.front(), output.buf + output.offset, output.size - output.offset);
            if (delta == -1)
            {
                srvInterface.log("buffer overflow");
                return OUTPUT_NEEDED;
            }
            output.offset += delta;
            pendingDocuments.pop_front();
            limit -= 1;
        }

        srvInterface.log(timedout ? "timeout" : "limit exceeded");
        commitOffsets(srvInterface);
        return DONE;
    }

private:

    int getPartitionCount(rd_kafka_t* rdkafka, const char *topic_name)
    {
        int partitionCount = 0;

        rd_kafka_topic_t *rdtopic = rd_kafka_topic_new(rdkafka, topic_name, 0);
        const rd_kafka_metadata_t *rdmetadata;

        rd_kafka_resp_err_t err = rd_kafka_metadata(rdkafka, 0, rdtopic, &rdmetadata, 30000);
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            for (int i = 0; i < rdmetadata->topic_cnt; ++i)
            {
                partitionCount = rdmetadata->topics[i].partition_cnt;
            }
            rd_kafka_metadata_destroy(rdmetadata);
        }

        rd_kafka_topic_destroy(rdtopic);

        return partitionCount;
    }

    void parsePartitions(const std::string &partFmt)
    {
        std::map<std::string, int> partConsts;
        partConsts["*"] = ALL_PARTITIONS;
        partConsts["%"] = REBALANCE_PARTITIONS;

        int partitionCount = 1;

        std::vector<std::string> fmtParts = splitString(partFmt, ',');

        std::vector<PartitionTask> partitionList;
        for (std::string part : fmtParts)
        {
            std::vector<std::string> tuple = splitString(part, ':');

            if (tuple.size() != 3)
            {
                vt_report_error(0, "partition format missmatch: [partition:offset:limit](,[partition:offset:limit])*");
            }

            PartitionTask t;
            t.isNull = false;
            t.partition = partConsts.count(tuple[0]) ? partConsts[tuple[0]] : toInt(tuple[0], -1);
            t.offset = toInt(tuple[1]);
            t.limit = toInt(tuple[2]);
            t.commit = t.offset < 0;

            if (t.partition < 0 && tuple[0] != std::string("*") && tuple[0] != std::string("%"))
            {
                vt_report_error(0, "partition number must be integer, '*' or '%'");
            }
            else if (t.partition < 0 && fmtParts.size() > 1)
            {
                vt_report_error(0, "only one partition clause is expected for '*' or '%'");
            }
            else if (t.offset == NOTINT || (t.offset < 0 && t.offset != -1 && t.offset != -2 && t.offset != -1000))
            {
                vt_report_error(0, "partition offset must be positive integer or -1 for latest or -2 for earlest or -1000 for last read");
            }
            else if (t.partition == REBALANCE_PARTITIONS && t.offset != -1000)
            {
                vt_report_error(0, "subscribe is only available with offset -1000 (last read)");
            }
            else if (t.limit == NOTINT || t.limit < 0)
            {
                vt_report_error(0, "partition limit must be positive integer");
            }
            else
            {
                partitionList.push_back(t);
                partitionCount = max(partitionCount, t.partition + 1);
            }
        }

        if (partitionCount == 1 && (partitionList[0].partition == ALL_PARTITIONS ||
                                    partitionList[0].partition == REBALANCE_PARTITIONS))
        {
            partitions = partitionList;
        }
        else
        {
            partitions.resize(partitionCount);
            for (PartitionTask p : partitionList)
            {
                partitions[p.partition] = p;
            }
        }
    }

    int64 getCurrentLimit() const
    {
        int64 partitionLimit = 0;
        for (PartitionTask p : partitions)
        {
            if (!p.isNull && p.limit > 0)
            {
                partitionLimit += p.limit;
            }
        }
        return max(0LL, max(limit, partitionLimit));
    }

    void commitOffsets(ServerInterface &srvInterface)
    {
        TopicPartitionList offsets;
        for (PartitionTask &p : partitions)
        {
            if (!p.isNull && p.commit && p.offset > 0)
            {
                offsets.push_back(TopicPartition(topic, p.partition, p.offset));
                srvInterface.log("partition %d: commit offset %lld", p.partition, p.offset);
                p.offset = -1;
            }
        }
        if (!offsets.empty())
        {
            consumer->store_offsets(offsets);
            consumer->commit(offsets);
        }
    }


};


class KafkaSourceFactory : public SourceFactory
{
public:

    virtual void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt)
    {
        std::vector<std::string> args = srvInterface.getParamReader().getParamNames();

        /* Check parameters */
        if (args.size() != 7 ||
            find(args.begin(), args.end(), "brokers") == args.end() ||
            find(args.begin(), args.end(), "topic") == args.end() ||
            find(args.begin(), args.end(), "partitions") == args.end() ||
            find(args.begin(), args.end(), "group_id") == args.end() ||
            find(args.begin(), args.end(), "format") == args.end() ||
            find(args.begin(), args.end(), "delimiter") == args.end() ||
            find(args.begin(), args.end(), "terminator") == args.end())
        {
            vt_report_error(0, "Must have exactly 7 arguments");
        }

        /* Populate planData */
        planCtxt.getWriter().getStringRef("brokers").copy(srvInterface.getParamReader().getStringRef("brokers"));
        planCtxt.getWriter().getStringRef("topic").copy(srvInterface.getParamReader().getStringRef("topic"));
        planCtxt.getWriter().getStringRef("partitions").copy(srvInterface.getParamReader().getStringRef("partitions"));
        planCtxt.getWriter().getStringRef("group_id").copy(srvInterface.getParamReader().getStringRef("group_id"));
        planCtxt.getWriter().getStringRef("format").copy(srvInterface.getParamReader().getStringRef("format"));
        planCtxt.getWriter().getStringRef("delimiter").copy(srvInterface.getParamReader().getStringRef("delimiter"));
        planCtxt.getWriter().getStringRef("terminator").copy(srvInterface.getParamReader().getStringRef("terminator"));

        /* Munge nodes list */
        std::vector<std::string> executionNodes;
        executionNodes.push_back(srvInterface.getCurrentNodeName());
        planCtxt.setTargetNodes(executionNodes);
    }

    virtual bool isSourceApportionable()
    {
        return false;
    }

    virtual ssize_t getDesiredThreads(ServerInterface &srvInterface, ExecutorPlanContext &planCtxt)
    {
        return 1;
    }

    virtual std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt)
    {
        ParamReader udParams = srvInterface.getUDSessionParamReader();
        ParamReader fnParams = planCtxt.getReader();

        std::vector<UDSource*> retVal;
        retVal.push_back(vt_createFuncObject<KafkaSource>(
                srvInterface.allocator,
                fnParams.getStringRef("brokers").str(),
                fnParams.getStringRef("topic").str(),
                fnParams.getStringRef("partitions").str(),
                fnParams.getStringRef("group_id").str(),
                new CSVSink(
                    fnParams.getStringRef("format").str(),
                    fnParams.getStringRef("delimiter").str(),
                    fnParams.getStringRef("terminator").str()
                )
            ));
        return retVal;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(1024, "brokers");
        parameterTypes.addVarchar(1024, "topic");
        parameterTypes.addVarchar(8000, "partitions");
        parameterTypes.addVarchar(1024, "group_id");
        parameterTypes.addVarchar(32000, "format");
        parameterTypes.addChar(1, "delimiter");
        parameterTypes.addChar(1, "terminator");
    }
};

RegisterFactory(KafkaSourceFactory);
