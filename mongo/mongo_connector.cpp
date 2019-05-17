
#include "Vertica.h"

#include <iostream>
#include <unordered_map>
#include "tsl/hopscotch_map.h"
#include <vector>
#include <chrono>
#include <ctime>
#include <regex>
#include <iomanip>

#include <bson/bson.h>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/stdx/string_view.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/error_code.hpp>

//#include "backtrace.h"
//INSTALL_SIGSEGV_TRAP

#define MAX_INDEX_SIZE 2024
#define MAX_FIELD_COUNT 256
#define MAX_GROUP_COUNT 64
#define MAX_VALUES_SIZE 16000


using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

using namespace Vertica;


inline std::string toAlignedString(int x)
{
    char buf[5];
    snprintf(buf, 5, "%04x", x);
    std::string str(buf);
    return str;
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

inline std::string toString(const bsoncxx::document::element &el, const std::string &delim, const std::string &term)
{
    if (!el || el.type() == bsoncxx::types::b_null::type_id)
    {
        return std::string();
    }
    else if (el.type() == bsoncxx::types::b_oid::type_id)
    {
        return el.get_oid().value.to_string();
    }
    else if (el.type() == bsoncxx::types::b_int32::type_id)
    {
        std::ostringstream ss;
        ss << el.get_int32().value;
        return ss.str();
    }
    else if (el.type() == bsoncxx::types::b_int64::type_id)
    {
        std::ostringstream ss;
        ss << el.get_int64().value;
        return ss.str();
    }
    else if (el.type() == bsoncxx::types::b_utf8::type_id)
    {
        std::string s = el.get_utf8().value.to_string();
        for (size_t i = 0; i < s.size(); ++i)
        {
            if ((!delim.empty() && s[i] == delim[0]) || (!term.empty() && s[i] == term[0]))
            {
                s[i] = ' ';
            }
        }
        return s;
    }
    else if (el.type() == bsoncxx::types::b_bool::type_id)
    {
        return el.get_bool().value ? "t" : "f";
    }
    else if (el.type() == bsoncxx::types::b_double::type_id)
    {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(7) << el.get_double().value;
        return ss.str();
    }
    else if (el.type() == bsoncxx::types::b_date::type_id)
    {
        std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(el.get_date().value);
        std::chrono::seconds s = std::chrono::duration_cast<std::chrono::seconds>(ms);

        std::time_t t = s.count();
        //int milli = ms.count() % 1000;

        const int size = sizeof "2001-01-01T00:00:00" - 1;
        char buf[size];
        memset(buf, ' ', size);
        strftime(buf, size, "%FT%T", gmtime(&t));
        //sprintf(buf, "%s.%d", buf, milli);

        return std::string(buf, size);
    }
    else if (el.type() == bsoncxx::types::b_document::type_id)
    {
        return bsoncxx::to_json(el.get_document().value);
    }
    else if (el.type() == bsoncxx::types::b_array::type_id)
    {
        bson_t bson;
        bson_init_static(&bson, el.get_array().value.data(), el.get_array().value.length());

        size_t size;
        char *str = bson_array_as_json (&bson, &size);
        std::string res(str, size);
        bson_free (str);

        return res;
    }
    return std::string();
}

inline int64_t toInt(const bsoncxx::document::element &el)
{
    if (el.type() == bsoncxx::types::b_int32::type_id)
    {
        return el.get_int32().value;
    }
    else if (el.type() == bsoncxx::types::b_int64::type_id)
    {
        return el.get_int64().value;
    }
    else if (el.type() == bsoncxx::types::b_utf8::type_id)
    {
        std::string t = el.get_utf8().value.to_string();
        std::size_t const b = t.find_first_of("0123456789");
        if (b != std::string::npos)
        {
            std::size_t const e = t.find_first_not_of("0123456789", b);
            return stoi(t.substr(b, e != std::string::npos ? e - b : e));
        }
    }
    return 0;
}

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

std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string> > > parseRules(std::string s)
{
    std::unordered_map<std::string, std::unordered_map<std::string, std::vector<std::string> > > res;
    std::vector<std::string> rules = splitString(s, ';');
    for (unsigned i = 0; i < rules.size(); ++i)
    {
        std::regex rgx("^(.*)\\[(.*)\\]=(.*)$");
        std::smatch match;

        if (std::regex_search(rules[i], match, rgx))
        {
            res[match[1]][match[2]] = splitString(match[3], ',');
        }
    }
    return res;
}


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


static mongocxx::instance inst{};


enum OutputFormat
{
    JSON,
    EAV,
    ARRAY
};


class MongodbSource : public UDSource
{
    mongocxx::client conn;
    mongocxx::collection col;
    mongocxx::cursor *cursor;
    std::list<std::string> pendingDocuments;
    int cursorPosition;

    const std::string url, database, collection, query;
    const std::string format;
    const std::string delimiter, terminator;

    int formatType;
    int arrayGroupCount;
    std::vector<std::string> arrayKeys;
    std::vector<int> keyGroup;
    tsl::hopscotch_map<std::string, int, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<std::string, int> >, 30, true, tsl::power_of_two_growth_policy> keyIndex;

private:
    std::string toArrayRecordComp(const bsoncxx::document::view &doc)
    {
        std::string id = doc["_id"].get_oid().value.to_string();

        std::vector<std::string> fieldValues(arrayKeys.size(), std::string());
        int fieldCount = 0;
        for (auto i = doc.begin(); i != doc.end() && fieldCount < MAX_FIELD_COUNT; ++i)
        {
            const bsoncxx::document::element &el = *i;
            auto found = keyIndex.find(std::string(el.key()));
            if (found != keyIndex.end())
            {
                int ind = found->second & 0xffff;
                fieldValues[ind] = toString(el, delimiter, terminator);
                fieldCount += 1;
            }
        }

        std::vector<std::string> valueGroups(arrayGroupCount, std::string());
        std::vector<bool> groupIsOpen(arrayGroupCount, false);

        const int indexCapacity = std::min(int(fieldValues.size()), MAX_INDEX_SIZE);
        static char buf[MAX_INDEX_SIZE * 2];
        memset(buf, '#', indexCapacity * 2);

        int indexShift = 0;
        for (int i = 0; i < indexCapacity; ++i)
        {
            int group = keyGroup[i];

            if (fieldValues[i].size() == 0)
            {
                if (groupIsOpen[group])
                {
                    groupIsOpen[group] = false;
                    int offset = valueGroups[group].size();
                    intToHex(encode256(align256(offset, 0, 128), 128), buf + indexShift, 1);
                    indexShift += 2;
                    continue;
                }
                else
                {
                    indexShift += 1;
                    continue;
                }
            }

            int offset = valueGroups[group].size();
            int size = fieldValues[i].size();

            int offsetAligned = align256(offset, size, 128);
            valueGroups[group] += std::string(offsetAligned - offset, ' ');
            valueGroups[group] += fieldValues[i];

            intToHex(encode256(offsetAligned, 128), buf + indexShift, 1);
            indexShift += 2;
            groupIsOpen[group] = true;
        }

        std::string res = id + delimiter + std::string(buf, buf + indexShift);
        for (size_t i = 0; i < valueGroups.size(); ++i)
        {
            res += delimiter + valueGroups[i];
        }
        return res + terminator;
    }

    std::string toJSONRecord(const bsoncxx::document::view &doc)
    {
        std::string j = bsoncxx::to_json(doc);
        return doc["_id"].get_oid().value.to_string() + delimiter + j + terminator;
    }

    std::string toEAVRecord(const bsoncxx::document::view &doc)
    {
        std::string line;
        std::string id = doc["_id"].get_oid().value.to_string();
        for (auto i = doc.begin(); i != doc.end(); ++i)
        {
            const bsoncxx::document::element &el = *i;
            std::string k(el.key().data());
            line += id + delimiter + k + delimiter + toString(el, delimiter, terminator) + terminator;
        }
        return line;
    }

    std::string toRecord(const bsoncxx::document::view &doc, char *buffer, size_t &offset, size_t size)
    {
        if (formatType == JSON)
        {
            return toJSONRecord(doc);
        }
        else if (formatType == EAV)
        {
            return toEAVRecord(doc);
        }
        else if (formatType == ARRAY)
        {
            return toArrayRecordComp(doc);
        }
        return std::string();
    }

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output)
    {
        if (pendingDocuments.empty())
        {
            output.offset = 0;
        }

        srvInterface.log("process offset=%lu size=%lu", output.offset, output.size);

        while (true)
        {
            try
            {
                if (cursor == NULL)
                {
                    mongocxx::read_preference pref;
                    pref.mode(mongocxx::read_preference::read_mode::k_secondary_preferred);
                    pref.max_staleness(std::chrono::seconds(180));

                    mongocxx::options::find options{};
                    options.read_preference(pref);
                    options.batch_size(10000);
                    options.no_cursor_timeout(true);
                    options.sort(make_document(kvp("_id", 1)));
                    options.skip(cursorPosition);

                    if (arrayKeys.size() > 0)
                    {
                        bsoncxx::builder::basic::document proj;
                        for (size_t i = 0; i < arrayKeys.size(); ++i)
                        {
                            proj.append(kvp(arrayKeys[i], 1));
                        }
                        options.projection(proj.extract());
                    }

                    srvInterface.log("query: %s", bsoncxx::to_json(bsoncxx::from_json(query)).c_str());
                    mongocxx::cursor results = col.find(bsoncxx::from_json(query), options);
                    cursor = new mongocxx::cursor(std::move(results));
                    srvInterface.log("query: cursor created");
                }

                mongocxx::cursor::iterator iter = cursor->begin();
                srvInterface.log("query: cursor started");

                while (true)
                {
                    std::string line;
                    if (!pendingDocuments.empty())
                    {
                        line = pendingDocuments.front();
                        pendingDocuments.pop_front();
                    }
                    else if (iter != cursor->end())
                    {
                        line = toRecord(*iter, output.buf, output.offset, output.size);
                        iter++;
                        cursorPosition++;
                    }
                    else
                    {
                        return DONE;
                    }

                    if (output.offset + line.size() > output.size)
                    {
                        pendingDocuments.push_back(line);
                        return OUTPUT_NEEDED;
                    }
                    else
                    {
                        memcpy(output.buf + output.offset, line.data(), line.size());
                        output.offset += line.size();
                    }
                }
            }
            catch (const mongocxx::exception& e)
            {
                if (e.code().value() == 43)
                {
                    srvInterface.log("mongodb error: %s", e.what());
                    delete cursor;
                    cursor = NULL;
                }
                else
                {
                    throw e;
                }
            }
        }

        return DONE;
    }

public:
    MongodbSource(std::string url, std::string database, std::string collection, std::string query,
                  std::string format, std::string delimiter, std::string terminator)
        : cursor(NULL), cursorPosition(0),
          url(url), database(database), collection(collection), query(query),
          format(format), delimiter(delimiter), terminator(terminator)
    {
        if (format == std::string("json"))
        {
            formatType = JSON;
        }
        else if (format == std::string("eav"))
        {
            formatType = EAV;
        }
        else if (format.substr(0, 5) == std::string("array"))
        {
            formatType = ARRAY;

            std::unordered_map<std::string, int> groups;

            std::vector<std::string> keys = splitString(format.substr(6), ',');
            for (size_t j = 0; j < keys.size(); ++j)
            {
                std::vector<std::string> keyParts = splitString(keys[j], '=');
                std::string key = keyParts[0];
                arrayKeys.push_back(key);

                std::string group = keyParts.size() > 0 ? keyParts[1] : "";
                auto it = groups.find(group);
                if (it == groups.end())
                {
                    it = groups.insert(std::make_pair(group, groups.size())).first;
                }
                keyIndex[key] = j;
                keyGroup.push_back(it->second);
            }

            arrayGroupCount = groups.size();
        }
        else
        {
            formatType = JSON;
        }
    }

    void setup(ServerInterface &srvInterface)
    {
        delete cursor;

        conn = mongocxx::client(mongocxx::uri(url));
        col = conn[database][collection];

        cursor = NULL;
        cursorPosition = 0;
    }

    void destroy(ServerInterface &srvInterface)
    {
    }

    virtual std::string getUri() {return url;}
};


class MongodbSourceFactory : public SourceFactory
{
public:

    virtual void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt)
    {
        std::vector<std::string> args = srvInterface.getParamReader().getParamNames();

        /* Check parameters */
        if (args.size() != 7 ||
            find(args.begin(), args.end(), "url") == args.end() ||
            find(args.begin(), args.end(), "database") == args.end() ||
            find(args.begin(), args.end(), "collection") == args.end() ||
            find(args.begin(), args.end(), "query") == args.end() ||
            find(args.begin(), args.end(), "format") == args.end() ||
            find(args.begin(), args.end(), "delimiter") == args.end() ||
            find(args.begin(), args.end(), "terminator") == args.end())
        {
            vt_report_error(0, "Must have exactly 7 arguments");
        }

        /* Populate planData */
        planCtxt.getWriter().getStringRef("url").copy(srvInterface.getParamReader().getStringRef("url"));
        planCtxt.getWriter().getStringRef("database").copy(srvInterface.getParamReader().getStringRef("database"));
        planCtxt.getWriter().getStringRef("collection").copy(srvInterface.getParamReader().getStringRef("collection"));
        planCtxt.getWriter().getStringRef("query").copy(srvInterface.getParamReader().getStringRef("query"));
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
        retVal.push_back(vt_createFuncObject<MongodbSource>(
                srvInterface.allocator,
                fnParams.getStringRef("url").str(),
                fnParams.getStringRef("database").str(),
                fnParams.getStringRef("collection").str(),
                fnParams.getStringRef("query").str(),
                fnParams.getStringRef("format").str(),
                fnParams.getStringRef("delimiter").str(),
                fnParams.getStringRef("terminator").str()
            ));
        return retVal;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(1024, "url");
        parameterTypes.addVarchar(1024, "database");
        parameterTypes.addVarchar(1024, "collection");
        parameterTypes.addVarchar(1024, "query");
        parameterTypes.addVarchar(32000, "format");
        parameterTypes.addChar(1, "delimiter");
        parameterTypes.addChar(1, "terminator");
    }
};

RegisterFactory(MongodbSourceFactory);
