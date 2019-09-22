/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/*
 * Description: Example User Defined Analytic Function Lag.
 *
 * Create Date: Nov 22, 2011
 */

#include <string>
#include <unordered_map>
#include <sstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iostream>
#include "rapidjson/reader.h"
#include "rapidjson/stringbuffer.h"
#include "Vertica.h"

#define MAX_LEVEL 2
//#define PRINT_DEBUG

using namespace Vertica;
using namespace std;
using namespace rapidjson;


struct JsonValue
{
    JsonValue()
    {
        type = 0;
    }

    int type;
    vbool b;
    vint i;
    double d;
    string s;
};

struct JsonParam
{
    string name;
    vector<string> keys;
    int type;
    int len;
};

vector<string> splitString(string s, char delim)
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

vector<JsonParam> parseParams(string s)
{
    vector<JsonParam> res;
    vector<string> params = splitString(s, ';');
    for (unsigned i = 0; i < params.size(); ++i)
    {
        JsonParam p;
        p.type = VarcharOID;
        p.len = 32;

        vector<string> keys_type_name = splitString(params[i], ' ');
        if (keys_type_name.size() > 0)
        {
            p.keys = splitString(keys_type_name[0], ',');
            p.name = p.keys[0];
        }
        if (keys_type_name.size() > 1)
        {
            string t = keys_type_name[1];
            string n = splitString(t, '(')[0];
            if (n == "bool" || n == "boolean")
            {
                p.type = BoolOID;
            }
            else if (n == "int" || n == "integer")
            {
                p.type = Int8OID;
            }
            else if (n == "numeric" || n == "number" || n == "decimal")
            {
                p.type = Float8OID;
            }
            else if (n == "varchar" || n == "char")
            {
                std::size_t const b = t.find_first_of("0123456789");
                if (b != std::string::npos)
                {
                    std::size_t const e = t.find_first_not_of("0123456789", b);
                    p.len = stoi(t.substr(b, e != std::string::npos ? e - b : e));
                }
                p.type = VarcharOID;
            }
        }
        if (keys_type_name.size() > 2)
        {
            p.name = keys_type_name[2];
        }

        if (p.name != "")
        {
            res.push_back(p);
        }
    }
    return res;
}

enum {ARRAY = 1, OBJECT = 2};

struct SAXJsonParser
{
    unordered_map<string, JsonValue> data;
    string current_key;
    vector<int> levelType;
    vector<int> levelSize;

    void init()
    {
        data.clear();
        current_key = string();
        levelType.clear();
        levelType.push_back(OBJECT);
        levelSize.clear();
        levelSize.push_back(0);
    }

    inline string getValue(const string &key) const
    {
        const unordered_map<string,JsonValue>::const_iterator &it = data.find(key);
        if (it != data.end())
        {
            const JsonValue &v = it->second;
            if (v.type == -1)
            {
                return string();
            }
            if (v.type == BoolOID)
            {
                return v.b ? "true" : "false";
            }
            else if (v.type == Int8OID)
            {
                std::ostringstream ss;
                ss << v.i;
                return ss.str();
            }
            else if (v.type == Float8OID)
            {
                std::ostringstream ss;
                ss << v.d;
                return ss.str();
            }
            else if (v.type == VarcharOID)
            {
                return v.s;
            }
        }
        return string();
    }

    bool Null()
    {
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? ",null" : "null");
        }
        else
        {
            data[current_key].type = -1;
        }
        levelSize.back() += 1;
        return true;
    }

    bool Bool(bool b)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + string(b ? "true" : "false");
        }
        else
        {
            data[current_key].type = BoolOID;
            data[current_key].b = b;
        }
        levelSize.back() += 1;
        return true;
    }

    bool Int(int i)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            std::ostringstream ss;
            ss << i;
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        else
        {
            data[current_key].type = Int8OID;
            data[current_key].i = i;
        }
        levelSize.back() += 1;
        return true;
    }

    bool Uint(unsigned u)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            std::ostringstream ss;
            ss << u;
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        else
        {
            data[current_key].type = Int8OID;
            data[current_key].i = u;
        }
        levelSize.back() += 1;
        return true;
    }

    bool Int64(int64_t i)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            std::ostringstream ss;
            ss << i;
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        else
        {
            data[current_key].type = Int8OID;
            data[current_key].i = i;
        }
        levelSize.back() += 1;
        return true;
    }

    bool Uint64(uint64_t u)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            std::ostringstream ss;
            ss << u;
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        else
        {
            data[current_key].type = Int8OID;
            data[current_key].i = u;
        }
        levelSize.back() += 1;
        return true;
    }

    bool Double(double d)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            std::ostringstream ss;
            ss << d;
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        else
        {
            data[current_key].type = Float8OID;
            data[current_key].d = d;
        }
        levelSize[levelSize.size() - 1] += 1;
        return true;
    }

    bool RawNumber(const char* str, SizeType length, bool copy)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + string(str, length);
        }
        else
        {
            data[current_key].type = VarcharOID;
            data[current_key].s = string(str, length);
        }
        levelSize[levelSize.size() - 1] += 1;
        return true;
    }

    bool String(const char* str, SizeType length, bool copy)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? ",\"" : "\"") + string(str, length) + "\"";
        }
        else
        {
            data[current_key].type = VarcharOID;
            data[current_key].s = string(str, length);
        }
        levelSize[levelSize.size() - 1] += 1;
        return true;
    }

    bool StartObject()
    {
        int parentType = levelType.back();
        levelType.push_back(OBJECT);
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].type = VarcharOID;
            data[current_key].s += (levelSize.back() > 0 && parentType == ARRAY ? ",{" : "{");
        }
        levelSize.back() += 1;
        levelSize.push_back(0);
        return true;
    }

    bool Key(const char* str, SizeType length, bool copy)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].s += (levelSize.back() == 0 && levelType.back() == OBJECT ? "\"" : ",\"") + string(str, length) + "\":";
        }
        else
        {
            current_key = string(str, length);
        }
        return true;
    }

    bool EndObject(SizeType memberCount)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].s += "}";
        }
        levelType.pop_back();
        levelSize.pop_back();
        return true;
    }

    bool StartArray()
    {
        int parentType = levelType.back();
        levelType.push_back(ARRAY);
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].type = VarcharOID;
            data[current_key].s += (levelSize.back() > 0 && parentType == ARRAY ? ",[" : "[");
        }
        levelSize.back() += 1;
        levelSize.push_back(0);
        return true;
    }

    bool EndArray(SizeType elementCount)
    {
        if (levelType.size() > MAX_LEVEL)
        {
            data[current_key].s += "]";
        }
        levelType.pop_back();
        levelSize.pop_back();
        return true;
    }
};


class RapidJsonExtractor : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter)
    {
        try {

            SizedColumnTypes &inputTypes = inputReader.getTypeMetaData();
            int json_col = inputTypes.getColumnCount() - 1;
            if (!inputTypes.getColumnType(json_col).isVarchar())
            {
                vt_report_error(0, "Last column has to be varchar column");
            }

            vector<JsonParam> params = parseParams(srvInterface.getParamReader().getStringRef("keys").str());

            Reader reader;
            SAXJsonParser parser;

            do
            {
                // copy regular columns
                for (unsigned i = 0; i < (inputTypes.getColumnCount() - 1); ++i)
                {
                    if (inputReader.isNull(i))
                    {
                        outputWriter.setNull(i);
                    }
                    else
                    {
                        outputWriter.copyFromInput(i, inputReader, i);
                    }
                }

                // parse json
                parser.init();
                if (!inputReader.isNull(json_col))
                {
                    const VString &json = inputReader.getStringRef(json_col);
                    StringStream ss(json.data());
                    reader.Parse<kParseNumbersAsStringsFlag>(ss, parser);
#ifdef PRINT_DEBUG
                    string pairs;
                    for (unordered_map<string,JsonValue>::const_iterator it = parser.data.begin(); it != parser.data.end(); ++it)
                    {
                        std::ostringstream ss;
                        ss << it->second.type;
                        pairs.append(it->first + "=" + it->second.s + " " + ss.str() + " ");
                    }
                    srvInterface.log("%lld: %s", inputReader.getIntRef(0), pairs.c_str());
#endif
                }

                // fill json fields as new columns
                for (unsigned i = 0; i < params.size(); ++i)
                {
                    bool isNull = true;
                    for (unsigned j = 0; j < params[i].keys.size(); ++j)
                    {
                        string v = parser.getValue(params[i].keys[j]);
                        if (!v.empty())
                        {
                            if (params[i].type == VarcharOID)
                            {
                                outputWriter.getStringRef(json_col + i).copy(v.substr(0, params[i].len).data());
                            }
                            else if (params[i].type == Int8OID)
                            {
                                outputWriter.setInt(json_col + i, strtol(v.data(), 0, 10));
                            }
                            else if (params[i].type == Float8OID)
                            {
                                outputWriter.setFloat(json_col + i, strtod(v.data(), 0));
                            }
                            else if (params[i].type == BoolOID)
                            {
                                outputWriter.setBool(json_col + i, v == "true");
                            }
                            isNull = false;
                            break;
                        }
                    }
                    if (isNull)
                    {
                        outputWriter.setNull(json_col + i);
                    }
                }

                outputWriter.next();
            }
            while (inputReader.next());

        }
        catch(std::exception& e)
        {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};


class RapidJsonFactory : public TransformFunctionFactory
{

    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(32000, "keys", SizedColumnTypes::Properties(false, true, false, "keys delimited with semicolon"));
    }

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addAny();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
        for (unsigned i = 0; i < (inputTypes.getColumnCount() - 1); ++i)
        {
            if (inputTypes.getColumnType(i).isVarchar())
            {
                outputTypes.addVarchar(inputTypes.getColumnType(i).getStringLength(), inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isChar())
            {
                outputTypes.addChar(inputTypes.getColumnType(i).getStringLength(), inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isInt() && inputTypes.isPartitionByColumn(i))
            {
                outputTypes.addIntPartitionColumn(inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isInt() && inputTypes.isOrderByColumn(i))
            {
                outputTypes.addIntOrderColumn(inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isInt())
            {
                outputTypes.addInt(inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isNumeric())
            {
                outputTypes.addNumeric(inputTypes.getColumnType(i).getNumericPrecision(),
                                       inputTypes.getColumnType(i).getNumericScale(),
                                       inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isFloat())
            {
                outputTypes.addFloat(inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isTimestamp())
            {
                outputTypes.addTimestamp(inputTypes.getColumnType(i).getTimestampPrecision(), inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isDate())
            {
                outputTypes.addDate(inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isTime())
            {
                outputTypes.addTime(inputTypes.getColumnType(i).getTimePrecision(), inputTypes.getColumnName(i));
            }
            else if (inputTypes.getColumnType(i).isBool())
            {
                outputTypes.addBool(inputTypes.getColumnName(i));
            }
            else
            {
                vt_report_error(0, "Column type not supported: %lld", inputTypes.getColumnType(i).getTypeOid());
            }
        }

        if (srvInterface.getParamReader().containsParameter("keys"))
        {
            vector<JsonParam> params = parseParams(srvInterface.getParamReader().getStringRef("keys").str());

#ifdef PRINT_DEBUG
            for (unsigned i = 0; i < params.size(); ++i)
            {
                string keys;
                for (unsigned j = 0; j < params[i].keys.size(); ++j)
                {
                    keys.append(params[i].keys[j]);
                    keys.append(" ");
                }
                srvInterface.log("%s(%d %d): %s", params[i].name.c_str(), params[i].type, params[i].len, keys.c_str());
            }
#endif

            for (unsigned i = 0; i < params.size(); ++i)
            {               
                if (params[i].type == BoolOID)
                {
                    outputTypes.addBool(params[i].name);
                }
                else if (params[i].type == Int8OID)
                {
                    outputTypes.addInt(params[i].name);
                }
                else if (params[i].type == Float8OID)
                {
                    outputTypes.addFloat(params[i].name);
                }
                else if (params[i].type == VarcharOID)
                {
                    outputTypes.addVarchar(params[i].len, params[i].name);
                }
            }
        }
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, RapidJsonExtractor);
    }

};

RegisterFactory(RapidJsonFactory);
