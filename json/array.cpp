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

#define ELEMENT_LEVEL 2

using namespace Vertica;
using namespace std;
using namespace rapidjson;


struct JsonValue
{
    JsonValue(int t) : type(t) {}

    int type;
    vbool b;
    vint i;
    double d;
    string s;
};

struct JsonParam
{
    string name;
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

enum {ARRAY = 1, OBJECT = 2};

struct SAXArrayParser
{
    vector<JsonValue> data;
    vector<int> levelType;
    vector<int> levelSize;

    void init()
    {
        data.clear();
        levelType.clear();
        levelType.push_back(OBJECT);
        levelSize.clear();
        levelSize.push_back(0);
    }

    inline string getValue(size_t r) const
    {
        if (r >= data.size())
        {
            return string();
        }

        const JsonValue &v = data[r];
        if (v.type == -1)
        {
            return string();
        }
        else if (v.type == BoolOID)
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
        return string();
    }

    bool Null()
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(-1);
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? ",null" : "null");
        }
        levelSize.back() += 1;
        return true;
    }

    bool Bool(bool b)
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(BoolOID);
            v.b = b;
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + string(b ? "true" : "false");
        }
        levelSize.back() += 1;
        return true;
    }

    bool Int(int i)
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(Int8OID);
            v.i = i;
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            std::ostringstream ss;
            ss << i;
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        levelSize.back() += 1;
        return true;
    }

    bool Uint(unsigned u)
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(Int8OID);
            v.i = u;
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            std::ostringstream ss;
            ss << u;
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        levelSize.back() += 1;
        return true;
    }

    bool Int64(int64_t i)
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(Int8OID);
            v.i = i;
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            std::ostringstream ss;
            ss << i;
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        levelSize.back() += 1;
        return true;
    }

    bool Uint64(uint64_t u)
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(Int8OID);
            v.i = u;
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            std::ostringstream ss;
            ss << u;
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        levelSize.back() += 1;
        return true;
    }

    bool Double(double d)
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(Float8OID);
            v.d = d;
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            std::ostringstream ss;
            ss << d;
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + ss.str();
        }
        levelSize.back() += 1;
        return true;
    }

    bool RawNumber(const char* str, SizeType length, bool copy)
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(VarcharOID);
            v.s = string(str, length);
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? "," : "") + string(str, length);
        }
        levelSize.back() += 1;
        return true;
    }

    bool String(const char* str, SizeType length, bool copy)
    {
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(VarcharOID);
            v.s = string(str, length);
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().s += string(levelSize.back() > 0 && levelType.back() == ARRAY ? ",\"" : "\"") + string(str, length) + "\"";
        }
        levelSize.back() += 1;
        return true;
    }

    bool StartObject()
    {
        int parentType = levelType.back();
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(VarcharOID);
            v.s += string("{");
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().type = VarcharOID;
            data.back().s += string(levelSize.back() > 0 && parentType == ARRAY ? ",{" : "{");
        }
        levelType.push_back(OBJECT);
        levelSize.back() += 1;
        levelSize.push_back(0);
        return true;
    }

    bool Key(const char* str, SizeType length, bool copy)
    {
        if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().s += (levelSize.back() == 0 && levelType.back() == OBJECT ? "\"" : ",\"") + string(str, length) + "\":";
        }
        return true;
    }

    bool EndObject(SizeType memberCount)
    {
        if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().s += string("}");
        }
        levelType.pop_back();
        levelSize.pop_back();
        return true;
    }

    bool StartArray()
    {
        int parentType = levelType.back();
        if (levelType.size() == ELEMENT_LEVEL)
        {
            JsonValue v(VarcharOID);
            v.s += string("[");
            data.push_back(v);
        }
        else if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().type = VarcharOID;
            data.back().s += string(levelSize.back() > 0 && parentType == ARRAY ? ",[" : "[");
        }
        levelType.push_back(ARRAY);
        levelSize.back() += 1;
        levelSize.push_back(0);
        return true;
    }

    bool EndArray(SizeType elementCount)
    {
        if (levelType.size() > ELEMENT_LEVEL)
        {
            data.back().s += string("]");
        }
        levelType.pop_back();
        levelSize.pop_back();
        return true;
    }
};


class RapidArrayExtractor : public TransformFunction
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

            int element_len = RapidArrayExtractor::parseElementLength(srvInterface.getParamReader());

            Reader reader;
            SAXArrayParser parser;

            do
            {
                // parse array
                parser.init();
                if (!inputReader.isNull(json_col))
                {
                    const VString &json = inputReader.getStringRef(json_col);
                    StringStream ss(json.data());
                    reader.Parse<kParseNumbersAsStringsFlag>(ss, parser);
                }

                if (parser.data.size() == 0)
                {
                    parser.data.push_back(JsonValue(-1));
                }

                // fill array row by row
                for (size_t j = 0; j < parser.data.size(); ++j)
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

                    // fill array element
                    outputWriter.setInt(json_col, j + 1);

                    string v = parser.getValue(j);
                    if (!v.empty())
                    {
                        outputWriter.getStringRef(json_col + 1).copy(v.substr(0, element_len).data());
                    }
                    else
                    {
                        outputWriter.setNull(json_col + 1);
                    }

                    outputWriter.next();
                }
            }
            while (inputReader.next());

        }
        catch(std::exception& e)
        {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }

public:

    static int
    parseElementLength(ParamReader params)
    {
        int len = 32;
        if (params.containsParameter("element_length"))
        {
            len = params.getIntRef("element_length");
        }
        return len;
    }

};


class RapidArrayFactory : public TransformFunctionFactory
{

    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addInt("element_length");
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

        int element_len = RapidArrayExtractor::parseElementLength(srvInterface.getParamReader());
        outputTypes.addInt("rnk");
        outputTypes.addVarchar(element_len, "value");
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, RapidArrayExtractor);
    }

};

RegisterFactory(RapidArrayFactory);
