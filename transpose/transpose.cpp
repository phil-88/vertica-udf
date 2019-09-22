/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/*
 * Description: Example User Defined Analytic Function Lag.
 *
 * Create Date: Nov 22, 2011
 */

#include "Vertica.h"
#include <string>
#include <sstream>
#include <iostream>
#include <vector>
#include <algorithm>

using namespace Vertica;
using namespace std;


std::vector<std::pair<std::string, int> >
getDimentionColumns(ServerInterface &srvInterface, const SizedColumnTypes &inputTypes)
{
    std::map<string, int> columnNo;
    for (size_t i = 0; i < inputTypes.getColumnCount(); ++i)
    {
        string n = inputTypes.getColumnName(i);
        transform(n.begin(), n.end(), n.begin(), ::tolower);
        columnNo[n] = i;
    }

    std::vector<std::pair<std::string, int> > dimentionColumns;
    if (srvInterface.getParamReader().containsParameter("dimentions"))
    {
        istringstream f(srvInterface.getParamReader().getStringRef("dimentions").str());
        string dimention;
        while (getline(f, dimention, ','))
        {
            transform(dimention.begin(), dimention.end(), dimention.begin(), ::tolower);

            auto i = columnNo.find(dimention);
            dimentionColumns.push_back(make_pair(dimention, i != columnNo.end() ? i->second : -1));
        }
    }
    return dimentionColumns;
}

std::string
getStringValue(PartitionReader &inputReader, int idx, VerticaType t)
{
    if (t.isStringType())
    {
        return inputReader.getStringRef(idx).str();
    }
    else if (t.isInt())
    {
        std::ostringstream ss;
        ss << inputReader.getIntRef(idx);
        return ss.str();
    }
    else if (t.isFloat())
    {
        std::ostringstream ss;
        ss << inputReader.getFloatRef(idx);
        return ss.str();
    }
    else if (t.isBool())
    {
        return std::string(inputReader.getBoolRef(idx) ? "t" : "f");
    }
    else
    {
        return std::string();
    }
}


class Transpose : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter)
    {
        try {

            const SizedColumnTypes &metaData = inputReader.getTypeMetaData();

            vector<int> dimentions;
            std::vector<std::pair<std::string, int> > dimentionColumns = getDimentionColumns(srvInterface, metaData);
            for (int i = 0; i < (int)dimentionColumns.size(); ++i)
            {
                std::string name = dimentionColumns[i].first;
                int dim_col = dimentionColumns[i].second;
                dimentions.push_back(dim_col);
            }

            bool isNum = true;
            vector<int> metrics;
            for (size_t i = 0; i < metaData.getColumnCount(); ++i)
            {
                if (find(dimentions.begin(), dimentions.end(), i) == dimentions.end())
                {
                    metrics.push_back(i);
                    isNum = isNum && metaData.getColumnType(i).isInt();
                    srvInterface.log("metric: %s", metaData.getColumnName(i).c_str());
                }
                else
                {
                    srvInterface.log("dimention: %s", metaData.getColumnName(i).c_str());
                }
            }
            int metric_name_idx = dimentions.size();
            int metric_value_idx = dimentions.size() + 1;

            do
            {
                for (size_t i = 0; i < metrics.size(); i++)
                {
                    if (!inputReader.isNull(metrics.at(i)))
                    {
                        for (size_t j = 0; j < dimentions.size(); j++)
                        {
                            if (metaData.getColumnType(dimentions.at(j)).isInt())
                            {
                                outputWriter.setInt(j, inputReader.getIntRef(dimentions.at(j)));
                            }
                            else if (metaData.getColumnType(dimentions.at(j)).isVarchar())
                            {
                                outputWriter.getStringRef(j).copy(inputReader.getStringRef(dimentions.at(j)));
                            }
                        }

                        const string &columnName = metaData.getColumnName(metrics.at(i));
                        VString &key = outputWriter.getStringRef(metric_name_idx);
                        key.copy(columnName);

                        if (isNum)
                        {
                            const vint &value = inputReader.getIntRef(metrics.at(i));
                            outputWriter.setInt(metric_value_idx, value);
                        }
                        else
                        {
                            const string &value = getStringValue(inputReader, metrics.at(i), metaData.getColumnType(metrics.at(i)));
                            VString &out = outputWriter.getStringRef(metric_value_idx);
                            out.copy(value);
                        }

                        outputWriter.next();
                    }
                }
            }
            while (inputReader.next());

        }
        catch(std::exception& e)
        {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};


class TransposeFactory : public TransformFunctionFactory
{

    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(1024, "dimentions", SizedColumnTypes::Properties(false, true, false, "columns contaning dimentions"));
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
        if (inputTypes.getColumnCount() < 1)
        {
            vt_report_error(0, "Function accepts 1 or more arguments, no arguments provided");
        }

        std::set<int> dimentions;

        std::vector<std::pair<std::string, int> > dimentionColumns = getDimentionColumns(srvInterface, inputTypes);
        for (int i = 0; i < (int)dimentionColumns.size(); ++i)
        {
            std::string name = dimentionColumns[i].first;
            int no = dimentionColumns[i].second;
            if (no == -1)
            {
                vt_report_error(0, "No dimention column %s found", name.data());
            }
            else if (inputTypes.getColumnType(no).isVarchar())
            {
                outputTypes.addVarchar(inputTypes.getColumnType(no).getStringLength(), name);
            }
            else if (inputTypes.getColumnType(no).isInt())
            {
                outputTypes.addInt(name);
            }
            else
            {
                vt_report_error(0, "Only integer and varchar dimentions supported");
            }
            dimentions.insert(no);
        }

        bool isNum = true;
        int keySize = 256;
        int valueSize = 256;

        for (size_t i = 0; i < inputTypes.getColumnCount(); ++i)
        {
            if (dimentions.find(i) == dimentions.end())
            {
                VerticaType t = inputTypes.getColumnType(i);
                isNum = isNum && t.isInt();
                valueSize = max(valueSize, (int)t.getStringLength(false));
                keySize = max(keySize, (int)inputTypes.getColumnName(i).size());
            }
        }

        outputTypes.addVarchar(keySize, "col");
        isNum ? outputTypes.addInt("value") : outputTypes.addVarchar(valueSize, "value");
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, Transpose);
    }

};

RegisterFactory(TransposeFactory);
