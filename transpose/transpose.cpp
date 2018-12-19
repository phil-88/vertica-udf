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


class Transpose : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter)
    {
        try {

            const SizedColumnTypes &metaData = inputReader.getTypeMetaData();

            istringstream f(srvInterface.getParamReader().getStringRef("dimentions").str());
            vector<int> dimentions;
            string dimention;
            while (getline(f, dimention, ','))
            {
                transform(dimention.begin(), dimention.end(), dimention.begin(), ::tolower);
                int dim_col = -1;
                for (int i = 0; i < metaData.getColumnCount(); ++i)
                {
                    string col = metaData.getColumnName(i);
                    transform(col.begin(), col.end(), col.begin(), ::tolower);
                    if (col == dimention)
                    {
                        dim_col = i;
                        break;
                    }
                }
                if (dim_col == -1)
                {
                    vt_report_error(0, "No dimention column %s found", dimention.data());
                    return;
                }
                else if (!metaData.getColumnType(dim_col).isInt() && !metaData.getColumnType(dim_col).isVarchar())
                {
                    vt_report_error(0, "Only integer and varchar dimentions supported");
                    return;
                }
                else
                {
                    dimentions.push_back(dim_col);
                }
            }

            vector<int> metrics;
            for (int i = 0; i < metaData.getColumnCount(); ++i)
            {
                if (find(dimentions.begin(), dimentions.end(), i) == dimentions.end())
                {
                    metrics.push_back(i);
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

                        const vint &value = inputReader.getIntRef(metrics.at(i));
                        outputWriter.setInt(metric_value_idx, value);

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

        std::map<string, int> dimentionTypes;
        for (int i = 0; i < inputTypes.getColumnCount(); ++i)
        {
            string n = inputTypes.getColumnName(i);
            dimentionTypes[n] = i;
        }

        if (srvInterface.getParamReader().containsParameter("dimentions"))
        {
            istringstream f(srvInterface.getParamReader().getStringRef("dimentions").str());
            string dimention;
            while (getline(f, dimention, ','))
            {
                if (inputTypes.getColumnType(dimentionTypes[dimention]).isVarchar())
                {
                    outputTypes.addVarchar(inputTypes.getColumnType(dimentionTypes[dimention]).getStringLength(), dimention);
                }
                else if (inputTypes.getColumnType(dimentionTypes[dimention]).isInt())
                {
                    outputTypes.addInt(dimention);
                }
                else
                {
                    vt_report_error(0, "Only integer and varchar dimentions supported");
                }
            }
        }

        outputTypes.addVarchar(128, "col");
        outputTypes.addInt("value");
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, Transpose);
    }

};

RegisterFactory(TransposeFactory);
