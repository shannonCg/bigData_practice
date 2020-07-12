package com.shaice.bigdata.superwebanalytics;

import com.shaice.bigdata.superwebanalytics.schema.Data;

public class DataPailStructure extends ThriftPailStructure<Data> {

    @Override
    public Class getType() {
        return Data.class;
    }

    @Override
    protected Data createThriftObject() {
        return new Data();
    }

}