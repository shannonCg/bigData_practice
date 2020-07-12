package com.shaice.bigdata.superwebanalytics;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.backtype.hadoop.pail.PailStructure;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public abstract class ThriftPailStructure<T extends Comparable> implements PailStructure<T>{

    private transient TSerializer ser;
    private transient TDeserializer des;

    private TSerializer getSerializer(){
        if (Objects.isNull(ser))
            ser = new TSerializer();
        return ser;
    }

    private TDeserializer getDeserializer(){
        if (Objects.isNull(des))
            des = new TDeserializer();
        return des;
    }

    @Override
    public boolean isValidTarget(String... dirs) {
        return true;
    }

    @Override
    public List<String> getTarget(T object) {
        return Collections.emptyList();
    }
    
    @Override
    public byte[] serialize(T object) {
        try {
            return getSerializer().serialize((TBase)object);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(byte[] serialized) {
        T ret = createThriftObject();
        try {
            getDeserializer().deserialize((TBase)ret, serialized);
        } catch (TException e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    protected abstract T createThriftObject();
}