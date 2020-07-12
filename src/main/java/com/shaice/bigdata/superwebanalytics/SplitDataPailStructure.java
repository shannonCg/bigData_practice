package com.shaice.bigdata.superwebanalytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.shaice.bigdata.superwebanalytics.schema.Data;
import com.shaice.bigdata.superwebanalytics.schema.DataUnit;

import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;

public class SplitDataPailStructure extends DataPailStructure{

    public static Map<Short, FieldStructure> validFieldMap = new HashMap<>();

    static {
        FieldStructure fieldStructure;
        for(DataUnit._Fields k: DataUnit.metaDataMap.keySet()){
            FieldValueMetaData meta = DataUnit.metaDataMap.get(k).valueMetaData;
            if(meta instanceof StructMetaData && 
                ((StructMetaData)meta).structClass.getName().endsWith("Property")){
                fieldStructure = new PropertyStructure(((StructMetaData)meta).structClass);
            }else{
                fieldStructure = new EdgeStructure();
            }
            validFieldMap.put(k.getThriftFieldId(), fieldStructure);
        }
    }

    @Override
    public List<String> getTarget(Data object) {
        List<String> ret = new ArrayList<>();
        DataUnit du = object.get_dataunit();
        short id = du.getSetField().getThriftFieldId();
        ret.add(""+id);
        validFieldMap.get(id).fillTarget(ret, du.getFieldValue());

        return ret;
    }

    @Override
    public boolean isValidTarget(String... dirs) {
        if(dirs.length == 0)
            return false;
        try {
            short id = Short.parseShort(dirs[0]);
            FieldStructure s = validFieldMap.get(id);
            if(Objects.isNull(s))
                return false;
            else
                return s.isValidTarget(dirs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}