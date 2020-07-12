package com.shaice.bigdata.superwebanalytics;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TUnion;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;

public class PropertyStructure implements FieldStructure {

    private TFieldIdEnum valueId;
    private Set<Short> validIds;

    public PropertyStructure(Class prop){
        try {
            Map<TFieldIdEnum, FieldMetaData> propMeta = getMetadataMap(prop);
            Class valClass = Class.forName(prop.getName()+"Value");
            valueId = getIdForClass(propMeta, valClass);

            validIds = new HashSet<>();
            Map<TFieldIdEnum, FieldMetaData> valMeta = getMetadataMap(valClass);
            for(TFieldIdEnum validId: valMeta.keySet()){
                validIds.add(validId.getThriftFieldId());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<TFieldIdEnum, FieldMetaData> getMetadataMap(Class c){
        try {
            Object o = c.newInstance();
            return (Map) c.getField("metaDataMap").get(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static TFieldIdEnum getIdForClass(Map<TFieldIdEnum, FieldMetaData> meta, Class toFind){
        for(TFieldIdEnum k: meta.keySet()){
            FieldValueMetaData md = meta.get(k).valueMetaData;
            if(md instanceof StructMetaData){
                if(toFind.equals(((StructMetaData)md).structClass)){
                    return k;
                }
            }
        }

        throw new RuntimeException("Could not find"+toFind.toString()+" in "+meta.toString());
    }

    @Override
    public boolean isValidTarget(String[] dirs) {
        // if(dirs.length < 2)
        //     return false;
        try {
            short s = Short.parseShort(dirs[1]);
            return validIds.contains(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fillTarget(List<String> ret, Object val) {
        ret.add(""+((TUnion)((TBase)val).getFieldValue(valueId)).getSetField().getThriftFieldId());
    }

}