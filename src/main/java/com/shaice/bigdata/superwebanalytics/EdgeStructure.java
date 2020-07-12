package com.shaice.bigdata.superwebanalytics;

import java.util.List;

public class EdgeStructure implements FieldStructure {

    @Override
    public boolean isValidTarget(String[] dirs) {
        return true;
    }

    @Override
    public void fillTarget(List<String> ret, Object val) {
    }

}