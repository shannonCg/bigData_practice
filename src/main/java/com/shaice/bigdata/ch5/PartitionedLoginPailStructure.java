package com.shaice.bigdata.ch5;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class PartitionedLoginPailStructure extends LoginPailStructure{

    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

    @Override
    public List<String> getTarget(Login object) {
        ArrayList<String> directoryPath = new ArrayList<>();
        Date date = new Date(object.loginUnixTime * 1000L);
        directoryPath.add(formatter.format(date));

        return directoryPath;
    }

    @Override
    public boolean isValidTarget(String... dirs) {
        try {
            return (formatter.parse(dirs[0]) != null);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}