package com.shaice.bigdata.ch5;

import java.io.IOException;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

public class TestSimpeWrite {
    public static void main(String[] args){
        simpleWrite();
        
    }

    public static void simpleWrite(){
        try{
            Pail pail = Pail.create("hdfs://10.211.55.11:9000/tmp/mypail");
            TypedRecordOutputStream os = pail.openWrite();
            os.writeObject(new byte[]{1, 2, 3});
            os.writeObject(new byte[]{1, 2, 3, 4});
            os.writeObject(new byte[]{1, 2, 3, 4, 5});
            os.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
    
}