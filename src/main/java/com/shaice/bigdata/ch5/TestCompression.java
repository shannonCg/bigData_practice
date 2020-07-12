package com.shaice.bigdata.ch5;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.SequenceFileFormat;

public class TestCompression {
    public static void main(String[] args) {
        // writeUncompress();
        writeCompress();
        // readSerializedObject("logins_uncompress");
        // readSerializedObject("logins_compress");
    }

    public static void writeUncompress(){
        try{
            Pail<Login> loginPail = Pail.create("hdfs://10.211.55.11:9000/tmp/logins_uncompress", new LoginPailStructure());
            TypedRecordOutputStream out = loginPail.openWrite();
            out.writeObject(new Login("alex", 1352679231));
            out.writeObject(new Login("bob", 1352674216));
            out.writeObject(new Login("cat", 1352679231));
            out.writeObject(new Login("dog", 1352674216));
            out.writeObject(new Login("elephant", 1352679231));
            out.writeObject(new Login("fox", 1352674216));
            out.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    public static void writeCompress(){
        try{
            Map<String, Object> options = new HashMap<>();
            options.put(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
            options.put(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK);

            PailSpec pailSpec = new PailSpec("SequenceFile", options, new LoginPailStructure());
            Pail<Login> loginPail = Pail.create("hdfs://10.211.55.11:9000/tmp/logins_compress", pailSpec);
            TypedRecordOutputStream out = loginPail.openWrite();
            out.writeObject(new Login("alex", 1352679231));
            out.writeObject(new Login("bob", 1352674216));
            out.writeObject(new Login("cat", 1352679231));
            out.writeObject(new Login("dog", 1352674216));
            out.writeObject(new Login("elephant", 1352679231));
            out.writeObject(new Login("fox", 1352674216));
            out.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
    
    public static void readSerializedObject(String path){
        try{
            Pail<Login> loginPail = new Pail<>("hdfs://10.211.55.11:9000/tmp/"+path);
            for(Login login: loginPail){
                System.out.println(login.userName+" "+login.loginUnixTime);
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}