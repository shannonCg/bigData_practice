package com.shaice.bigdata.ch5;


import java.io.IOException;import com.backtype.hadoop.pail.Pail;

import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

public class TestAppendData {
    public static void main(String[] args) {
        // writeSerializedObject();
        // appendData();
        readSerializedObject();
    }

    public static void writeSerializedObject(){
        try{
            Pail<Login> loginPail = Pail.create("hdfs://10.211.55.11:9000/tmp/updates", new LoginPailStructure());
            TypedRecordOutputStream out = loginPail.openWrite();
            out.writeObject(new Login("aaa", 1300000000));
            out.writeObject(new Login("bbb", 1400000000));
            out.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    public static void appendData(){
        try{
            Pail<Login> loginPail = new Pail<>("hdfs://10.211.55.11:9000/tmp/logins");
            Pail<Login> updatePail = new Pail<>("hdfs://10.211.55.11:9000/tmp/updates");
            loginPail.absorb(updatePail);
            loginPail.consolidate();

        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    public static void readSerializedObject(){
        try{
            Pail<Login> loginPail = new Pail<>("hdfs://10.211.55.11:9000/tmp/logins");
            for(Login login: loginPail){
                System.out.println(login.userName+" "+login.loginUnixTime);
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}