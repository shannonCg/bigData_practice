package com.shaice.bigdata.ch5;

import java.io.IOException;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

public class TestPartition {
    public static void main(final String[] args) {
        // writeSerializedObject();
        readSerializedObject();
    }

    public static void writeSerializedObject(){
        try{
            Pail<Login> loginPail = Pail.create("hdfs://10.211.55.11:9000/tmp/partitioned_logins", new PartitionedLoginPailStructure());
            TypedRecordOutputStream out = loginPail.openWrite();
            out.writeObject(new Login("chris", 1352702020));
            out.writeObject(new Login("david", 1352788472));
            out.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    public static void readSerializedObject(){
        try{
            Pail<Login> loginPail = new Pail<>("hdfs://10.211.55.11:9000/tmp/partitioned_logins");
            for(Login login: loginPail){
                System.out.println(login.userName+" "+login.loginUnixTime);
            }
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}