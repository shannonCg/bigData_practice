package com.shaice.bigdata.ch5;

import java.io.IOException;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

public class Test {
    public static void main(String[] args){
        readSerializedObject();
        
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
    
    public static void writeSerializedObject(){
        try{
            Pail<Login> loginPail = Pail.create("hdfs://10.211.55.11:9000/tmp/logins", new LoginPailStructure());
            TypedRecordOutputStream out = loginPail.openWrite();
            out.writeObject(new Login("alex", 1352679231));
            out.writeObject(new Login("bob", 1352674216));
            out.close();
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