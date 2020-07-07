package com.shaice.bigdata.ch5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.backtype.hadoop.pail.PailStructure;

public class LoginPailStructure implements PailStructure<Login> {

    @Override
    public boolean isValidTarget(String... dirs) {
        return true;
    }

    @Override
    public Login deserialize(byte[] serialized) {
        DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(serialized));
        try {
            byte[] userBytes = new byte[dataIn.readInt()];
            dataIn.read(userBytes);

            return new Login(new String(userBytes), dataIn.readLong());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(Login object) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);
        byte[] userBytes = object.userName.getBytes();
        try {
            dataOut.writeInt(userBytes.length);
            dataOut.write(userBytes);
            dataOut.writeLong(object.loginUnixTime);
            dataOut.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return byteOut.toByteArray();
    }

    @Override
    public List<String> getTarget(Login object) {
        return Collections.emptyList();
    }

    @Override
    public Class getType() {
        return Login.class;
    }

   
}