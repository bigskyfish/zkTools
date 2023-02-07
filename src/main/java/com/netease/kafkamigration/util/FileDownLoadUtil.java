package com.netease.kafkamigration.util;

import java.io.*;

public class FileDownLoadUtil {

    public static void downZKNode(){

    }


    public  static void writeFile(String fileName, String data) throws IOException {
        File txt = new File(fileName);
        if(txt.exists()){
            txt.delete();
        }
        txt.createNewFile();
        byte[] bytes = data.getBytes();
        FileOutputStream fos = new FileOutputStream(txt, false);
        fos.write(bytes);
        fos.close();
    }


    public static void copyFile(String file, String toPath) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        StringBuilder stringBuilder = new StringBuilder();
        String toFileName = "";
        String line;
        while ((line = bufferedReader.readLine()) != null){
            if(!line.startsWith(" ")){
                if(!toFileName.isEmpty()){
                    writeFile(toPath + toFileName, stringBuilder.toString());
                    stringBuilder = new StringBuilder();
                }
                String replace = line.replace(".", "_").replace(":", "");
                toFileName = replace + ".yaml";
            }
            stringBuilder.append(line + "\n");
        }
        inputStream.close();
        bufferedReader.close();

    }


    public static void main(String[] args) throws IOException {
        copyFile("/Users/netease/publicFile/kafkaSchema.yaml", "/Users/netease/publicFile/schema/");
    }


}
