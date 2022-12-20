package com.netease.kafkamigration.kafka.io;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Scanner;

public class NIOClient {


    public static void main(String[] args) throws IOException {
        Scanner scan = new Scanner(System.in);
        while (true){
            Socket socket = new Socket("127.0.0.1", 9999);
            OutputStream outputStream = socket.getOutputStream();
            System.out.println("请输入：");
            String data = scan.next();
            if ("q".equalsIgnoreCase(data)){
                break;
            }
            outputStream.write(data.getBytes());
            outputStream.close();
        }
        scan.close();
    }
}
