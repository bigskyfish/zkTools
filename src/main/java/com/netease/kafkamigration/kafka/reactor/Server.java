package com.netease.kafkamigration.kafka.reactor;

public class Server {


    public static void main(String[] args) {
        new Thread(new Reactor(9999)).start();
    }
}
