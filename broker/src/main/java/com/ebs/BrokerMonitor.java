package com.ebs;

import lombok.SneakyThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BrokerMonitor extends Thread {

    private String NextBroker;

    public BrokerMonitor(String nextBroker) {
        this.NextBroker = nextBroker;
    }

    ProcessBuilder pb = new ProcessBuilder(cmds);

    private static final String[] cmd = {"C:\\jdk-14.0.1\\bin\\java.exe", "-javaagent:C:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3.4\\lib\\idea_rt.jar=49201:C:\\Program Files\\JetBrains\\IntelliJ IDEA 2019.3.4\\bin", "-Dfile.encoding=UTF-8", "-classpath", "C:\\Users\\mamatei\\Desktop\\Facultate\\Master\\EBS\\proiect\\ebs-publisher-subscriber\\broker\\target\\classes;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-java\\1.10.1\\flink-java-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-core\\1.10.1\\flink-core-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-annotations\\1.10.1\\flink-annotations-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-metrics-core\\1.10.1\\flink-metrics-core-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\esotericsoftware\\kryo\\kryo\\2.24.0\\kryo-2.24.0.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\esotericsoftware\\minlog\\minlog\\1.2\\minlog-1.2.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\objenesis\\objenesis\\2.1\\objenesis-2.1.jar;C:\\Users\\mamatei\\.m2\\repository\\commons-collections\\commons-collections\\3.2.2\\commons-collections-3.2.2.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\commons\\commons-compress\\1.18\\commons-compress-1.18.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-shaded-asm-7\\7.1-9.0\\flink-shaded-asm-7-7.1-9.0.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\commons\\commons-lang3\\3.3.2\\commons-lang3-3.3.2.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\commons\\commons-math3\\3.5\\commons-math3-3.5.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\slf4j\\slf4j-api\\1.7.15\\slf4j-api-1.7.15.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\google\\code\\findbugs\\jsr305\\1.3.9\\jsr305-1.3.9.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\force-shading\\1.10.1\\force-shading-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-streaming-java_2.12\\1.10.1\\flink-streaming-java_2.12-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-runtime_2.12\\1.10.1\\flink-runtime_2.12-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-queryable-state-client-java\\1.10.1\\flink-queryable-state-client-java-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-hadoop-fs\\1.10.1\\flink-hadoop-fs-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\commons-io\\commons-io\\2.4\\commons-io-2.4.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-shaded-netty\\4.1.39.Final-9.0\\flink-shaded-netty-4.1.39.Final-9.0.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-shaded-jackson\\2.10.1-9.0\\flink-shaded-jackson-2.10.1-9.0.jar;C:\\Users\\mamatei\\.m2\\repository\\commons-cli\\commons-cli\\1.3.1\\commons-cli-1.3.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\javassist\\javassist\\3.24.0-GA\\javassist-3.24.0-GA.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\scala-lang\\scala-library\\2.12.7\\scala-library-2.12.7.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\typesafe\\akka\\akka-actor_2.12\\2.5.21\\akka-actor_2.12-2.5.21.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\typesafe\\config\\1.3.3\\config-1.3.3.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\scala-lang\\modules\\scala-java8-compat_2.12\\0.8.0\\scala-java8-compat_2.12-0.8.0.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\typesafe\\akka\\akka-stream_2.12\\2.5.21\\akka-stream_2.12-2.5.21.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\reactivestreams\\reactive-streams\\1.0.2\\reactive-streams-1.0.2.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\typesafe\\ssl-config-core_2.12\\0.3.7\\ssl-config-core_2.12-0.3.7.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\scala-lang\\modules\\scala-parser-combinators_2.12\\1.1.1\\scala-parser-combinators_2.12-1.1.1.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\typesafe\\akka\\akka-protobuf_2.12\\2.5.21\\akka-protobuf_2.12-2.5.21.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\typesafe\\akka\\akka-slf4j_2.12\\2.5.21\\akka-slf4j_2.12-2.5.21.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\clapper\\grizzled-slf4j_2.12\\1.3.2\\grizzled-slf4j_2.12-1.3.2.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\github\\scopt\\scopt_2.12\\3.5.0\\scopt_2.12-3.5.0.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\xerial\\snappy\\snappy-java\\1.1.4\\snappy-java-1.1.4.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\twitter\\chill_2.12\\0.7.6\\chill_2.12-0.7.6.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\twitter\\chill-java\\0.7.6\\chill-java-0.7.6.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\lz4\\lz4-java\\1.5.0\\lz4-java-1.5.0.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-clients_2.12\\1.10.1\\flink-clients_2.12-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-optimizer_2.12\\1.10.1\\flink-optimizer_2.12-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-shaded-guava\\18.0-9.0\\flink-shaded-guava-18.0-9.0.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\flink\\flink-connector-rabbitmq_2.12\\1.10.1\\flink-connector-rabbitmq_2.12-1.10.1.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\rabbitmq\\amqp-client\\4.2.0\\amqp-client-4.2.0.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\slf4j\\slf4j-log4j12\\1.7.7\\slf4j-log4j12-1.7.7.jar;C:\\Users\\mamatei\\.m2\\repository\\log4j\\log4j\\1.2.17\\log4j-1.2.17.jar;C:\\Users\\mamatei\\Desktop\\Facultate\\Master\\EBS\\proiect\\ebs-publisher-subscriber\\common\\target\\classes;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\logging\\log4j\\log4j-api\\2.13.3\\log4j-api-2.13.3.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\apache\\logging\\log4j\\log4j-core\\2.13.3\\log4j-core-2.13.3.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\google\\protobuf\\protobuf-java\\3.12.0\\protobuf-java-3.12.0.jar;C:\\Users\\mamatei\\.m2\\repository\\com\\googlecode\\json-simple\\json-simple\\1.1.1\\json-simple-1.1.1.jar;C:\\Users\\mamatei\\.m2\\repository\\junit\\junit\\4.10\\junit-4.10.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\hamcrest\\hamcrest-core\\1.1\\hamcrest-core-1.1.jar;C:\\Users\\mamatei\\.m2\\repository\\org\\projectlombok\\lombok\\1.18.12\\lombok-1.18.12.jar;C:\\Users\\mamatei\\.m2\\repository\\commons-codec\\commons-codec\\1.9\\commons-codec-1.9.jar", "com.ebs.Broker"};
    private static final List<String> cmds = new ArrayList<String>();

    @SneakyThrows
    public void run() {

        cmds.addAll(Arrays.asList(cmd));
        cmds.add(NextBroker);

        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        System.out.println("Starting broker "+ this.NextBroker);

        Process process = CreateBroker2();

        while (true) {
            if (!process.isAlive()) {

                System.out.println("Broker is down let's restart=>Broker "+this.NextBroker);
                var routingCollection = RoutingService.routingCollection;

                Thread.sleep(5000);
                System.out.println(routingCollection.keySet());
                for (var value : routingCollection.keySet()) {
                    RoutingService.ReForwardSubscription(value);
                }

                process = CreateBroker2();
                Thread.sleep(5000);


            }
        }
    }

    private Process CreateBroker2() {
        Process p = null;

        try {
            p = pb.start();

            long pid = p.pid();
            System.out.println("Broker " + this.NextBroker+  " PID: " + pid);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return p;
    }
}
