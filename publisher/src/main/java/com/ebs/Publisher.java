/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ebs;

import generator.models.Publication;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import services.GeneratorService;
import services.PreferencesService;
import services.rabbit.ExactlyOncePublishOptions;
import services.rabbit.RMQSinkDurableQueue;
import services.serializer.PublicationMessageSerializer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static generator.utils.FileUtil.deserialize;
import static generator.utils.FileUtil.saveAndSerialize;

public class Publisher {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        Map<PreferencesService.ConfigKey, String> preferences = PreferencesService.getInstance().getPreferences();
        String host = preferences.get(PreferencesService.ConfigKey.HOST_IP);
        String virtualHost = preferences.get(PreferencesService.ConfigKey.VIRTUAL_HOST);
        String rabbitName = preferences.get(PreferencesService.ConfigKey.RABBIT_USER);
        String rabbitPass = preferences.get(PreferencesService.ConfigKey.RABBIT_PASS);
        String rabbitQueuePublications = preferences.get(PreferencesService.ConfigKey.RABBIT_QUEUE_PUBLICATIONS);
        int publicationCount = Integer.parseInt(preferences.get(PreferencesService.ConfigKey.PUBLICATION_COUNT));

        int paramValue = Integer.parseInt(args[0]);
        int port = Integer.parseInt(preferences.get(PreferencesService.ConfigKey.getPort(paramValue)));
        //System.setOut(new PrintStream(new FileOutputStream("pub_" + paramValue + ".log")));
        System.out.println("RUNNING AT IP: " + host + "  | PORT: " + port);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        var connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(host)
                .setPort(port)
                .setVirtualHost(virtualHost)
                .setUserName(rabbitName)
                .setPassword(rabbitPass)
                .build();

        var options = new ExactlyOncePublishOptions<Publication>(rabbitQueuePublications);
        var rmqSink = new RMQSinkDurableQueue<>(connectionConfig, new PublicationMessageSerializer(port), options);

        //long startTime = System.currentTimeMillis();
        //long threeMinutes = 180000;
        //long endTime = startTime  + threeMinutes;
        //while(System.currentTimeMillis() < endTime) {
        //    var publicationsList = GeneratorService.getPublications(publicationCount);
        //    var publications = env.fromCollection(publicationsList);
        //    publications.addSink(rmqSink);
        //    publications.print();
        //    Thread.sleep(100);
        //    try {
        //        env.execute();
        //    } catch (Exception e) {
        //        e.printStackTrace();
        //    }
        //}
        List<Publication> publicationsList = new ArrayList<>();

//      publicationsList = GeneratorService.getPublications(publicationCount);
        publicationsList = getSerializedPublications();

        var publications = env.fromCollection(publicationsList);
        publications.addSink(rmqSink);
        publications.print();
        //Thread.sleep(100);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static List<Publication> getSerializedPublications() {
        String filename = "C:\\Users\\mamatei\\Desktop\\Facultate\\Master\\EBS\\proiect\\ebs-publisher-subscriber\\generator.models.Publications-1592410442463.ser";

        try{
            return Objects.requireNonNull(deserialize(filename)).stream()
                    .filter(entry -> entry instanceof Publication)
                    .map(entry -> (Publication) entry)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
