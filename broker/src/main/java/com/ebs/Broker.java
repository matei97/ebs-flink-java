package com.ebs;

import com.ebs.project.proto.PublicationMessage;
import com.ebs.project.proto.SubscriptionMessage;
import com.google.protobuf.AbstractMessageLite;
import generator.models.BiTouple;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import services.PreferencesService;
import services.PublishToNodeConnectionConfig;
import services.deserializer.PublicationDTODeserializer;
import services.deserializer.SubscriptionDTODeserializer;
import services.rabbit.ExactlyOncePublishOptions;
import services.rabbit.RMQSinkDurableQueue;
import services.rabbit.RMQSinkMultipleOutputs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Broker {

    static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    static int brokerNumber = 0;

    public static void main(String[] args) throws IOException {


        brokerNumber = Integer.parseInt(args[0]);
        Map<PreferencesService.ConfigKey, String> preferences = PreferencesService.getInstance().getPreferences();
        String host = preferences.get(PreferencesService.ConfigKey.HOST_IP);
        int port1 = Integer.parseInt(preferences.get(PreferencesService.ConfigKey.HOST_PORT_1));
        int port2 = Integer.parseInt(preferences.get(PreferencesService.ConfigKey.HOST_PORT_2));
        int port3 = Integer.parseInt(preferences.get(PreferencesService.ConfigKey.HOST_PORT_3));
        String virtualHost = preferences.get(PreferencesService.ConfigKey.VIRTUAL_HOST);
        String rabbitName = preferences.get(PreferencesService.ConfigKey.RABBIT_USER);
        String rabbitPass = preferences.get(PreferencesService.ConfigKey.RABBIT_PASS);
        String rabbitQueueSubscriptions = preferences.get(PreferencesService.ConfigKey.RABBIT_QUEUE_SUBSCRIPTIONS);
        String rabbitQueuePublications = preferences.get(PreferencesService.ConfigKey.RABBIT_QUEUE_PUBLICATIONS);

        int portIn, portOut;

        switch (brokerNumber) {
            case 1: {
//                System.out.printf("Current pid " + ProcessHandle.current().pid());
                BrokerMonitor monitor = new BrokerMonitor("2");
                monitor.start();
                portIn = port1;
                portOut = port2;
                break;
            }
            case 2: {
                BrokerMonitor monitor = new BrokerMonitor("3");
                monitor.start();

                portIn = port2;
                portOut = port3;
                break;
            }
            case 3: {
                portIn = port3;
                portOut = port2;
                break;
            }
            default: {
                throw new UnsupportedOperationException("Invalid option argument");
            }
        }
        System.out.println("RUNNING AT IP: " + host + "  | PORT: " + portIn);

//checkpointing section
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        env.setStateBackend(new FsStateBackend("file:///C:/Users/mamatei/Desktop/Facultate/Master/EBS/proiect/ebs-publisher-subscriber/checkpoints", false));
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        var connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(host)
                .setPort(portIn)
                .setVirtualHost(virtualHost)
                .setUserName(rabbitName)
                .setPassword(rabbitPass)
                .build();

        initSubscriptionListener(env, connectionConfig, rabbitQueueSubscriptions);
        initPublicationListener(env, connectionConfig, rabbitQueuePublications, portIn);

        buildConfig(host, portIn, portOut, virtualHost, rabbitName, rabbitPass, rabbitQueueSubscriptions, rabbitQueuePublications);
        if (brokerNumber == 2) {
            buildConfig(host, portIn, port1, virtualHost, rabbitName, rabbitPass, rabbitQueueSubscriptions, rabbitQueuePublications);
        }

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void buildConfig(String host, int portIn, int portOut, String virtualHost, String rabbitName, String rabbitPass,
                                    String rabbitQueueSubscriptions, String rabbitQueuePublications) {
        var brokerConfig = PublishToNodeConnectionConfig.builder()
                .host(host)
                .port(portOut)
                .virtualHost(virtualHost)
                .username(rabbitName)
                .password(rabbitPass)
                .queueName(rabbitQueueSubscriptions)
                .build();

        var currentPublishConfig = PublishToNodeConnectionConfig.builder()
                .host(host)
                .port(portIn)
                .virtualHost(virtualHost)
                .username(rabbitName)
                .password(rabbitPass)
                .queueName(rabbitQueuePublications)
                .build();

        RoutingService.setCurrentNodePublicationConfig(currentPublishConfig);
        RoutingService.registerBroker(brokerConfig);
    }

    private static void initPublicationListener(StreamExecutionEnvironment env, RMQConnectionConfig connectionConfig,
                                                String rabbitQueuePublications, int port) {
        var publicationStream = env.addSource(
                new RMQSource<>(connectionConfig,
                        rabbitQueuePublications,
                        true,
                        new PublicationDTODeserializer())
        ).setParallelism(1);

        publicationStream
                .map((MapFunction<PublicationMessage.PublicationDTO, Object>) value -> {
                    List<PublishToNodeConnectionConfig> outputsThatMatchPublication = RoutingService.getConfigsBy(value);
                    //Logger.Broker.log(port, value, outputsThatMatchPublication);
                    return new BiTouple<>(value, outputsThatMatchPublication);
                })
                .addSink(new RMQSinkMultipleOutputs<>(port));
    }

    // WHAT IS THIS USED FOR?
    private static void initPublicationSender(PublishToNodeConnectionConfig publishToNodeConnectionConfig,
                                              PublicationMessage.PublicationDTO value) {

        var publications = env.fromElements(value);
        var connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(publishToNodeConnectionConfig.getHost())
                .setPort(publishToNodeConnectionConfig.getPort())
                .setVirtualHost(publishToNodeConnectionConfig.getVirtualHost())
                .setUserName(publishToNodeConnectionConfig.getUsername())
                .setPassword(publishToNodeConnectionConfig.getPassword())
                .build();

        var options = new ExactlyOncePublishOptions<PublicationMessage.PublicationDTO>(publishToNodeConnectionConfig.getQueueName());
        var rmqSink = new RMQSinkDurableQueue<>(connectionConfig,
                (SerializationSchema<PublicationMessage.PublicationDTO>) AbstractMessageLite::toByteArray, options);

        publications.addSink(rmqSink);
        //publications.print();
    }

    private static void initSubscriptionListener(StreamExecutionEnvironment env,
                                                 RMQConnectionConfig connectionConfig, String rabbitQueueSubscriptions) {
        DataStream<SubscriptionMessage.SubscriptionDTO> subscriptionStream = env.addSource(
                new RMQSource<>(connectionConfig,
                        rabbitQueueSubscriptions,
                        true,
                        new SubscriptionDTODeserializer())
        ).setParallelism(1);
        subscriptionStream.addSink(new SinkFunction<>() {
            @SneakyThrows
            @Override
            public void invoke(SubscriptionMessage.SubscriptionDTO value, Context context) {
                if (RoutingService.register(value)) {
                    RoutingService.forwardSubscription(value);
                    System.out.println(MessageFormat.format("Broker {0} Received {1}", brokerNumber, value.toString()));
                    //System.out.println(MessageFormat.format("Got {0}", value.toString()));
                }
            }
        });
    }

}

