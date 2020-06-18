package com.ebs;

import com.ebs.project.proto.PublicationMessage;
import generator.models.Subscription;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import services.GeneratorService;
import services.PreferencesService;
import services.PublishToNodeConnectionConfig;
import services.deserializer.PublicationDTODeserializer;
import services.logger.EBSLogger;
import services.rabbit.ExactlyOncePublishOptions;
import services.rabbit.RMQSinkDurableQueue;
import services.serializer.SubscriptionMessageSerializer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.Map;
import java.util.UUID;

public class Subscriber {
    public static void main(String[] args) throws FileNotFoundException {
        Map<PreferencesService.ConfigKey, String> preferences = PreferencesService.getInstance().getPreferences();
        String host = preferences.get(PreferencesService.ConfigKey.HOST_IP);
        int paramValue = Integer.parseInt(args[0]);

//        int port = Integer.parseInt(preferences.get(PreferencesService.ConfigKey.getRandomPort()));
        int port = Integer.parseInt(preferences.get(PreferencesService.ConfigKey.getPort(paramValue)));

        //System.setOut(new PrintStream(new FileOutputStream("sub_" + paramValue + ".log")));

        System.out.println("RUNNING AT IP: " + host + "  | PORT: " + port);

        String virtualHost = preferences.get(PreferencesService.ConfigKey.VIRTUAL_HOST);
        String rabbitName = preferences.get(PreferencesService.ConfigKey.RABBIT_USER);
        String rabbitPass = preferences.get(PreferencesService.ConfigKey.RABBIT_PASS);
        String rabbitQueueSubscriptions = preferences.get(PreferencesService.ConfigKey.RABBIT_QUEUE_SUBSCRIPTIONS);
        int subscriptionCount = Integer.parseInt(preferences.get(PreferencesService.ConfigKey.SUBSCRIPTION_COUNT));

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var subscriptionList = GeneratorService.getSubscriptions(subscriptionCount);

        var subscriptions = env.fromCollection(subscriptionList);

        var connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(host)
                .setPort(port)
                .setVirtualHost(virtualHost)
                .setUserName(rabbitName)
                .setPassword(rabbitPass)
                .build();

        var sourceConnectionConfig = PublishToNodeConnectionConfig.builder()
                .host(host)
                .port(port)
                .virtualHost(virtualHost)
                .username(rabbitName)
                .password(rabbitPass)
                .queueName(UUID.randomUUID().toString())
                .build();

        var options = new ExactlyOncePublishOptions<Subscription>(rabbitQueueSubscriptions);
        var rmqSink = new RMQSinkDurableQueue<>(connectionConfig, new SubscriptionMessageSerializer(sourceConnectionConfig), options);

        subscriptions.addSink(rmqSink);
        subscriptions.print();

        initPublicationListener(port, env, sourceConnectionConfig);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initPublicationListener(int port, StreamExecutionEnvironment env,
                                                PublishToNodeConnectionConfig publishToNodeConnectionConfig) {

        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(publishToNodeConnectionConfig.getHost())
                .setPort(publishToNodeConnectionConfig.getPort())
                .setVirtualHost(publishToNodeConnectionConfig.getVirtualHost())
                .setUserName(publishToNodeConnectionConfig.getUsername())
                .setPassword(publishToNodeConnectionConfig.getPassword())
                .build();
        DataStream<PublicationMessage.PublicationDTO> publicationStream = env.addSource(
                new RMQSource<>(connectionConfig,
                        publishToNodeConnectionConfig.getQueueName(),
                        true,
                        new PublicationDTODeserializer())
        ).setParallelism(1);

        publicationStream.addSink(new SinkFunction<>() {
            @Override
            public void invoke(PublicationMessage.PublicationDTO value, Context context) {
                //EBSLogger.Subscriber.log(port, value);
                System.out.println(MessageFormat.format("Received {0}", value.toString()));
            }
        });
    }
}
