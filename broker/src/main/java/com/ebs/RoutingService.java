package com.ebs;

import com.ebs.project.proto.PublicationMessage;
import com.ebs.project.proto.SubscriptionMessage;
import com.google.protobuf.Timestamp;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import generator.models.BiTouple;
import generator.models.Operator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Preconditions;
import services.Lazy;
import services.PublishToNodeConnectionConfig;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RoutingService {

    public static final Map<SubscriptionMessage.SubscriptionDTO, Integer> routingCollection = new ConcurrentHashMap<>();

    public static final Map<Integer, PublishToNodeConnectionConfig> uniqueConfigs = new ConcurrentHashMap<>();

    public static final Map<PublishToNodeConnectionConfig, Lazy<BiTouple<Connection, Channel>>> neighborBrokers = new ConcurrentHashMap<>();

    public static PublishToNodeConnectionConfig currentNodePublicationConfig = null;

    public static boolean register(SubscriptionMessage.SubscriptionDTO subscriptionDTO) {
        PublishToNodeConnectionConfig publishToNodeConnectionConfig = PublishToNodeConnectionConfig.builder()
                .host(subscriptionDTO.getHost())
                .virtualHost(subscriptionDTO.getVirtualHost())
                .username(subscriptionDTO.getUsername())
                .password(subscriptionDTO.getPassword())
                .port(subscriptionDTO.getPort())
                .queueName(subscriptionDTO.getQueueName())
                .build();
        int hashCode = publishToNodeConnectionConfig.hashCode();
        if (!uniqueConfigs.containsKey(hashCode)) {
            uniqueConfigs.put(hashCode, publishToNodeConnectionConfig);
        }
        if (routingCollection.containsKey(subscriptionDTO)) {
            return false;
        }
        routingCollection.put(subscriptionDTO, hashCode);
        return true;
    }

    public static List<PublishToNodeConnectionConfig> getConfigsBy(PublicationMessage.PublicationDTO publicationDTO) {
        return routingCollection.keySet().stream()
                .filter(subscriptionDTO -> doesItMatch(subscriptionDTO, publicationDTO))
                .map(routingCollection::get)
                .distinct()
                .map(uniqueConfigs::get)
                .collect(Collectors.toList());
    }

    public static void registerBroker(PublishToNodeConnectionConfig brokerConnectionConfig) {
        Lazy<BiTouple<Connection, Channel>> connectionTuple = Lazy.of(() -> {
            try {
                var connection = createConnectionFor(brokerConnectionConfig);
                var channel = connection.createChannel();
                channel.queueDeclare("subscriptions", true, false, false, null);

                return new BiTouple<>(connection, channel);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });

        neighborBrokers.put(brokerConnectionConfig, connectionTuple);
    }

    public static void setCurrentNodePublicationConfig(PublishToNodeConnectionConfig config) {
        currentNodePublicationConfig = config;
    }

    private static Connection createConnectionFor(PublishToNodeConnectionConfig config) throws Exception {
        var connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(config.getHost())
                .setVirtualHost(config.getVirtualHost())
                .setUserName(config.getUsername())
                .setPassword(config.getPassword())
                .setPort(config.getPort())
                .build();

        return connectionConfig.getConnectionFactory().newConnection();
    }

    public static void forwardSubscription(SubscriptionMessage.SubscriptionDTO subscriptionDTO) {
        //Preconditions.checkNotNull(currentNodePublicationConfig);

        try {
            neighborBrokers.forEach((config, connectionTuple) -> {
                if (subscriptionDTO.getPort() == config.getPort()) {
                    return;
                }

                var forwardedSubscription = SubscriptionMessage.SubscriptionDTO.newBuilder()
                        .mergeFrom(subscriptionDTO)
                        .setHost(currentNodePublicationConfig.getHost())
                        .setVirtualHost(currentNodePublicationConfig.getVirtualHost())
                        .setUsername(currentNodePublicationConfig.getUsername())
                        .setPassword(currentNodePublicationConfig.getPassword())
                        .setPort(currentNodePublicationConfig.getPort())
                        .setQueueName(currentNodePublicationConfig.getQueueName())
                        .build();

                var connectionChannelTupleOptional = connectionTuple.getOptional();

                connectionChannelTupleOptional.ifPresent((connectionChannelTuple) -> {
                    var channel = connectionChannelTuple.getValue();

                    byte[] msg = forwardedSubscription.toByteArray();

                    String rk = "subscriptions";
                    String exchange = StringUtils.EMPTY;
                    AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().correlationId(UUID.randomUUID().toString()).build();

                    try {
                        channel.basicPublish(exchange, rk, false, false, properties, msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });
            });

        } catch (Exception e) {
        }

    }

    public static void ReForwardSubscription(SubscriptionMessage.SubscriptionDTO subscriptionDTO) {
        //Preconditions.checkNotNull(currentNodePublicationConfig);
        boolean skip = true;
        try {

            for (Map.Entry<PublishToNodeConnectionConfig, Lazy<BiTouple<Connection, Channel>>> entry : neighborBrokers.entrySet()) {
                PublishToNodeConnectionConfig config = entry.getKey();
                Lazy<BiTouple<Connection, Channel>> connectionTuple = entry.getValue();

                if (neighborBrokers.size() == 2 && skip) {
                    skip = false;
                    continue;
                }
                
                var forwardedSubscription = SubscriptionMessage.SubscriptionDTO.newBuilder()
                        .mergeFrom(subscriptionDTO)
                        .setHost(currentNodePublicationConfig.getHost())
                        .setVirtualHost(currentNodePublicationConfig.getVirtualHost())
                        .setUsername(currentNodePublicationConfig.getUsername())
                        .setPassword(currentNodePublicationConfig.getPassword())
                        .setPort(subscriptionDTO.getOriginalPort())
                        .setQueueName(subscriptionDTO.getOriginalQueueName())
                        .build();

                var connectionChannelTupleOptional = connectionTuple.getOptional();

                connectionChannelTupleOptional.ifPresent((connectionChannelTuple) -> {
                    var channel = connectionChannelTuple.getValue();

                    byte[] msg = forwardedSubscription.toByteArray();

                    String rk = "subscriptions";
                    String exchange = StringUtils.EMPTY;
                    AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().correlationId(UUID.randomUUID().toString()).build();

                    try {
                        channel.basicPublish(exchange, rk, false, false, properties, msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });
            }

        } catch (Exception e) {
        }

    }

    private static boolean doesItMatch(SubscriptionMessage.SubscriptionDTO subscriptionDTO,
                                       PublicationMessage.PublicationDTO publicationDTO) {

        return matches(subscriptionDTO.getCompanyName(), publicationDTO.getCompanyName())
                && matches(subscriptionDTO.getDrop(), publicationDTO.getDrop())
                && matches(subscriptionDTO.getValue(), publicationDTO.getValue())
                && matches(subscriptionDTO.getVariation(), publicationDTO.getVariation())
                && matches(subscriptionDTO.getDate(), publicationDTO.getDate());
    }

    private static boolean matches(SubscriptionMessage.SubscriptionDTO.ValueStringRestriction subscriptionValue,
                                   String publicationValue) {
        if (subscriptionValue.getValue().isBlank() && subscriptionValue.getOperation().isBlank()) {
            return true;
        }
        Operator operator = Operator.getOperator(subscriptionValue.getOperation());
        boolean nameEquals = subscriptionValue.getValue().equals(publicationValue);
        switch (operator) {
            case EQUAL:
                return nameEquals;
            case NOT_EQUAL:
                return !nameEquals;
            default:
                throw new IllegalArgumentException(operator.getOperator());
        }
    }

    private static boolean matches(SubscriptionMessage.SubscriptionDTO.ValueDoubleRestriction subscriptionValue,
                                   double publicationValue) {
        if (subscriptionValue.getValue() == 0 && subscriptionValue.getOperation().isBlank()) {
            return true;
        }
        Operator operator = Operator.getOperator(subscriptionValue.getOperation());
        switch (operator) {
            case LOWER:
                return publicationValue < subscriptionValue.getValue();
            case LOWER_OR_EQUAL:
                return publicationValue <= subscriptionValue.getValue();
            case BIGGER:
                return publicationValue > subscriptionValue.getValue();
            case BIGGER_OR_EQUAL:
                return publicationValue >= subscriptionValue.getValue();
            default:
                throw new IllegalArgumentException(operator.getOperator());
        }
    }

    private static boolean matches(SubscriptionMessage.SubscriptionDTO.ValueDateRestriction subscriptionValue,
                                   Timestamp publicationValue) {
        if (subscriptionValue.getOperation().isBlank()) {
            return true;
        }
        // S:23.05  P:22.05 =>  1
        // S:23.05  P:23.05 =>  0
        // S:23.05  P:24.05 => -1
        Operator operator = Operator.getOperator(subscriptionValue.getOperation());
        int datesCompared = getDateFrom(subscriptionValue.getValue()).compareTo(getDateFrom(publicationValue));
        switch (operator) {
            case LOWER:
                return datesCompared > 0;
            case LOWER_OR_EQUAL:
                return datesCompared >= 0;
            case BIGGER:
                return datesCompared < 1;
            case BIGGER_OR_EQUAL:
                return datesCompared <= 0;
            default:
                throw new IllegalArgumentException(operator.getOperator());
        }
    }

    private static LocalDate getDateFrom(Timestamp timestamp) {
        Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        return LocalDate.ofInstant(instant, ZoneId.systemDefault());
    }
}
