package services.rabbit;

import com.ebs.project.proto.PublicationMessage;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import generator.models.BiTouple;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import services.PublishToNodeConnectionConfig;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RMQSinkMultipleOutputs<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RMQSink.class);

    private final int port;

    Map<PublishToNodeConnectionConfig, BiTouple<Connection, Map<QueueName, Channel>>> connectionCache = new HashMap<>();

    private int cnt;

    public RMQSinkMultipleOutputs(int port) {
        this.port = port;
        this.cnt = 0;
    }

    protected void setupQueue(String queueName, Channel channel) throws IOException {
        if (queueName != null) {
            channel.queueDeclare(queueName, true, false, false, null);
        }
    }

    @Override
    public void invoke(Object genericValue, Context context) throws Exception {

        BiTouple<PublicationMessage.PublicationDTO, List<PublishToNodeConnectionConfig>> value = (BiTouple<PublicationMessage.PublicationDTO,
                List<PublishToNodeConnectionConfig>>) genericValue;
        try {
            var publication = value.getKey();
            var messageSources = value.getValue();

            for (PublishToNodeConnectionConfig publishToNodeConnectionConfig : messageSources) {
                if(publishToNodeConnectionConfig == null) {
                    continue;
                }
                if (publication.getPort() == publishToNodeConnectionConfig.getPort()
                        && publishToNodeConnectionConfig.getQueueName().equals("publications")) {
                    continue;
                }
                if (!connectionCache.containsKey(publishToNodeConnectionConfig)) {
                    RMQConnectionConfig rmqConnectionConfig = new RMQConnectionConfig.Builder()
                            .setHost(publishToNodeConnectionConfig.getHost())
                            .setVirtualHost(publishToNodeConnectionConfig.getVirtualHost())
                            .setUserName(publishToNodeConnectionConfig.getUsername())
                            .setPassword(publishToNodeConnectionConfig.getPassword())
                            .setPort(publishToNodeConnectionConfig.getPort())
                            .build();

                    ConnectionFactory factory = rmqConnectionConfig.getConnectionFactory();
                    var connection = factory.newConnection();
                    var channel = connection.createChannel();

                    if (channel == null) { // MAP CONNECTION CONFIG TO CHANNEL TO REUSE
                        throw new RuntimeException("None of RabbitMQ channels are available");
                    }

                    setupQueue(publishToNodeConnectionConfig.getQueueName(), channel);

                    Map<QueueName, Channel> channelList = new HashMap<>();
                    channelList.put(new QueueName(publishToNodeConnectionConfig.getQueueName()), channel);
                    var cacheValue = new BiTouple<>(connection, channelList);

                    connectionCache.put(publishToNodeConnectionConfig, cacheValue);
                }

                var cachedConnection = connectionCache.get(publishToNodeConnectionConfig);
                Map<QueueName, Channel> channels = cachedConnection.getValue();

                Channel channel = channels.get(new QueueName(publishToNodeConnectionConfig.getQueueName()));

                if (channel == null) {
                    throw new RuntimeException("This should never happen");
                }
                byte[] msg = PublicationMessage.PublicationDTO.newBuilder()
                        .mergeFrom(publication)
                        .setPort(this.port)
                        .build()
                        .toByteArray();

                var publishOptions = new RMQSinkPublishOptions<BiTouple<PublicationMessage.PublicationDTO, List<PublishToNodeConnectionConfig>>>() {

                    @Override
                    public String computeRoutingKey(BiTouple<PublicationMessage.PublicationDTO, List<PublishToNodeConnectionConfig>> a) {
                        return publishToNodeConnectionConfig.getQueueName();
                    }

                    @Override
                    public AMQP.BasicProperties computeProperties(BiTouple<PublicationMessage.PublicationDTO, List<PublishToNodeConnectionConfig>> a) {
                        return new AMQP.BasicProperties().builder().correlationId(UUID.randomUUID().toString()).build();
                    }

                    @Override
                    public String computeExchange(BiTouple<PublicationMessage.PublicationDTO, List<PublishToNodeConnectionConfig>> a) {
                        return StringUtils.EMPTY;
                    }
                };

                boolean mandatory = publishOptions.computeMandatory(value);
                boolean immediate = publishOptions.computeImmediate(value);

                String rk = publishOptions.computeRoutingKey(value);
                String exchange = publishOptions.computeExchange(value);

                channel.basicPublish(exchange, rk, mandatory, immediate,
                        publishOptions.computeProperties(value), msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        Exception exception = null;

        Collection<BiTouple<Connection, Map<QueueName, Channel>>> connectionsAndChannels = connectionCache.values();
        for (BiTouple<Connection, Map<QueueName, Channel>> CCs : connectionsAndChannels) {
            Connection connection = CCs.getKey();
            Map<QueueName, Channel> channels = CCs.getValue();

            for (Channel channel : channels.values()) {
                try {
                    channel.close();
                } catch (Exception e) {
                    exception = e;
                    //System.out.println("Could not close channel");
                }
            }

            try {
                connection.close();
            } catch (Exception e) {
                exception = e;
                //System.out.println("Could not close connection");
            }

        }

        if (exception != null) {
            throw new RuntimeException("Error while closing", exception);
        }
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    private static final class QueueName { // should be just a type alias for String

        @NonNull
        final String name;
    }


}
