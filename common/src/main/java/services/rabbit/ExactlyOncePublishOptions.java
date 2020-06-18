package services.rabbit;

import com.rabbitmq.client.AMQP;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;

import java.util.UUID;

public class ExactlyOncePublishOptions<T> implements RMQSinkPublishOptions<T> {

    private final String queueName;

    public ExactlyOncePublishOptions(@NonNull String queueName) {
        this.queueName = queueName;
    }

    @Override
    public String computeRoutingKey(T a) {
        return queueName;
    }

    @Override
    public AMQP.BasicProperties computeProperties(T a) {
        return new AMQP.BasicProperties().builder().correlationId(UUID.randomUUID().toString()).build();
    }

    @Override
    public String computeExchange(T a) {
        return StringUtils.EMPTY;
    }
}
