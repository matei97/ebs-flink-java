package services.serializer;

import com.ebs.project.proto.SubscriptionMessage;
import com.google.protobuf.Timestamp;
import generator.models.Frequency;
import generator.models.Subscription;
import lombok.NonNull;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.serialization.SerializationSchema;
import services.PublishToNodeConnectionConfig;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

public final class SubscriptionMessageSerializer implements SerializationSchema<Subscription> {

    private final PublishToNodeConnectionConfig subscriptionSourceConfig;

    public SubscriptionMessageSerializer(@NonNull PublishToNodeConnectionConfig subscriptionSourceConfig) {
        this.subscriptionSourceConfig = subscriptionSourceConfig;
    }

    private static SubscriptionMessage.SubscriptionDTO.ValueDoubleRestriction getValueDoubleRestrictionBy(Frequency frequency) {
        return SubscriptionMessage.SubscriptionDTO.ValueDoubleRestriction.newBuilder()
                .setOperation(frequency.getOperator().getOperator())
                .setValue((Double) frequency.getValue())
                .build();
    }

    private static void addProperty(SubscriptionMessage.SubscriptionDTO.Builder builder, Frequency frequency) {
        switch (frequency.getKey()) {
            case NAME: {
                builder.setCompanyName(
                        SubscriptionMessage.SubscriptionDTO.ValueStringRestriction.newBuilder()
                                .setOperation(frequency.getOperator().getOperator())
                                .setValue((String) frequency.getValue())
                                .build()
                );
                break;
            }
            case DROP: {
                builder.setDrop(getValueDoubleRestrictionBy(frequency));
                break;
            }
            case VALUE: {
                builder.setValue(getValueDoubleRestrictionBy(frequency));
                break;
            }
            case VARIATION: {
                builder.setVariation(getValueDoubleRestrictionBy(frequency));
                break;
            }
            case DATE: {
                LocalDate date = (LocalDate) frequency.getValue();
                Timestamp timestamp = Timestamp.newBuilder()
                        .setSeconds(date.toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN))
                        .build();
                builder.setDate(
                        SubscriptionMessage.SubscriptionDTO.ValueDateRestriction.newBuilder()
                                .setOperation(frequency.getOperator().getOperator())
                                .setValue(timestamp)
                                .build()
                );
                break;
            }
            default: throw new NotImplementedException("Not a valid Key");
        }
    }

    @Override
    public byte[] serialize(Subscription subscription) {
        SubscriptionMessage.SubscriptionDTO.Builder subscriptionBuilder = SubscriptionMessage.SubscriptionDTO.newBuilder();
        subscription.getFields().forEach(frequency -> addProperty(subscriptionBuilder, frequency));
        subscriptionBuilder.setHost(subscriptionSourceConfig.getHost());
        subscriptionBuilder.setVirtualHost(subscriptionSourceConfig.getVirtualHost());
        subscriptionBuilder.setUsername(subscriptionSourceConfig.getUsername());
        subscriptionBuilder.setPassword(subscriptionSourceConfig.getPassword());
        subscriptionBuilder.setPort(subscriptionSourceConfig.getPort());
        subscriptionBuilder.setQueueName(subscriptionSourceConfig.getQueueName());
        subscriptionBuilder.setOriginalPort(subscriptionSourceConfig.getPort());
        subscriptionBuilder.setOriginalQueueName(subscriptionSourceConfig.getQueueName());
        var subscriptionDTO = subscriptionBuilder.build();
        return subscriptionDTO.toByteArray();
    }
}