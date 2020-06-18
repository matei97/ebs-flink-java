package services.deserializer;


import com.ebs.project.proto.SubscriptionMessage;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class SubscriptionDTODeserializer extends AbstractDeserializationSchema<SubscriptionMessage.SubscriptionDTO> {
    @Override
    public SubscriptionMessage.SubscriptionDTO deserialize(byte[] message) throws IOException {
        return SubscriptionMessage.SubscriptionDTO.parseFrom(message);
    }
}
