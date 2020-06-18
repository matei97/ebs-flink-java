package services.deserializer;


import com.ebs.project.proto.PublicationMessage;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class PublicationDTODeserializer extends AbstractDeserializationSchema<PublicationMessage.PublicationDTO> {
    @Override
    public PublicationMessage.PublicationDTO deserialize(byte[] message) throws IOException {
        return PublicationMessage.PublicationDTO.parseFrom(message);
    }
}
