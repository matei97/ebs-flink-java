package services.serializer;

import com.ebs.project.proto.PublicationMessage;
import com.google.protobuf.Timestamp;
import generator.models.BiTouple;
import generator.models.Company;
import generator.models.Operator;
import generator.models.Publication;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import services.logger.EBSLogger;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Map;

@AllArgsConstructor
public final class PublicationMessageSerializer implements SerializationSchema<Publication> {

    private final int port;

    @SneakyThrows
    @Override
    public byte[] serialize(Publication publication) {
        Map<Company.Key, BiTouple<Operator, Object>> biToupleMap = publication.getMap();
        String companyName = biToupleMap.containsKey(Company.Key.NAME) ? (String) biToupleMap.get(Company.Key.NAME).getValue() : StringUtils.EMPTY;
        Double drop = biToupleMap.containsKey(Company.Key.DROP) ? (Double) biToupleMap.get(Company.Key.DROP).getValue() : 0.0;
        Double value = biToupleMap.containsKey(Company.Key.VALUE) ? (Double) biToupleMap.get(Company.Key.VALUE).getValue() : 0.0;
        Double variation = biToupleMap.containsKey(Company.Key.VARIATION) ? (Double) biToupleMap.get(Company.Key.VARIATION).getValue() : 0.0;
        LocalDate date = biToupleMap.containsKey(Company.Key.DATE) ? (LocalDate) biToupleMap.get(Company.Key.DATE).getValue() : LocalDate.MIN;
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(date.toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.MIN)).build();

        PublicationMessage.PublicationDTO publicationDTO = PublicationMessage.PublicationDTO.newBuilder()
                .setCompanyName(companyName)
                .setDrop(drop)
                .setValue(value)
                .setVariation(variation)
                .setDate(timestamp)
                .setPort(port)
                .build();

        //EBSLogger.Publisher.log(port, publicationDTO);
        return publicationDTO.toByteArray();
    }
}