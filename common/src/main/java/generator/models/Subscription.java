package generator.models;

import lombok.Value;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Value
public class Subscription implements Serializable, EventElement {

    List<Frequency> fields;

    public Subscription() {
        fields = new ArrayList<>();
    }

    public Subscription(Publication publication) {
        this();
        publication.getMap().entrySet()
                .forEach(keyBiToupleEntry -> fields.add(new Frequency(keyBiToupleEntry)));
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("{");
        fields.stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().getOrder()))
                .forEach((entry) -> stringBuilder.append("(").append(entry.getKey()).append(", ").append(entry.getOperator().getOperator()).append(
                        ", ").append(entry.getValue()).append("); "));
        return stringBuilder.append("}\n").toString();
    }
}
