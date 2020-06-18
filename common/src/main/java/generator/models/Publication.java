package generator.models;

import lombok.Getter;
import lombok.Setter;
import lombok.Value;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

@Value
@Getter
@Setter
public class Publication implements Serializable, EventElement {

    private Map<Company.Key, BiTouple<Operator, Object>> map;

    public Publication() {
        map = new LinkedHashMap<>();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("{");
        map.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().getOrder()))
                .forEach((entry) -> stringBuilder.append("(").append(entry.getKey()).append(", ").append(entry.getValue().getValue()).append("); "));
        return stringBuilder.append("}\n").toString();
    }

    public boolean addValue(Company.Key key, Operator operator, Object value) {
        if (!key.getType().isInstance(value) || map.containsKey(key)) {
            return false;
        }
        map.put(key, new BiTouple<>(operator, value));
        return true;
    }
}
