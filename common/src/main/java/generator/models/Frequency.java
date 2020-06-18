package generator.models;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Getter
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class Frequency extends BiTouple<Company.Key, Object> implements Serializable {

    private Operator operator;

    public Frequency(Company.Key key, Operator operator, Object value) {
        super(key, value);
        this.operator = operator;
    }

    public Frequency(Map.Entry<Company.Key, BiTouple<Operator, Object>> keyBiToupleEntry) {
        this(keyBiToupleEntry.getKey(), keyBiToupleEntry.getValue().getKey(), keyBiToupleEntry.getValue().getValue());
    }
}
