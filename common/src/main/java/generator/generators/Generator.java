package generator.generators;

import generator.models.*;
import generator.utils.DateTimeUtil;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
public abstract class Generator<T> {

    public static int DEFAULT_TOTAL_MESSAGES = 1000;

    public int totalMessages;

    public Map<Frequency, Double> frequencyPercentages = new HashMap<>();

    public Map<BiTouple<Company.Key, LimitType>, Object> limits = new HashMap<>();

    public void addFrequencyPercentage(Company.Key key, Operator operator, Object value, Double percent) {
        if (percent == null || percent > 100) {
            throw new RuntimeException("Invalid percent.");
        }
        frequencyPercentages.put(new Frequency(key, operator, value), isPercentValid(percent) ? percent : 0);
    }

    public void setFrequencyPercentages(Map<Frequency, Double> frequencyPercentages) {
        frequencyPercentages.forEach((key, value) -> addFrequencyPercentage(key.getKey(), key.getOperator(), key.getValue(), value));
    }

    public void setLimits(Map<BiTouple<Company.Key, LimitType>, Object> limits) {
        limits.forEach((key, value) -> addLimit(key.getKey(), key.getValue(), value));
    }

    public void addLimit(Company.Key key, LimitType limitType, Object value) {
        List<Company.Key> applicableLimitKeys = Arrays.asList(Company.Key.DROP, Company.Key.VALUE, Company.Key.VARIATION, Company.Key.DATE);
        if (!applicableLimitKeys.contains(key)) {
            throw new RuntimeException("Limitation N/A on this type of key.");
        }
        BiTouple<Company.Key, LimitType> limit = new BiTouple<>(key, limitType);
        if (limits.containsKey(limit)) {
            throw new RuntimeException("Pair <Key, LimitType> already exist.");
        }
        if (key.getType() == LocalDate.class && !(value instanceof LocalDate)) {
            value = DateTimeUtil.toLocalDate((String) value);
        }
        limits.put(limit, value);
    }

    public boolean isPercentValid(Double percent) {
        return percent >= 0 && percent <= 100;
    }
}
