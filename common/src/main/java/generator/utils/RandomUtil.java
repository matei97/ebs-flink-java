package generator.utils;


import generator.data.Companies;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomUtil {

    private static final Random random = new Random();

    public static Long between(long min, long max) {
        return ThreadLocalRandom.current().nextLong(min, max);
    }

    public static LocalDate between(LocalDate min, LocalDate max) {
        return LocalDate.ofEpochDay(between(min.toEpochDay(), max.toEpochDay()));
    }

    public static Double between(double min, double max) {
        return new BigDecimal(Double.toString(min + (max - min) * random.nextDouble()))
                .setScale(2, RoundingMode.HALF_UP)
                .doubleValue();
    }

    public static List<Integer> listBetween(int limit, int min, int max) {
        if (limit > max - min) {
            throw new RuntimeException("You are stupid exception!");
        }
        List<Integer> randomInts = IntStream.range(min, max).boxed().collect(Collectors.toList());
        Collections.shuffle(randomInts);
        return randomInts.stream().limit(limit).collect(Collectors.toList());
    }

    public static Object between(Object minValue, Object maxValue) {
        if (minValue instanceof LocalDate && maxValue instanceof LocalDate) {
            return between((LocalDate) minValue, (LocalDate) maxValue);
        }
        if (minValue instanceof Double && maxValue instanceof Double) {
            return between(((Double) minValue).doubleValue(), ((Double) maxValue).doubleValue());
        }
        if (minValue instanceof String && maxValue instanceof String) {
            return Companies.getRandomCompanyName();
        }
        throw new RuntimeException("Unsupported object type.");
    }
}
