package generator.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.time.LocalDate;

@AllArgsConstructor
@Getter
@Setter
public class Company implements Serializable {

    private String name;

    private Double value;

    private Double drop;

    private Double variation;

    private LocalDate date;

    @AllArgsConstructor
    @Getter
    public enum Key {
        NAME(String.class, 1),
        VALUE(Double.class, 2),
        DROP(Double.class, 3),
        VARIATION(Double.class, 4),
        DATE(LocalDate.class, 5);

        public final Class<?> type;

        public final int order;
    }
}
