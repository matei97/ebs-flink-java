package generator.models;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Arrays;

@AllArgsConstructor
@Getter
public enum Operator implements Serializable {
    EQUAL("=", 2),
    NOT_EQUAL("!=", 2),
    BIGGER(">", 1),
    LOWER("<", 1),
    BIGGER_OR_EQUAL(">=", 0),
    LOWER_OR_EQUAL("<=", 0);

    private String operator;

    private int power;

    public static Operator getOperator(String operator) {
        return Arrays.stream(Operator.values())
                .filter(value -> value.getOperator().equals(operator))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Operator <" + operator + "> does not exist."));
    }
}
