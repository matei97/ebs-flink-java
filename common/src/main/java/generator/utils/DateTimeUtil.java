package generator.utils;

import lombok.experimental.UtilityClass;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@UtilityClass
public class DateTimeUtil {

    private static final String EBS_DATE_FORMAT = "yyyy-MM-dd";

    private static final DateTimeFormatter EBS_DATE_FORMATTER = DateTimeFormatter.ofPattern(EBS_DATE_FORMAT);

    public String toString(LocalDate localDate) {
        return localDate.format(EBS_DATE_FORMATTER);
    }

    public LocalDate toLocalDate(String localDate) {
        return LocalDate.parse(localDate, EBS_DATE_FORMATTER);
    }
}
