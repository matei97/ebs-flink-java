package generator.generators;

import generator.data.Companies;
import generator.models.*;
import generator.utils.RandomUtil;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Getter
public class PublicationGenerator extends Generator<Publication> {

    private int totalMessages;

    public PublicationGenerator() {
        this(DEFAULT_TOTAL_MESSAGES);
    }

    public PublicationGenerator(int totalMessages) {
        this.totalMessages = totalMessages < 1 ? DEFAULT_TOTAL_MESSAGES : totalMessages;
    }

    private void validateIfSumOfPercentagesIsValidFor(Frequency frequency) {
        double sumOfFrequency = frequencyPercentages.entrySet().stream()
                .filter(entry -> getValidOperators(entry.getKey().getKey()).contains(entry.getKey().getOperator()))
                .filter(entry -> entry.getKey().getKey() == frequency.getKey())
                .mapToDouble(Entry::getValue)
                .sum();
        if (sumOfFrequency < 100) {
            throw new RuntimeException("Publication frequencies <" + frequency.getKey().toString() + "> percentages are not 100%.");
        }
    }

    public List<Publication> generate() {
        frequencyPercentages.keySet().forEach(this::validateIfSumOfPercentagesIsValidFor);

        return Stream.of(generatePublications())
                .map(list -> fillTheGaps(list, Company.Key.NAME))
                .map(list -> fillTheGaps(list, Company.Key.VALUE))
                .map(list -> fillTheGaps(list, Company.Key.DROP))
                .map(list -> fillTheGaps(list, Company.Key.VARIATION))
                .map(list -> fillTheGaps(list, Company.Key.DATE))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<Publication> generatePublications() {
        List<Publication> rawPublication = IntStream.range(0, this.totalMessages)
                .mapToObj(index -> new Publication())
                .collect(Collectors.toList());

        return Stream.of(rawPublication)
                .map(list -> applyStringRules(list, Company.Key.NAME))
                .map(list -> applyDoubleAndDateRules(list, Company.Key.VALUE))
                .map(list -> applyDoubleAndDateRules(list, Company.Key.DROP))
                .map(list -> applyDoubleAndDateRules(list, Company.Key.VARIATION))
                .map(list -> applyDoubleAndDateRules(list, Company.Key.DATE))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Map<Frequency, Double> getRulesFor(Company.Key key) {
        List<Operator> validOperators = getValidOperators(key);
        return frequencyPercentages.keySet().stream()
                .filter(frequency -> frequency.getKey() == key)
                .filter(frequency -> validOperators.contains(frequency.getOperator()))
                .collect(Collectors.toMap(Function.identity(), frequencyPercentages::get));
    }

    private List<Operator> getValidOperators(Company.Key key) {
        List<Operator> validOperators = new ArrayList<>();
        if (key.getType() != String.class) {
            validOperators.addAll(Arrays.asList(Operator.BIGGER_OR_EQUAL, Operator.LOWER_OR_EQUAL, Operator.BIGGER, Operator.LOWER));
        } else {
            validOperators.addAll(Arrays.asList(Operator.EQUAL, Operator.NOT_EQUAL));
        }
        return validOperators;
    }

    private List<Publication> applyStringRules(List<Publication> list, Company.Key key) {
        if (key.getType() != String.class) {
            throw new RuntimeException("Unsupported key type.");
        }
        List<Entry<Frequency, Double>> ruleArray = new ArrayList<>(getRulesFor(key).entrySet());
        ruleArray.forEach(rule -> applyStringRule(list, rule));
        return list;
    }

    private void applyStringRule(List<Publication> list, Entry<Frequency, Double> rule) {
        AtomicInteger actualNumberOfElements = new AtomicInteger((int) (totalMessages * rule.getValue()) / 100);
        Collections.shuffle(list);
        IntStream.range(0, actualNumberOfElements.get())
                .forEach(it -> actualNumberOfElements.set(getActualNumberOfElements(list.get(it), actualNumberOfElements.get(), rule.getKey())));
    }

    private int getActualNumberOfElements(Publication publication, int actualNumberOfElements, Frequency frequency) {
        String triToupleValue = (String) frequency.getValue();
        String insertValue = frequency.getOperator() == Operator.EQUAL ? triToupleValue : Companies.getRandomCompanyNameButNot(triToupleValue);
        boolean result = publication.addValue(frequency.getKey(), frequency.getOperator(), insertValue);
        return !result && actualNumberOfElements + 1 <= totalMessages ? actualNumberOfElements + 1 : actualNumberOfElements;
    }

    private Object getLimit(Company.Key key, LimitType limitType) {
        return key.getType() == String.class ? StringUtils.EMPTY : limits.entrySet().stream()
                .filter(entry -> entry.getKey().getKey() == key)
                .filter(entry -> entry.getKey().getValue() == limitType)
                .min(getLimitComparatorByKey(key))
                .map(getLimitFunctionByKey(key))
                .orElseThrow(() -> new RuntimeException("Missing " + limitType + " limit for " + key));
    }

    private Comparator<Entry<BiTouple<Company.Key, LimitType>, Object>> getLimitComparatorByKey(Company.Key key) {
        return key.getType() == LocalDate.class ? Comparator.comparing(e -> ((LocalDate) e.getValue())) :
                Comparator.comparing(e -> ((Double) e.getValue()));
    }

    private Function<Entry<BiTouple<Company.Key, LimitType>, Object>, Object> getLimitFunctionByKey(Company.Key key) {
        return key.getType() == LocalDate.class ? entry -> (LocalDate) entry.getValue() : entry -> (Double) entry.getValue();
    }

    private List<Publication> applyDoubleAndDateRules(List<Publication> publications, Company.Key key) {
        if (key.getType() != LocalDate.class && key.getType() != Double.class) {
            throw new RuntimeException("Unsupported key type.");
        }
        Object inferiorLimit = getLimit(key, LimitType.INFERIOR);
        Object superiorLimit = getLimit(key, LimitType.SUPERIOR);
        AtomicReference<Double> remainingPercent = new AtomicReference<>(0.0);

        List<Entry<Frequency, Double>> ruleArray = new ArrayList<>(getRulesFor(key).entrySet());
        ruleArray.sort(Entry.comparingByValue());
        ruleArray.forEach(rule -> remainingPercent.set(getRemainingPercent(key, publications, ruleArray, inferiorLimit, superiorLimit,
                remainingPercent.get(), rule)));
        return publications;
    }

    private Double getRemainingPercent(Company.Key key, List<Publication> publications, List<Entry<Frequency, Double>> ruleArray,
                                       Object inferiorLimit,
                                       Object superiorLimit, Double remainingPercent, Entry<Frequency, Double> currentRule) {
        final int[] elementNumberArray = {(int) (totalMessages * (currentRule.getValue() - remainingPercent)) / 100};

        Frequency maxFrequency = getFrequency(ruleArray, inferiorLimit, Operator.LOWER, Operator.LOWER_OR_EQUAL);
        Frequency minFrequency = getFrequency(ruleArray, superiorLimit, Operator.BIGGER, Operator.BIGGER_OR_EQUAL);

        Object maxValue = getMaxValue(maxFrequency, key);
        Object minValue = getMinValue(minFrequency, key);

        Collections.shuffle(publications);

        for (int i = 0; i < elementNumberArray[0]; i++) {
            Object insertValue = RandomUtil.between(minValue, maxValue);
            elementNumberArray[0] = getUpdatedNumberOfElements(publications.get(i), currentRule.getKey(), elementNumberArray[0], insertValue);
        }
        return currentRule.getValue();
    }

    private List<Publication> fillTheGaps(List<Publication> publications, Company.Key key) {
        Object inferiorLimit = getLimit(key, LimitType.INFERIOR);
        Object superiorLimit = getLimit(key, LimitType.SUPERIOR);
        publications.forEach(publication -> publication.addValue(key, Operator.EQUAL, RandomUtil.between(inferiorLimit, superiorLimit)));
        return publications;
    }

    private Object getMaxValue(Frequency maxFrequency, Company.Key key) {
        Object returnVal = maxFrequency.getValue();
        return maxFrequency.getOperator() != Operator.LOWER ? returnVal :
                (key.getType() == LocalDate.class ? ((LocalDate) returnVal).minusDays(1) : (Double) returnVal - 1);
    }

    private Object getMinValue(Frequency maxFrequency, Company.Key key) {
        Object returnVal = maxFrequency.getValue();
        return maxFrequency.getOperator() != Operator.BIGGER ? returnVal :
                (key.getType() == LocalDate.class ? ((LocalDate) returnVal).plusDays(1) : (Double) returnVal + 1);
    }

    private int getUpdatedNumberOfElements(Publication publication, Frequency rule, int actualNumberOfElements, Object insertValue) {
        boolean increaseElementList =
                !publication.addValue(rule.getKey(), rule.getOperator(), insertValue) && actualNumberOfElements + 1 <= totalMessages;
        return increaseElementList ? actualNumberOfElements + 1 : actualNumberOfElements;
    }

    private Frequency getFrequency(List<Entry<Frequency, Double>> ruleArray, Object inferiorLimit, Operator actual, Operator actualOrEqual) {
        return ruleArray.stream()
                .filter(entry -> entry.getKey().getOperator() == actual || entry.getKey().getOperator() == actualOrEqual)
                .min(Comparator.comparingDouble(entry -> (Double) entry.getKey().getValue()))
                .map(Entry::getKey)
                .orElse(new Frequency(null, actualOrEqual, inferiorLimit));
    }
}
