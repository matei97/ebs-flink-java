package services;

import generator.generators.Generator;
import generator.generators.PublicationGenerator;
import generator.generators.SubscriptionGenerator;
import generator.models.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class GeneratorService {

    public static List<Publication> getPublications(int howMany) {
        PublicationGenerator generator = new PublicationGenerator(howMany);
        populateGenerator(generator);
        return generator.generate();
    }

    public static List<Subscription> getSubscriptions(int howMany) {
        SubscriptionGenerator generator = new SubscriptionGenerator(howMany);
        populateGenerator(generator);
        return generator.generate();
    }

    private static void populateGenerator(Generator generator) {
        generator.addFrequencyPercentage(Company.Key.NAME, Operator.EQUAL, "Google", 50.0);
        generator.addFrequencyPercentage(Company.Key.NAME, Operator.NOT_EQUAL, "Facebook", 60.0);

        generator.addFrequencyPercentage(Company.Key.DROP, Operator.LOWER, 100.0, 30.0);
        generator.addFrequencyPercentage(Company.Key.DROP, Operator.BIGGER_OR_EQUAL, 80.0, 80.0);

        generator.addFrequencyPercentage(Company.Key.VALUE, Operator.LOWER, 68.0, 50.0);
        generator.addFrequencyPercentage(Company.Key.VALUE, Operator.BIGGER_OR_EQUAL, 18.0, 60.0);

        generator.addFrequencyPercentage(Company.Key.VARIATION, Operator.LOWER_OR_EQUAL, 70.0, 89.0);
        generator.addFrequencyPercentage(Company.Key.VARIATION, Operator.BIGGER_OR_EQUAL, 30.0, 40.0);

        generator.addFrequencyPercentage(Company.Key.DATE, Operator.LOWER, LocalDate.of(2019, 2, 14), 78.0);
        generator.addFrequencyPercentage(Company.Key.DATE, Operator.BIGGER, LocalDate.of(2015, 5, 23), 34.0);

        generator.addLimit(Company.Key.DROP, LimitType.INFERIOR, 10.0);
        generator.addLimit(Company.Key.DROP, LimitType.SUPERIOR, 100.0);
        generator.addLimit(Company.Key.VALUE, LimitType.INFERIOR, 10.0);
        generator.addLimit(Company.Key.VALUE, LimitType.SUPERIOR, 95.0);
        generator.addLimit(Company.Key.VARIATION, LimitType.INFERIOR, 20.0);
        generator.addLimit(Company.Key.VARIATION, LimitType.SUPERIOR, 80.0);
        generator.addLimit(Company.Key.DATE, LimitType.INFERIOR, LocalDate.of(2010, 1, 2));
        generator.addLimit(Company.Key.DATE, LimitType.SUPERIOR, LocalDate.of(2020, 1, 1));
    }

}
