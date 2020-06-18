package generator.generators;

import generator.models.Subscription;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
public class SubscriptionGenerator extends Generator<Subscription> {

    public SubscriptionGenerator() {
        this(Generator.DEFAULT_TOTAL_MESSAGES);
    }

    public SubscriptionGenerator(int totalMessages) {
        super();
        this.totalMessages = totalMessages < 1 ? DEFAULT_TOTAL_MESSAGES : totalMessages;
    }

    public List<Subscription> generate() {
        PublicationGenerator publicationGenerator = new PublicationGenerator(this.totalMessages);
        publicationGenerator.setFrequencyPercentages(this.frequencyPercentages);
        publicationGenerator.setLimits(this.limits);

        return publicationGenerator.generatePublications()
                .stream()
                .map(Subscription::new)
                .collect(Collectors.toList());
    }
}
