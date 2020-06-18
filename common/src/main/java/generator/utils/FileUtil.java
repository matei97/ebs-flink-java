package generator.utils;

import generator.generators.Generator;
import generator.generators.PublicationGenerator;
import generator.generators.SubscriptionGenerator;
import generator.models.*;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FileUtil {

    public static void saveAndSerialize(List<Publication> publications, List<Subscription> subscriptions, boolean serialize) {
        try {
            FileUtil.saveToFile(publications);
            FileUtil.saveToFile(subscriptions);

            if (serialize) {
                String publicationsFilename = serialize(publications);
                List<Publication> publicationsDeserialize = Objects.requireNonNull(deserialize(publicationsFilename)).stream()
                        .filter(entry -> entry instanceof Publication)
                        .map(entry -> (Publication) entry)
                        .collect(Collectors.toList());
                //publicationsDeserialize.forEach(System.out::println);

                String subscriptionsFilename = serialize(subscriptions);
                List<Subscription> subscriptionsDeserialize = Objects.requireNonNull(deserialize(subscriptionsFilename)).stream()
                        .filter(entry -> entry instanceof Subscription)
                        .map(entry -> (Subscription) entry)
                        .collect(Collectors.toList());
                //subscriptionsDeserialize.forEach(System.out::println);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Generator<? extends EventElement> constructGenerator(String type) {
        Generator<? extends EventElement> generator = null;
        JSONParser parser = new JSONParser();
        try {
            Object fileRead = parser.parse(new FileReader("input.json"));
            JSONObject jsonObject = (JSONObject) fileRead;
            int totalMessages = Integer.parseInt(jsonObject.get("messages").toString());
            switch (type) {
                case "publication": {
                    generator = new PublicationGenerator(totalMessages);
                    break;
                }
                case "subscription": {
                    generator = new SubscriptionGenerator(totalMessages);
                    break;
                }
                default:
                    throw new RuntimeException("Type <" + type + "> does not exist.");
            }
            JSONObject typeObject = (JSONObject) jsonObject.get(type);
            JSONArray typeFrequencies = (JSONArray) typeObject.get("frequencies");
            for (Object element : typeFrequencies) {
                JSONObject jsonElement = (JSONObject) element;
                Company.Key key = Company.Key.valueOf(jsonElement.get("key").toString().toUpperCase());
                JSONArray rules = (JSONArray) jsonElement.get("rules");
                for (Object rule : rules) {
                    JSONObject ruleAsObject = (JSONObject) rule;
                    Operator operator = Operator.getOperator(ruleAsObject.get("operator").toString().toUpperCase());
                    Object value = wrapValueByKey(key, ruleAsObject.get("value"));
                    Double percent = (Double) ruleAsObject.get("percent");
                    generator.addFrequencyPercentage(key, operator, value, percent);
                }
                JSONObject limits = (JSONObject) jsonElement.get("limits");
                if (limits != null && key.getType() != String.class) {
                    generator.addLimit(key, LimitType.INFERIOR, wrapValueByKey(key, limits.get("inferior")));
                    generator.addLimit(key, LimitType.SUPERIOR, wrapValueByKey(key, limits.get("superior")));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return generator;
    }

    public static void saveToFile(List<? extends EventElement> objectList) throws IOException {
        if (objectList.isEmpty()) {
            return;
        }
        String filename = getFilenameBySaveType(objectList.get(0), ".txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        for (EventElement element : objectList) {
            writer.write(element.toString());
        }
        writer.close();
    }

    private static Object wrapValueByKey(Company.Key key, Object value) {
        switch (key) {
            case NAME: {
                if (value instanceof String) {
                    return value;
                }
                return value.toString();
            }
            case DATE: {
                if (value instanceof LocalDate) {
                    return value;
                }
                if (value instanceof String) {
                    return DateTimeUtil.toLocalDate((String) value);
                }
                throw new RuntimeException("DATE value can be only String or LocalDate.");
            }
            default: {
                if (value instanceof Double) {
                    return value;
                }
                throw new RuntimeException(key.toString() + " value can be only Double.");
            }
        }
    }

    public static String serialize(List<? extends EventElement> objectList) throws IOException {
        if (objectList.isEmpty()) {
            return StringUtils.EMPTY;
        }
        String filename = getFilenameBySaveType(objectList.get(0), ".ser");
        FileOutputStream fileOutputStream = new FileOutputStream(filename);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        objectOutputStream.writeObject(objectList);
        objectOutputStream.close();
        fileOutputStream.close();
        return filename;
    }

    public static List<EventElement> deserialize(String filename) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(filename);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        Object object = objectInputStream.readObject();
        if (!(object instanceof List)) {
            return null;
        }
        List<EventElement> eventElements = (List<EventElement>) object;
        objectInputStream.close();
        fileInputStream.close();
        return eventElements;
    }

    private static String getFilenameBySaveType(EventElement event, String fileType) {
        return event.getClass().getName() + "s-" + System.currentTimeMillis() + fileType;
    }

}
