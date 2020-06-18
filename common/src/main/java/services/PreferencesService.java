package services;

import lombok.Getter;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

@Getter
public class PreferencesService {

    private static PreferencesService instance = null;

    private final Map<ConfigKey, String> preferences;

    private PreferencesService() {
        //System.out.println(System.getProperty("os.name"));
        this.preferences = new HashMap<>();
        try {
            InputStream inputStream;
            Properties prop = new Properties();
            String propFileName = "config.properties";
            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("Property file '" + propFileName + "' not found in the classpath");
            }
            Arrays.asList(ConfigKey.values()).forEach(configKey -> preferences.put(configKey, prop.getProperty(configKey.name())));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static PreferencesService getInstance() {
        if (instance == null) {
            instance = new PreferencesService();
        }
        return instance;
    }

    @Getter
    public enum ConfigKey {
        HOST_OS_NAME,
        HOST_IP,
        HOST_PORT_1,
        HOST_PORT_2,
        HOST_PORT_3,
        VIRTUAL_HOST,
        RABBIT_USER,
        RABBIT_PASS,
        RABBIT_QUEUE_SUBSCRIPTIONS,
        RABBIT_QUEUE_PUBLICATIONS,
        SUBSCRIPTION_COUNT,
        PUBLICATION_COUNT;

        public static ConfigKey getPort(int paramValue) {
            switch (paramValue) {
                case 1: {
                    return HOST_PORT_1;
                }
                case 2: {
                    return HOST_PORT_2;
                }
                case 3: {
                    return HOST_PORT_3;
                }
                default: {
                    throw new UnsupportedOperationException("Invalid option argument");
                }
            }
        }

        public static ConfigKey getRandomPort() {
            Random rand = new Random();
            return getPort(rand.nextInt(3) + 1);
        }
    }
}
