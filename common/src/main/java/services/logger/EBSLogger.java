package services.logger;

import com.ebs.project.proto.PublicationMessage;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.Logger;
import services.PublishToNodeConnectionConfig;

import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;

public class EBSLogger {

    private static class MessageLog {

        private static final Logger logger = LogManager.getLogger();

        private static String hash(PublicationMessage.PublicationDTO value) {
            PublicationMessage.PublicationDTO hashablePub = value.toBuilder()
                    .setPort(0)
                    .build();
            return DigestUtils.md5Hex(hashablePub.toByteArray());
        }

        public static void logPublication(String module, int port, PublicationMessage.PublicationDTO publicationDTO,
                                          List<PublishToNodeConnectionConfig> outputsThatMatchPublication) {
            StringBuilder message = new StringBuilder();
            message.append("[").append(module).append("]").append("-");
            message.append("[").append(port).append("]").append("-");
            message.append("[").append(System.currentTimeMillis()).append("]").append("-");
            message.append("[").append(publicationDTO.getPort()).append("]").append("-");
            message.append("[").append(hash(publicationDTO)).append("]");
            if (outputsThatMatchPublication.size() > 0) {
                message.append("-[");
                outputsThatMatchPublication.forEach(node -> message.append("{").append(node.getQueueName()).append(":").append(node.getPort()).append("}").append("-"));
                message.append("]");
            }
            //System.out.println(message.toString());
        }
    }

    public static class Broker extends MessageLog {

        public static void log(int port, PublicationMessage.PublicationDTO value,
                               List<PublishToNodeConnectionConfig> outputsThatMatchPublication) {
            logPublication("Broker", port, value, outputsThatMatchPublication);
        }
    }

    public static class Subscriber extends MessageLog {

        public static void log(int port, PublicationMessage.PublicationDTO value) {
            logPublication("Subscriber", port, value, Collections.emptyList());
        }
    }

    public static class Publisher extends MessageLog {

        public static void log(int port, PublicationMessage.PublicationDTO value) {
            logPublication("Publisher", port, value, Collections.emptyList());
        }
    }

}
