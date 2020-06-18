package services;


import lombok.*;

import java.io.Serializable;

@Getter
@Builder
@EqualsAndHashCode
@ToString
public final class PublishToNodeConnectionConfig implements Serializable {
    @NonNull private final String host;
    @NonNull private final String virtualHost;
    @NonNull private final String username;
    @NonNull private final String password;
    @NonNull private final String queueName;
    @NonNull private final int port;

}
