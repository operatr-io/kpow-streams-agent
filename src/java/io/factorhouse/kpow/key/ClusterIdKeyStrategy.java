package io.factorhouse.kpow.key;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;

/**
 * A {@link KeyStrategy} implementation that uses the Kafka cluster ID as the primary identifier
 * for keying metrics data in Kpow's internal Kafka topic.
 * <p>This key strategy uses the cluster ID, obtained via an {@link org.apache.kafka.clients.admin.Admin#describeCluster()} call.</p>
 * <p>This AdminClient is created once during registry initialization and then closed.</p>
 * <p>This is the default and recommended keying strategy for Kpow, as it provides a robust way
 * to uniquely associate metrics data with a specific Kafka cluster.</p>
 */
public class ClusterIdKeyStrategy implements KeyStrategy {

    private final String clusterId;

    /**
     * Creates an instance of {@code ClusterIdKeyStrategy} using properties for AdminClient configuration.
     *
     * @param props Kafka connection properties used for the AdminClient.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     * @throws ExecutionException   if an error occurred during cluster description due to a connection issue with the Kafka brokers.
     */
    public ClusterIdKeyStrategy(Properties props)
        throws InterruptedException, ExecutionException {
        try (AdminClient adminClient = AdminClient.create(props)) {
            DescribeClusterResult describeClusterResult =
                adminClient.describeCluster();
            this.clusterId = describeClusterResult.clusterId().get();
        }
    }

    @Override
    public Taxon getTaxon(String clientId, String applicationId) {
        return new Taxon("cluster", clusterId, "streams-agent", clientId);
    }
}
