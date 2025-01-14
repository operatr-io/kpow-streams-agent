package io.factorhouse.kpow.key;

/**
 * A {@link KeyStrategy} implementation that derives the key for metrics data using the client ID
 * and application ID of the active Kafka Streams instance.
 *
 * <p>This strategy simplifies the configuration process by relying solely on the client ID
 * and application ID, eliminating the need for a Kafka {@link org.apache.kafka.clients.admin.AdminClient}.
 * It is particularly suitable for constrained environments where creating an {@code AdminClient}
 * may not be feasible or appropriate due to security concerns.</p>
 *
 * <p><b>Important:</b> In a multi-cluster Kpow deployment, where the same application ID and client ID
 * are used across multiple environments (e.g., staging, development, production), this strategy
 * cannot determine which cluster the Kafka Streams instance is associated with. This limitation may
 * impact the accuracy of metrics alignment in the Kpow UI for such setups.</p>
 *
 * <p>This strategy is most effective in single-cluster deployments, lightweight environments, or
 * scenarios where client IDs and application IDs are guaranteed to be unique across clusters.</p>
 */
public class ClientIdKeyStrategy implements KeyStrategy {

    /**
     * Constructs a new instance of the {@link ClientIdKeyStrategy}.
     */
    public ClientIdKeyStrategy() {}

    @Override
    public Taxon getTaxon(String clientId, String applicationId) {
        return new Taxon("streams", clientId, "streams-agent", null);
    }
}
