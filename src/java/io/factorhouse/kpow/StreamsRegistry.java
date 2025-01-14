package io.factorhouse.kpow;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import io.factorhouse.kpow.key.KeyStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.ArrayList;
import java.util.Properties;

/**
 * The {@code StreamsRegistry} class serves as a centralized registry for one or more Kafka Streams applications.
 * Consumers use this class to register their Kafka Streams applications, enabling integration with Kpow's advanced
 * Kafka Streams features.
 *
 *
 * <p>A single instance of {@code StreamsRegistry} can manage multiple Kafka Streams applications.
 * Each registered application is uniquely identified and can be individually managed (e.g., unregistered)
 * through the {@link StreamsAgent} returned upon registration.</p>
 *
 * <p>All registered Kafka Streams applications will be periodically observed, and their metrics will be
 * collected and sent to Kpow's internal Kafka topic. This enables real-time monitoring and insights
 * into the performance and health of your applications.</p>
 *
 * This class implements {@link AutoCloseable} which deregisters and stops telemetry for all registered Kafka Streams applications.
 */
public class StreamsRegistry implements AutoCloseable {

    /**
     * Represents an agent for a registered Kafka Streams application.
     * Provides the unique identifier of the registered application.
     */
    public static class StreamsAgent {
        private final String _id;

        /**
         * Constructs a {@code StreamsAgent} with the given identifier.
         *
         * @param id the unique identifier of the Kafka Streams application.
         */
        StreamsAgent(String id) {
            _id = id;
        }

        /**
         * Returns the unique identifier of the registered Kafka Streams application.
         *
         * @return the identifier as a {@link String}.
         */
        public String getId() {
            return _id;
        }
    }

    private final Object agent;

    /**
     * Filters a {@link Properties} object to retain only allowed Kafka properties.
     * Additional default values for compression and idempotence properties are set if not provided.
     *
     * @param props the input {@link Properties} containing Kafka configuration.
     * @return a filtered {@link Properties} object containing only the allowed properties.
     */
    public static Properties filterProperties(Properties props) {
        ArrayList<String> allowedKeys = new ArrayList<>();
        allowedKeys.add("ssl.enabled.protocols");
        allowedKeys.add("sasl.client.callback.handler.class");
        allowedKeys.add("ssl.endpoint.identification.algorithm");
        allowedKeys.add("ssl.provider");
        allowedKeys.add("ssl.truststore.location");
        allowedKeys.add("ssl.keystore.key");
        allowedKeys.add("ssl.key.password");
        allowedKeys.add("ssl.protocol");
        allowedKeys.add("ssl.keystore.password");
        allowedKeys.add("sasl.login.class");
        allowedKeys.add("ssl.trustmanager.algorithm");
        allowedKeys.add("ssl.keystore.location");
        allowedKeys.add("sasl.login.callback.handler.class");
        allowedKeys.add("ssl.truststore.certificates");
        allowedKeys.add("ssl.cipher.suites");
        allowedKeys.add("ssl.truststore.password");
        allowedKeys.add("ssl.keymanager.algorithm");
        allowedKeys.add("ssl.keystore.type");
        allowedKeys.add("ssl.secure.random.implementation");
        allowedKeys.add("ssl.truststore.type");
        allowedKeys.add("sasl.jaas.config");
        allowedKeys.add("ssl.keystore.certificate.chain");
        allowedKeys.add("sasl.mechanism");
        allowedKeys.add("sasl.oauthbearer.jwks.endpoint.url");
        allowedKeys.add("sasl.oauthbearer.token.endpoint.url");
        allowedKeys.add("sasl.kerberos.service.name");
        allowedKeys.add("security.protocol");
        allowedKeys.add("bootstrap.servers");

        Properties nextProps = new Properties();
        for (String key : allowedKeys) {
            if (props.containsKey(key)) {
                nextProps.setProperty(key, String.valueOf(props.get(key)));
            }
        }

        String compressionType = props.getProperty("compression.type", "gzip");
        nextProps.setProperty("compression.type", compressionType);

        String idempotence = props.getProperty("enable.idempotence", "false");
        nextProps.setProperty("enable.idempotence", idempotence);

        return nextProps;
    }

    /**
     * Constructs a {@code StreamsRegistry} instance using the specified Kafka properties and metrics filter.
     *
     * <p>This constructor initializes the registry, allowing Kafka Streams applications to be registered
     * for monitoring through Kpow's user interface and API. The provided {@link Properties} object is
     * used to configure the underlying Kafka producer, and the {@link MetricFilter} determines which metrics
     * are collected and sent to Kpow's internal Kafka topic.</p>
     *
     * <p><b>Important:</b> The Kafka producer properties provided in {@code props} must match the connection
     * details of Kpow's primary cluster, where Kpow's internal topic resides.</p>
     *
     * @param props         the {@link Properties} object containing Kafka configuration.
     *                      Must include essential properties like {@code bootstrap.servers}.
     * @param metricsFilter the {@link MetricFilter} to customize which metrics are reported.
     *                      Use {@link MetricFilter#defaultMetricFilter()} for default behavior.
     * @throws IllegalArgumentException if the provided {@code props} are invalid or incomplete.
     */
    public StreamsRegistry(Properties props, MetricFilter metricsFilter) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("io.factorhouse.kpow.agent"));
        IFn agentFn = Clojure.var("io.factorhouse.kpow.agent", "init-registry");
        require.invoke(Clojure.read("io.factorhouse.kpow.serdes"));
        IFn serdesFn = Clojure.var("io.factorhouse.kpow.serdes", "transit-json-serializer");
        Serializer keySerializer = (Serializer) serdesFn.invoke();
        Serializer valSerializer = (Serializer) serdesFn.invoke();
        Properties producerProps = filterProperties(props);
        KafkaProducer producer = new KafkaProducer<>(producerProps, keySerializer, valSerializer);
        agent = agentFn.invoke(producer, metricsFilter);
    }

    /**
     * Constructs a {@code StreamsRegistry} instance using the specified Kafka connection properties and the default
     * {@link MetricFilter}.
     *
     * <p>This constructor simplifies the initialization process by applying the default {@link MetricFilter},
     * which collects and reports all relevant metrics to the internal Kpow Kafka topic. The provided
     * {@link Properties} object is used to configure the underlying Kafka producer.</p>
     *
     * <p><b>Important:</b> The Kafka producer properties provided in {@code props} must match the connection
     * details of Kpow's primary cluster, where Kpow's internal topic resides.</p>
     *
     * @param props the {@link Properties} object containing Kafka configuration.
     *              Must include essential properties like {@code bootstrap.servers}.
     * @throws IllegalArgumentException if the provided {@code props} are invalid or incomplete.
     * @see MetricFilter#defaultMetricFilter()
     */
    public StreamsRegistry(Properties props) {
        this(props, MetricFilter.defaultMetricFilter());
    }

    /**
     * Registers a Kafka Streams application with the registry for monitoring.
     *
     * <p>Once registered, the Kafka Streams application will be periodically observed, and its metrics
     * will be collected and sent to an internal Kpow Kafka topic. These metrics enable real-time insights
     * into the application's performance, resource usage, and health from within Kpow's user interface and API.</p>
     *
     * <p>The registration process associates the application with a unique identifier encapsulated
     * in a {@link StreamsAgent}. This identifier can later be used to unregister the application
     * when it is no longer needed.</p>
     *
     * @param streams     the {@link KafkaStreams} instance representing the application to be registered.
     * @param topology    the {@link Topology} of the Kafka Streams application, which defines its processing logic.
     * @param keyStrategy the {@link KeyStrategy} defining the keying mechanism for metrics data written
     *                    to Kpow's internal Kafka topic.
     * @return a {@link StreamsAgent} representing the registered application, or {@code null} if registration fails.
     * @throws IllegalArgumentException if any of the provided parameters are {@code null}.
     * @see #unregister(StreamsAgent)
     */
    public StreamsAgent register(KafkaStreams streams, Topology topology, KeyStrategy keyStrategy) {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("io.factorhouse.kpow.agent"));
        IFn registerFn = Clojure.var("io.factorhouse.kpow.agent", "register");
        String id = (String) registerFn.invoke(agent, streams, topology, keyStrategy);
        if (id != null) {
            return new StreamsAgent(id);
        } else {
            return null;
        }
    }

    /**
     * Unregisters a previously registered Kafka Streams application from the registry.
     *
     * <p>Unregistering a Kafka Streams application removes it from active monitoring.
     * After unregistration, no further metrics or observations will be collected or reported for the application.
     * This method is typically used when an application is being stopped or removed.</p>
     *
     * <p>The unregistration process requires a {@link StreamsAgent}, which is provided when the application is
     * initially registered using the {@link #register(KafkaStreams, Topology, KeyStrategy)} method. Each
     * {@code StreamsAgent} contains a unique identifier corresponding to the registered application.</p>
     *
     * <p><b>Note:</b> If the provided {@code StreamsAgent} is {@code null}, or if it does not correspond to
     * a currently registered application, this method will have no effect. It is the caller's responsibility
     * to ensure that the provided {@code StreamsAgent} is valid and matches a previously registered application.</p>
     *
     * @param streamsAgent the {@link StreamsAgent} representing the application to be unregistered.
     *                     Must not be {@code null}.
     * @throws IllegalArgumentException if the {@code StreamsAgent} is invalid or does not correspond
     *                                  to a currently registered application.
     * @see #register(KafkaStreams, Topology, KeyStrategy)
     */
    public void unregister(StreamsAgent streamsAgent) {
        if (streamsAgent != null) {
            IFn require = Clojure.var("clojure.core", "require");
            require.invoke(Clojure.read("io.factorhouse.kpow.agent"));
            IFn unregisterFn = Clojure.var("io.factorhouse.kpow.agent", "unregister");
            unregisterFn.invoke(agent, streamsAgent.getId());
        }
    }

    @Override
    public void close() {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("io.factorhouse.kpow.agent"));
        IFn closeFn = Clojure.var("io.factorhouse.kpow.agent", "close-registry");
        closeFn.invoke(agent);
    }
}
