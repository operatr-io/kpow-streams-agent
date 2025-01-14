package io.factorhouse.kpow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.kafka.common.MetricName;

/**
 * Provides filtering functionality for metrics collected from Kafka Streams applications.
 *
 * <p>The {@code MetricFilter} allows developers to define which metrics should be included or excluded
 * when reporting to Kpow's internal Kafka topic. Filters can be customized to suit the needs of specific
 * deployments or use cases.</p>
 *
 * <h3>Common Filters</h3>
 * <ul>
 *   <li>{@link #acceptAllMetricFilter()} - A filter that accepts all metrics.</li>
 *   <li>{@link #denyAllMetricFilter()} - A filter that denies all metrics.</li>
 *   <li>{@link #stateStoreMetricsOnlyFilter()} - A filter that includes only state store metrics.</li>
 *   <li>{@link #defaultMetricFilter()} - The default filter configuration for typical Kafka Streams applications.</li>
 * </ul>
 *
 * <p>Developers can use these predefined static methods to quickly configure common filtering behaviors,
 * or implement custom filters if required.</p>
 */
public class MetricFilter {

    private String filterId = null;

    /**
     * Returns a unique identifier used by Kpow's user interface to describe which MetricFilter has been configured.
     *
     * @return The identifier of the MetricFilter
     */
    public String getFilterId() {
        return filterId;
    }

    /**
     * Enum representing the type of filter operation.
     */
    public enum FilterType {
        /**
         * Represents the acceptance of a metric.
         */
        ACCEPT,

        /**
         * Represents the denial of a metric.
         */
        DENY,
    }

    /**
     * The FilterCriteria class encapsulates both the filtering type and the predicate used to define a MetricFilter.
     */
    public static class FilterCriteria {

        private final Predicate<MetricName> predicate;
        private final FilterType filterType;

        /**
         * Constructs a new {@link FilterCriteria} object with the specified predicate and filter type.
         *
         * @param predicate The predicate used to define which metrics should be accepted or denied
         * @param filterType The type of filter operation (ACCEPT or DENY)
         */
        private FilterCriteria(
            Predicate<MetricName> predicate,
            FilterType filterType
        ) {
            this.predicate = predicate;
            this.filterType = filterType;
        }

        /**
         * Returns the predicate used in this filter criteria.
         *
         * @return The predicate for filtering metric names.
         */
        public Predicate<MetricName> getPredicate() {
            return predicate;
        }

        /**
         * Returns the type of operation (ACCEPT or DENY) associated with this filter criteria.
         *
         * @return The filter type (ACCEPT or DENY).
         */
        public FilterType getFilterType() {
            return filterType;
        }
    }

    private final List<FilterCriteria> filters;

    /**
     * Creates a new MetricFilter instance for custom-defined filters.
     */
    public MetricFilter() {
        this.filters = new ArrayList<>();
        this.filterId = "custom";
    }

    private MetricFilter(String id) {
        this.filters = new ArrayList<>();
        this.filterId = id;
    }

    /**
     * Returns a metrics filter that accepts all numeric metrics from the running Streams application.
     *
     * @return accept all metric filter
     */
    public static MetricFilter acceptAllMetricFilter() {
        return new MetricFilter("acceptAll").accept();
    }

    /**
     * Returns a metrics filter that denies all metrics, only sending across the Kafka Streams topology + state
     * on every observation.
     *
     * @return deny all metric filter
     */
    public static MetricFilter denyAllMetricFilter() {
        return new MetricFilter("denyAll").deny();
    }

    /**
     * Returns a metrics filter that includes only state store metrics.
     *
     * @return state store metrics only filter
     */
    public static MetricFilter stateStoreMetricsOnlyFilter() {
        Predicate<MetricName> stateStoreMetricsOnly = m ->
            m.tags().containsKey("store") ||
            m.tags().containsKey("in-memory-state-id") ||
            m.tags().containsKey("in-memory-window-state-id") ||
            m.tags().containsKey("in-memory-session-state-id") ||
            m.tags().containsKey("rocksdb-session-state-id") ||
            m.tags().containsKey("rocksdb-state-id") ||
            m.tags().containsKey("rocksdb-window-state-id");
        return new MetricFilter("stateStoreMetricsOnly").accept(
            stateStoreMetricsOnly
        );
    }

    /**
     * Returns the default metricsFilter used by the streams agent.
     * By default, Kpow's streams agent will only send across a few key Kafka Streams metrics:
     *  Latency:
     *  - commit-latency-avg
     *  - process-latency-avg
     *  - poll-latecny-avg
     *  Throughput:
     *  - process-rate
     *  - records-processed-rate
     * Lag:
     * - commit-rate
     * - records-lag-max
     * - records-lag
     * Stability:
     * - failed-stream-threads
     * - rebalances
     * State store health:
     * - put-rate
     * - get-rate
     * - flush-rate
     *
     * @return the default metrics filter
     */
    public static MetricFilter defaultMetricFilter() {
        return new MetricFilter("default")
            // Latency
            .acceptNameStartsWith("commit-latency-avg")
            .acceptNameStartsWith("process-latency-avg")
            .acceptNameStartsWith("poll-latency-avg")
            // Throughput
            .acceptNameStartsWith("process-rate")
            .acceptNameStartsWith("records-processed-rate")
            // Lag
            .acceptNameStartsWith("commit-rate")
            .acceptNameStartsWith("records-lag-max")
            .acceptNameStartsWith("records-lag")
            // Stability
            .acceptNameStartsWith("failed-stream-threads")
            .acceptNameStartsWith("rebalances")
            // State store health
            .acceptNameStartsWith("put-rate")
            .acceptNameStartsWith("get-rate")
            .acceptNameStartsWith("flush-rate");
    }

    /**
     * Returns an unmodifiable list of {@link FilterCriteria} objects representing the current filter rules applied by this MetricFilter.
     *
     * @return An unmodifiable list of {@link FilterCriteria}
     */
    public List<FilterCriteria> getFilters() {
        return Collections.unmodifiableList(filters);
    }

    /**
     * Accepts all metrics.
     *
     * @return an updated MetricFilter
     */
    public MetricFilter accept() {
        Predicate<MetricName> acceptPredicate = _filter -> {
            return true;
        };
        FilterCriteria criteria = new FilterCriteria(
            acceptPredicate,
            FilterType.ACCEPT
        );
        this.filters.add(criteria);
        return this;
    }

    /**
     * Accepts a metric based on the specified Predicate.
     *
     * @param acceptFilter the predicate used to determine if a metric should be accepted
     * @return an updated MetricFilter
     */
    public MetricFilter accept(Predicate<MetricName> acceptFilter) {
        FilterCriteria criteria = new FilterCriteria(
            acceptFilter,
            FilterType.ACCEPT
        );
        this.filters.add(criteria);
        return this;
    }

    /**
     * Denies all metrics.
     *
     * @return an updated MetricFilter
     */
    public MetricFilter deny() {
        Predicate<MetricName> denyFilter = _filter -> {
            return true;
        };
        FilterCriteria criteria = new FilterCriteria(
            denyFilter,
            FilterType.DENY
        );
        this.filters.add(criteria);
        return this;
    }

    /**
     * Denies a metric based on the specified Predicate.
     *
     * @param denyFilter the predicate used to determine if a metric should be denied
     * @return an updated MetricFilter
     */
    public MetricFilter deny(Predicate<MetricName> denyFilter) {
        FilterCriteria criteria = new FilterCriteria(
            denyFilter,
            FilterType.DENY
        );
        this.filters.add(criteria);
        return this;
    }

    /**
     * Accepts all metrics whose name start with the specified prefix.
     *
     * @param prefix the prefix of the metric names to accept
     * @return an updated MetricFilter
     */
    public MetricFilter acceptNameStartsWith(String prefix) {
        Predicate<MetricName> acceptFilter = metricName -> {
            return metricName.name().startsWith(prefix);
        };
        FilterCriteria criteria = new FilterCriteria(
            acceptFilter,
            FilterType.ACCEPT
        );
        this.filters.add(criteria);
        return this;
    }

    /**
     * Denies all metrics whose name start with the specified prefix.
     *
     * @param prefix the prefix of the metric names to deny
     * @return an updated MetricFilter
     */
    public MetricFilter denyNameStartsWith(String prefix) {
        Predicate<MetricName> denyFilter = metricName -> {
            return metricName.name().startsWith(prefix);
        };
        FilterCriteria criteria = new FilterCriteria(
            denyFilter,
            FilterType.DENY
        );
        this.filters.add(criteria);
        return this;
    }
}
