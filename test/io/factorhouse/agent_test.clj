(ns io.factorhouse.agent-test
  (:require [clojure.core.protocols :as p]
            [clojure.test :refer :all]
            [io.factorhouse.kpow.agent :as agent])
  (:import (io.factorhouse.kpow MetricFilter StreamsRegistry)
           (io.factorhouse.kpow.key ClientIdKeyStrategy)
           (java.util Properties)
           (org.apache.kafka.clients.producer Producer)
           (org.apache.kafka.common Metric MetricName)
           (org.apache.kafka.streams KafkaStreams$State StreamsBuilder Topology)))

(defn ^Properties ->props [m]
  (let [props (Properties.)]
    (doseq [[k v] m]
      (.put props k v))
    props))

(deftest filter-props
  (is (= {"compression.type"   "gzip"
          "enable.idempotence" "false"}
         (StreamsRegistry/filterProperties (->props {"fo" "bar"}))))
  (is (= {"bootstrap.servers"  "xyz"
          "compression.type"   "lz4"
          "enable.idempotence" "false"}
         (into {} (StreamsRegistry/filterProperties (->props {"compression.type" "lz4" "bootstrap.servers" "xyz"})))))
  (is (= {"bootstrap.servers"  "xyz"
          "compression.type"   "gzip"
          "enable.idempotence" "false"}
         (into {} (StreamsRegistry/filterProperties (->props {"fo" "bar" "bootstrap.servers" "xyz"}))))))

(defn ^Topology test-topology
  []
  (let [builder (StreamsBuilder.)]
    (.stream builder "__oprtr_snapshot_state")
    (.build builder)))

(defn mock-producer
  [records]
  (reify Producer
    (send [_ record]
      (swap! records conj record)
      (future nil))
    (send [_ record _]
      (swap! records conj record)
      nil)))

(defprotocol MockStreams
  (state [this])
  (metrics [this]))

(defn mock-streams [m]
  (reify MockStreams
    (metrics [_] (into {} m))
    (state [_] KafkaStreams$State/RUNNING)))

(defn mock-metric
  [name group description tags value]
  (let [metric-name  (MetricName. name group description tags)
        metric-value (reify Metric
                       (metricName [_] metric-name)
                       (metricValue [_] value))]
    [metric-name metric-value]))

(deftest agent-metrics
  (is (= [{:value 1.0, :description "mock metric", :group "first", :name "first.metric", :tags {}}
          {:value 2.0, :description "mock metric", :group "first", :name "second.metric", :tags {}}]
         (into []
               (map #(dissoc % :metric-name))
               (agent/metrics (mock-streams [(mock-metric "first.metric" "first" "mock metric" {} 1.0)
                                             (mock-metric "second.metric" "first" "mock metric" {} 2.0)]))))))

(deftest datafy-topo
  (is (= {:sub-topologies #{{:id    0,
                             :nodes #{{:name          "KSTREAM-SOURCE-0000000000",
                                       :predecessors  #{},
                                       :successors    #{},
                                       :topic-set     #{"__oprtr_snapshot_state"},
                                       :topic-pattern nil}}}},
          :global-stores  #{}}
         (p/datafy (.describe (test-topology))))))

(deftest agent-test
  (let [records        (atom [])
        metrics-filter (-> (MetricFilter.) (.accept))
        registry       (agent/init-registry (mock-producer records) metrics-filter)
        agent-id       (agent/register registry
                                       (mock-streams [(mock-metric "first.metric" "first" "mock metric" {} 1.0)
                                                      (mock-metric "application-id" "first" "mock metric" {"client-id" "abc123"} "xxx")
                                                      (mock-metric "second.metric" "first" "mock metric" {"client-id" "abc123"} 2.0)])
                                       (test-topology)
                                       (ClientIdKeyStrategy.))]

    (is agent-id)

    (is (deref (:latch registry) 5000 false))

    (let [captured (into #{} (map (fn [record] (-> record (.value) :captured))) @records)]

      (testing "consistent :captured value"
        (is (= 1 (count captured))))

      (is (= #{[[:streams "abc123" :kafka/streams-agent]
                {:type           :kafka/streams-agent,
                 :application-id "xxx",
                 :client-id      "abc123",
                 :data           {:topology {:sub-topologies #{{:id    0,
                                                                :nodes #{{:name          "KSTREAM-SOURCE-0000000000",
                                                                          :predecessors  #{},
                                                                          :successors    #{},
                                                                          :topic-set     #{"__oprtr_snapshot_state"},
                                                                          :topic-pattern nil}}}},
                                             :global-stores  #{}},
                                  :state    "RUNNING"},
                 :snapshot/id    {:domain :streams, :id [:streams "abc123" :kafka/streams-agent]}}]
               [[:streams "abc123" :kafka/streams-agent]
                {:type           :observation/plan
                 :application-id "xxx",
                 :client-id      "abc123",
                 :snapshot/id    {:domain :streams, :id [:streams "abc123" :kafka/streams-agent]}
                 :data           {:type  :observe/streams-agent
                                  :agent {:captured        (first captured)
                                          :metrics-summary {:id    "custom"
                                                            :sent  2
                                                            :total 3}
                                          :id              agent-id
                                          :version         "1.0.0"}}}]
               [[:streams "abc123" :kafka/streams-agent]
                {:type           :kafka/streams-agent-metrics,
                 :application-id "xxx",
                 :client-id      "abc123",
                 :data           [{:name "first.metric", :tags {}, :value 1.0}
                                  {:name "second.metric", :tags {"client-id" "abc123"}, :value 2.0}],
                 :snapshot/id    {:domain :streams, :id [:streams "abc123" :kafka/streams-agent]}}]}
             (into #{} (map (fn [record]
                              [(.key record) (dissoc (.value record) :job/id :captured)]))
                   @records))))

    (is (agent/unregister registry agent))

    (is (empty? (agent/close-registry registry)))))

(deftest agent-test-metric-filters
  (let [records        (atom [])
        metrics-filter (-> (MetricFilter.)
                           (.denyNameStartsWith "second")
                           (.acceptNameStartsWith "first")
                           (.acceptNameStartsWith "rocksdb")
                           (.deny))
        registry       (agent/init-registry (mock-producer records) metrics-filter)
        agent-id       (agent/register registry
                                       (mock-streams [(mock-metric "first.metric" "first" "mock metric" {} 1.0)
                                                      (mock-metric "rocksdb.foo" "first" "mock metric" {"client-id" "abc123"} 3.0)
                                                      (mock-metric "application-id" "first" "mock metric" {"client-id" "abc123"} "xxx")
                                                      (mock-metric "second.metric" "first" "mock metric" {"client-id" "abc123"} 2.0)])
                                       (test-topology)
                                       (ClientIdKeyStrategy.))]

    (is agent)

    (is (deref (:latch registry) 5000 false))

    (let [captured (into #{} (map (fn [record] (-> record (.value) :captured))) @records)]

      (testing "consistent :captured value"
        (is (= 1 (count captured))))

      (is (= #{[[:streams "abc123" :kafka/streams-agent]
                {:type           :kafka/streams-agent,
                 :application-id "xxx",
                 :client-id      "abc123",
                 :data           {:topology {:sub-topologies #{{:id    0,
                                                                :nodes #{{:name          "KSTREAM-SOURCE-0000000000",
                                                                          :predecessors  #{},
                                                                          :successors    #{},
                                                                          :topic-set     #{"__oprtr_snapshot_state"},
                                                                          :topic-pattern nil}}}},
                                             :global-stores  #{}},
                                  :state    "RUNNING"},
                 :snapshot/id    {:domain :streams, :id [:streams "abc123" :kafka/streams-agent]}}]
               [[:streams "abc123" :kafka/streams-agent]
                {:type           :observation/plan
                 :application-id "xxx",
                 :client-id      "abc123",
                 :snapshot/id    {:domain :streams, :id [:streams "abc123" :kafka/streams-agent]}
                 :data           {:type  :observe/streams-agent
                                  :agent {:captured        (first captured)
                                          :metrics-summary {:id    "custom"
                                                            :sent  2
                                                            :total 4}
                                          :id              agent-id
                                          :version         "1.0.0"}}}]
               [[:streams "abc123" :kafka/streams-agent]
                {:type           :kafka/streams-agent-metrics,
                 :application-id "xxx",
                 :client-id      "abc123",
                 :data           [{:name "first.metric", :tags {}, :value 1.0}
                                  {:name "rocksdb.foo", :tags {"client-id" "abc123"}, :value 3.0}],
                 :snapshot/id    {:domain :streams, :id [:streams "abc123" :kafka/streams-agent]}}]}
             (into #{} (map (fn [record]
                              [(.key record) (dissoc (.value record) :job/id :captured)]))
                   @records))))

    (is (agent/unregister registry agent))

    (is (empty? (agent/close-registry registry)))))