(ns io.factorhouse.kpow.agent
  (:require [clojure.core.protocols :as p]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (io.factorhouse.kpow MetricFilter MetricFilter$FilterCriteria)
           (io.factorhouse.kpow.key KeyStrategy Taxon)
           (java.util UUID)
           (java.util.concurrent Executors ThreadFactory TimeUnit)
           (java.util.concurrent.atomic AtomicInteger)
           (org.apache.kafka.clients.producer Producer ProducerRecord)
           (org.apache.kafka.common MetricName)
           (org.apache.kafka.streams KeyValue Topology TopologyDescription TopologyDescription$GlobalStore
                                     TopologyDescription$Node TopologyDescription$Processor TopologyDescription$Sink
                                     TopologyDescription$Source TopologyDescription$Subtopology)))

(def kpow-snapshot-topic
  {:topic "__oprtr_snapshot_state"})

(extend-protocol p/Datafiable
  Taxon
  (datafy [v]
    (if-let [object-id (.getObjectId v)]
      [(keyword (.getDomain v)) (.getDomainId v) (keyword "kafka" (.getObject v)) object-id]
      [(keyword (.getDomain v)) (.getDomainId v) (keyword "kafka" (.getObject v))]))

  KeyValue
  (datafy [kv]
    {:key   (.key kv)
     :value (.value kv)})

  TopologyDescription
  (datafy [td]
    {:sub-topologies (set (map p/datafy (.subtopologies td)))
     :global-stores  (set (map p/datafy (.globalStores td)))})

  TopologyDescription$Subtopology
  (datafy [st]
    {:id    (.id st)
     :nodes (set (map p/datafy (.nodes st)))})

  TopologyDescription$GlobalStore
  (datafy [gs]
    {:id        (.id gs)
     :source    (p/datafy (.source gs))
     :processor (p/datafy (.processor gs))})

  TopologyDescription$Node
  (datafy [node]
    (cond-> {:name         (.name node)
             :predecessors (set (map #(identity {:name (.name %)}) (.predecessors node)))
             :successors   (set (map #(identity {:name (.name %)}) (.successors node)))}
      (instance? TopologyDescription$Source node)
      (merge {:topic-set     (into #{} (.topicSet ^TopologyDescription$Source node))
              :topic-pattern (some-> (.topicPattern ^TopologyDescription$Source node) str)})
      (instance? TopologyDescription$Processor node)
      (assoc :stores (.stores ^TopologyDescription$Processor node))
      (instance? TopologyDescription$Sink node)
      (merge {:topic             (.topic ^TopologyDescription$Sink node)
              :topic-extraction? (not (nil? (.topicNameExtractor ^TopologyDescription$Sink node)))}))))

(defn metrics
  [streams]
  (into [] (map (fn [[_ metric]]
                  (let [metric-name ^MetricName (.metricName metric)]
                    {:value       (.metricValue metric)
                     :description (.description metric-name)
                     :group       (.group metric-name)
                     :name        (.name metric-name)
                     :tags        (into {} (.tags metric-name))
                     :metric-name metric-name})))
        (.metrics streams)))

(defn application-id
  [metrics]
  (some #(when (= "application-id" (:name %)) (:value %)) metrics))

(defn client-id-tag
  [metrics]
  (some #(get-in % [:tags "client-id"]) metrics))

(defn client-id
  [metrics]
  (let [app-id    (application-id metrics)
        client-id (client-id-tag metrics)]
    (some-> client-id
            (str/replace (str app-id "-") "")
            (str/split #"-StreamThread-")
            (first))))

(defn apply-metric-filters
  [^MetricName metric-name filters]
  (reduce
   (fn [acc ^MetricFilter$FilterCriteria filter-criteria]
     (let [metric-filter-type (.getFilterType filter-criteria)
           predicate          (.getPredicate filter-criteria)]
       (if (.test predicate metric-name)
         (reduced
          (case (.name metric-filter-type)
            "ACCEPT" true
            "DENY" false))
         acc)))
   nil
   filters))

(defn numeric-metrics
  [metrics ^MetricFilter metrics-filter]
  (let [filters (.getFilters metrics-filter)]
    (into [] (comp (filter (comp number? :value))
                   (remove (fn [{:keys [value]}]
                             (if (double? value)
                               (Double/isNaN value)
                               false)))
                   (filter (fn [{:keys [metric-name]}]
                             (apply-metric-filters metric-name filters)))
                   (map #(select-keys % [:name :tags :value])))
          metrics)))

(defn snapshot-send
  [{:keys [snapshot-topic ^Producer producer taxon application-id job-id client-id captured]} data]
  (let [taxon    (p/datafy taxon)
        snapshot {:type           :kafka/streams-agent
                  :application-id application-id
                  :client-id      client-id
                  :captured       captured
                  :data           data
                  :job/id         job-id
                  :snapshot/id    {:domain :streams :id taxon}}
        record   (ProducerRecord. (:topic snapshot-topic) taxon snapshot)]
    (.get (.send producer record))))

(defn metrics-send
  [{:keys [snapshot-topic producer taxon application-id job-id client-id captured]} metrics]
  (let [taxon (p/datafy taxon)]
    (doseq [data (partition-all 50 metrics)]
      (let [value  {:type           :kafka/streams-agent-metrics
                    :application-id application-id
                    :client-id      client-id
                    :captured       captured
                    :data           (vec data)

                    :job/id         job-id
                    :snapshot/id    {:domain :streams :id taxon}}
            record (ProducerRecord. (:topic snapshot-topic) taxon value)]
        (.get (.send producer record))))
    (log/infof "Kpow: sent [%s] streams metrics for application.id %s" (count metrics) application-id)))

(defn plan-send
  [{:keys [snapshot-topic producer job-id captured taxon metrics-summary agent-id application-id client-id]}]
  (let [taxon  (p/datafy taxon)
        plan   {:type           :observation/plan
                :captured       captured
                :snapshot/id    {:domain :streams :id taxon}
                :job/id         job-id
                :application-id application-id
                :client-id      client-id
                :data           {:type  :observe/streams-agent
                                 :agent {:metrics-summary metrics-summary
                                         :id              agent-id
                                         :captured        captured
                                         :version         "1.0.0"}}}
        record (ProducerRecord. (:topic snapshot-topic) taxon plan)]
    (.get (.send producer record))))

(defn snapshot-telemetry
  [{:keys [streams ^Topology topology ^MetricFilter metrics-filter ^KeyStrategy key-strategy] :as ctx}]
  (let [metrics (metrics streams)]
    (if (empty? metrics)
      (log/warn "KafkStreams .metrics() method returned an empty collection, no telemetry was sent. Has something mutated the global metrics registry?")
      (let [topology         (p/datafy (.describe topology))
            state            (str (.state streams))
            snapshot         {:topology topology :state state}
            client-id        (client-id metrics)
            application-id   (application-id metrics)
            taxon            (.getTaxon key-strategy client-id application-id)
            ctx              (assoc ctx
                                    :captured (System/currentTimeMillis)
                                    :client-id client-id
                                    :application-id application-id
                                    :taxon taxon)
            filtered-metrics (numeric-metrics metrics metrics-filter)]
        (when (nil? application-id)
          (throw (Exception. "Cannot infer application id from metrics returned from KafkaStreams instance. Expected metric \"application-id\" in the metrics registry.")))
        (when (nil? client-id)
          (throw (Exception.
                  (format "Cannot infer client id from metrics returned from KafkaStreams instance. Got: client-id %s and application-id %s"
                          (client-id-tag metrics)
                          application-id))))
        (snapshot-send ctx snapshot)
        (metrics-send ctx filtered-metrics)
        (assoc ctx :metrics-summary {:total (count metrics)
                                     :sent  (count filtered-metrics)
                                     :id    (some-> metrics-filter .getFilterId)})))))

(defn snapshot-task
  ^Runnable [snapshot-topic producer registered-topologies metrics-filter latch]
  (fn []
    (let [job-id (str (UUID/randomUUID))
          ctx    {:job-id         job-id
                  :snapshot-topic snapshot-topic
                  :producer       producer
                  :metrics-filter metrics-filter}]
      (doseq [[id [streams topology key-strategy]] @registered-topologies]
        (try (when-let [next-ctx (snapshot-telemetry (assoc ctx
                                                            :streams streams
                                                            :topology topology
                                                            :key-strategy key-strategy
                                                            :agent-id id))]
               (Thread/sleep 2000)
               (plan-send next-ctx))
             (catch Throwable e
               (log/warnf e "Kpow: error sending streams snapshot for agent %s" id))))

      (deliver latch true))))

(defonce thread-factory
  (let [n (AtomicInteger. 0)]
    (reify ThreadFactory
      (newThread [_ r]
        (doto (Thread. r)
          (.setName (format "kpow-streams-agent-%d" (.getAndIncrement n))))))))

(defn start-registry
  [{:keys [snapshot-topic producer metrics-filter]}]
  (log/info "Kpow: starting registry")
  (let [registered-topologies (atom {})
        pool                  (Executors/newSingleThreadScheduledExecutor thread-factory)
        register-fn           (fn [streams topology key-strategy]
                                (let [id (str (UUID/randomUUID))]
                                  (log/infof "Kpow: registering new streams application with id %s" id)
                                  (swap! registered-topologies assoc id [streams topology key-strategy])
                                  id))
        latch                 (promise)
        task                  (snapshot-task snapshot-topic producer registered-topologies metrics-filter latch)
        scheduled-future      (.scheduleWithFixedDelay pool task 500 60000 TimeUnit/MILLISECONDS)]
    {:register         register-fn
     :pool             pool
     :scheduled-future scheduled-future
     :topologies       registered-topologies
     :close            (fn [] (.shutdownNow pool))
     :latch            latch}))

(defn close-registry
  [agent]
  (log/info "Kpow: closing registry")
  (when-let [close (:close agent)]
    (close))
  (when-let [registered-topologies (:topologies agent)]
    (reset! registered-topologies {}))
  {})

(defn init-registry
  [producer metrics-filter]
  (start-registry {:snapshot-topic kpow-snapshot-topic
                   :producer       producer
                   :metrics-filter metrics-filter}))

(defn register
  [agent streams topology key-strategy]
  (when-let [register-fn (:register agent)]
    (let [id (register-fn streams topology key-strategy)]
      (log/infof "Kpow: registring new streams agent %s" id)
      id)))

(defn unregister
  [agent ^String id]
  (when-let [registered-topologies (:topologies agent)]
    (swap! registered-topologies dissoc id)
    (log/infof "Kpow: unregistered streams agent %s" id)
    true))
