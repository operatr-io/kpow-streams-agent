(defproject io.factorhouse/kpow-streams-agent "1.0.0-rc12"
  :description "Kpow's Kafka Streams monitoring agent"
  :url "https://github.com/factorhouse/kpow-streams-agent"
  :license {:name         "Apache-2.0 License"
            :url          "https://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo
            :comments     "same as Kafka"}
  :scm {:name "git" :url "https://github.com/factorhouse/kpow-streams-agent"}
  :pom-addition ([:developers
                  [:developer
                   [:id "wavejumper"]
                   [:name "Thomas Crowley"]
                   [:email "tom@factorhouse.io"]
                   [:url "https://factorhouse.io"]
                   [:roles
                    [:role "developer"]
                    [:role "maintainer"]]]
                  [:developer
                   [:id "d-t-w"]
                   [:name "Derek Troy-West"]
                   [:email "derek@factorhouse.io"]
                   [:url "https://factorhouse.io"]
                   [:roles
                    [:role "developer"]
                    [:role "maintainer"]]]])
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [com.cognitect/transit-clj "1.0.333"]
                 [org.clojure/tools.logging "1.3.0"]
                 [org.apache.kafka/kafka-streams "3.7.1" :scope "provided"]]
  :pom-plugins [[org.sonatype.central/central-publishing-maven-plugin "0.6.0"
                 {:extensions    "true"
                  :configuration [:publishingServerId "central"]}]
                [org.apache.maven.plugins/maven-source-plugin "3.3.1"
                 {:executions ([:execution
                                [:id "attach-sources"]
                                [:goals [:goal "jar-no-fork"]]])}]
                [org.apache.maven.plugins/maven-javadoc-plugin "3.11.2"
                 {:executions ([:execution
                                [:id "attach-javadocs"]
                                [:goals [:goal "jar"]]])}]
                [org.apache.maven.plugins/maven-gpg-plugin "3.2.7"
                 {:configuration [:gpgArguments
                                  ([:arg "--pinentry-mode"]
                                   [:arg "loopback"])]
                  :executions    ([:execution
                                   [:id "sign-artifacts"]
                                   [:phase "verify"]
                                   [:goals [:goal "sign"]]])}]]
  :uberjar {:prep-tasks ["clean" "javac" "compile"]
            :aot        :all}
  :profiles {:kaocha     {:dependencies [[lambdaisland/kaocha "1.91.1392"]]}
             :dev        {:resource-paths ["dev-resources"]
                          :plugins        [[lein-cljfmt "0.9.2"]]
                          :dependencies   [[org.slf4j/slf4j-api "2.0.16"]
                                           [ch.qos.logback/logback-classic "1.3.14"]
                                           [cheshire "5.13.0" :exclusions [com.fasterxml.jackson.core/jackson-databind]]
                                           [clj-kondo "2024.09.27"]]}
             :smoke      {:pedantic? :abort}}
  :aliases {"kaocha" ["with-profile" "+kaocha" "run" "-m" "kaocha.runner"]
            "kondo"  ["with-profile" "+smoke" "run" "-m" "clj-kondo.main" "--lint" "src"]
            "fmt"    ["with-profile" "+smoke" "cljfmt" "check"]
            "fmtfix" ["with-profile" "+smoke" "cljfmt" "fix"]}
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :java-source-paths ["src/java"]
  :resource-paths ["src/clojure"]
  :source-paths ["src/clojure"])
