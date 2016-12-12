(defproject influx-cp "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "https://github.com/iivalchev/influx-cp"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-http "3.4.1"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.5"]]
  :main ^:skip-aot influx-cp.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
