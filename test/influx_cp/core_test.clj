(ns influx-cp.core-test
  (:require [clojure.test :refer :all]
            [influx-cp.core :refer :all]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :refer [as-url]]))

(deftest build-query-test
  (testing "QUERY FOR MEASUREMENT"
    (is (= "SELECT * FROM measure GROUP BY *" (build-query "measure")))
    (is (= "SELECT * FROM measure GROUP BY *" (build-query "measure" {})))
    (is (= "SELECT * FROM measure WHERE tag='value' GROUP BY *" (build-query "measure" {"tag" "value"})))
    (is (= "SELECT * FROM measure WHERE tag1='value1' AND tag2='value2' GROUP BY *" (build-query "measure" {"tag1" "value1" "tag2" "value2"})))))

(def json "{
    \"results\": [
        {
           \"series\": [
                {
                   \"name\":\"h2o_quality\",
                   \"tags\": {
                       \"location\":\"santa_monica\",
                       \"randtag\":\"3\"
                    },
                   \"columns\": [
                       \"time\",
                       \"index\"
                    ],
                   \"values\": [
                        [
                           \"2015-08-18T00:12:00Z\",
                            65
                        ],
                        [
                           \"2015-08-18T00:18:00Z\",
                            57
                        ]
                    ]
                }
            ]
        }
    ]
}")

(def line-series (clojure.string/join
                   "\n"
                   ["h2o_quality,location=santa_monica,randtag=3 index=65 2015-08-18T00:12:00Z"
                    "h2o_quality,location=santa_monica,randtag=3 index=57 2015-08-18T00:18:00Z"]))

(def json-no-tags "{
    \"results\": [
        {
           \"series\": [
                {
                   \"name\":\"h2o_quality\",
                   \"columns\": [
                       \"time\",
                       \"index\"
                    ],
                   \"values\": [
                        [
                           \"2015-08-18T00:12:00Z\",
                            65
                        ],
                        [
                           \"2015-08-18T00:18:00Z\",
                            57
                        ]
                    ]
                }
            ]
        }
    ]
}")

(def line-series-no-tags (clojure.string/join
                          "\n"
                          ["h2o_quality index=65 2015-08-18T00:12:00Z"
                           "h2o_quality index=57 2015-08-18T00:18:00Z"]))

(deftest json-to-lines-test
  (testing "TRANSFROM JSON SERIES TO LINE PROTOCOL"
    (is (= line-series (json-to-line json)))
    (is (= line-series-no-tags (json-to-line json-no-tags)))))


(def cli-args ["--source-url" "http://localhost:8086" "--target-url" "http://localhost:8086" "--source-db" "NOAA_water_database" "--target-db" "noaa_test" "-m" "h2o_quality" "-u" "username" "-p" "password"])

(deftest parse-cli-args
  (testing "PARSING OF CLI ARGS"
    (is (= {:source-url
            (as-url "http://localhost:8086"),
            :target-url
            (as-url "http://localhost:8086"),
            :source-db "NOAA_water_database",
            :target-db "noaa_test",
            :measurement "h2o_quality"
            :username "username"
            :password "password"}
           (:options (parse-opts cli-args cli-options))))))

(def options (:options (parse-opts cli-args cli-options)))

(deftest build-source-url-test5B5B5B5B
  (testing "BUILDING SOURCE URL"
    (is (= "http://localhost:8086/query" (build-source-url options)))))

(deftest build-target-url-test
  (testing "BUILDING TARGET URL"
    (is (= "http://localhost:8086/write?db=noaa_test" (build-target-url options)))))

(deftest build-source-url--params-test
  (testing "BUILDING SOURCE URL PARAMS"
    (is (= {:query-params
            {"db" "NOAA_water_database",
             "u" "username"
             "p" "password"
             "epoch" "ns",
             "q" "SELECT * FROM h2o_quality GROUP BY *"}}
           (build-params options)))))
