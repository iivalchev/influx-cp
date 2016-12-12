(ns influx-cp.core
  (:require [clj-http.client :as client]
            [clojure.string :refer [join]]
            [clojure.data.json :refer [read-str]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :refer [as-url]])
  (:gen-class))

(defn vec-remove
  [coll pos]
  (vec (concat (subvec coll 0 pos) (subvec coll (inc pos)))))

(defn join-key-val [col] (join "," (map #(join "=" %) col)))

(defn json-to-line [json-str]
  (let [json (read-str json-str)]
    (reduce (fn [acc result]
              (reduce (fn [acc series]
                        (let [measure (series "name")
                              tag-set (join-key-val (series "tags"))
                              header (if (empty? tag-set) measure (join "," [measure tag-set]))
                              lines (map (fn [vals]
                                           (let [time-col-index (.indexOf (series "columns") "time")
                                                 time (vals time-col-index)
                                                 cols (filter #(not= "time" %) (series "columns"))
                                                 values (vec-remove vals time-col-index)
                                                 field-set (join-key-val (map vector cols values))]
                                             (join " " [header field-set time])))
                                         (series "values"))
                              lines-joined (join \newline lines)]
                          (if (empty? acc) lines-joined
                              (join \newline [acc lines-joined]))))
                      acc (result "series")))
            "" (json "results"))))

(defn build-query
  ([measure] (build-query measure {}))
  ([measure tags] (str "SELECT * FROM " measure
                       (when (not-empty tags)
                         (str " WHERE "
                              (join " AND "
                                    (map (fn [[tag value]]
                                           (str tag "='" value "'"))
                                         tags))))
                       " GROUP BY *")))

(defn build-source-url [{source :source-url}]
  (str source "/query"))

(defn build-target-url [{target :target-url db :target-db}]
  (str target "/write?db=" db))

(defn build-params
  [{db :source-db measure :measurement tags :tags, :or {:tags {}}}]
  {:query-params {"db" db
                  "epoch" "ms"
                  "q" (build-query measure tags)}})

(def cli-options
  [[nil "--source-url URL" "source InfluxDB URL"
    :parse-fn as-url
    :validate [as-url "Must be a valid URL"]]
   [nil "--target-url URL" "target InfluxDB URL"
    :parse-fn as-url
    :validate [as-url "Must be a valid URL"]]

   [nil "--source-db DB-NAME" "Source db name"]
   [nil "--target-db DB-NAME" "Target db name"]

   ["-m" "--measurement NAME" "Measurement"]
   [nil "--tag KEY=VALUE" "tag"
    :parse-fn (fn [opt]
                (let [[_ k v] (re-matches #"(.+)=(.+)" opt)]
                  {k v}))
    :assoc-fn (fn [m k kv] (update-in m [k] merge kv))]
   ["-h" "--help"]])

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      errors (exit 1 (join \newline errors)))
    (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        source-url (build-source-url options)
        source-params (build-params options)
        target-url (build-target-url options)
        query-rsp (client/get source-url source-params)
        json (:body query-rsp)
        lines (json-to-line json)]
    (client/post target-url {:body lines}))))
