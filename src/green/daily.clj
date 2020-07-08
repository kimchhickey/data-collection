(ns green.daily
  (:require [clojure.java.jdbc :as j]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :as zx]
            [clojure.data.xml :as xml]
            [clojure.data.zip.xml :refer [xml-> tag=]]
            [green.monthly :refer [normalize process]]
            [green.config :refer [db]]
            [clojure.string :as str]))


(defn fetch-jobs [date]
  (let [id-list (->> (j/query db ["select id, count from price_temp where date = ? and count > 0" date])
                     (map :id)
                     (sort))]
    id-list))

(defn parse-item [elem]
  (->> (zip/children elem)
       (map (fn [p]
              [(:tag p) (-> p :content first)]))
       (into {})))

(defn fetch-items [job-id]
  (prn "fetching" job-id)
  (let [root (->> (j/query db ["select data from price_temp where id = ?", job-id])
                  first
                  :data
                  (xml/parse-str))
        body (-> root
                 :content
                 second
                 :content
                 first)]

    (assert (= (:tag body) :items))

    (->> (zx/xml-> (zip/xml-zip body) :item)
         (map parse-item))))

(defn preprocess [{:keys [delngDe whsalMrktNewCode catgoryNewCode stdSpciesNewCode delngPrut stdUnitNewCode sbidPric]}]
  (let [date (str/join "-" ((juxt #(subs % 0 4) #(subs % 4 6) #(subs % 6 8)) delngDe))]
    {:date         date
     :market-code  whsalMrktNewCode
     :kind-code    (Integer/parseInt catgoryNewCode)
     :species-code stdSpciesNewCode
     :deal-unit    (Float/parseFloat delngPrut)
     :unit-code    stdUnitNewCode
     :price        (Float/parseFloat sbidPric)}))

(defn insert! [m]
  (j/insert-multi! db :market_prices m))

(defn batch-daily [date]
  (let [jobs (fetch-jobs date)
        data (mapcat fetch-items jobs)]
    (prn "Number of jobs: " (count jobs))
    (->> data
         (map preprocess)
         (map normalize)
         process
         insert!)))

(comment
  (time
    (batch-daily "2020-07-04"))
  )