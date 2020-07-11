(ns green.market.daily
  (:require [clojure.java.jdbc :as j]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :as zx]
            [clojure.data.xml :as xml]
            [clojure.data.zip.xml :refer [xml-> tag=]]
            [green.market.core :refer [normalize process]]
            [green.market.config :refer [db-spec]]
            [clojure.string :as str]))


(defn fetch-jobs [db date]
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

(defn preprocess [{:keys [delngDe whsalMrktNewCode catgoryNewCode stdSpciesNewCode delngPrut stdUnitNewCode sbidPric delngQy]}]
  (let [date (str/join "-" ((juxt #(subs % 0 4) #(subs % 4 6) #(subs % 6 8)) delngDe))]
    {:date         date
     :market-code  whsalMrktNewCode
     :kind-code    (Integer/parseInt catgoryNewCode)
     :species-code stdSpciesNewCode
     :deal-unit    (Float/parseFloat delngPrut)
     :unit-code    stdUnitNewCode
     :price        (Float/parseFloat sbidPric)
     :amount       (Integer/parseInt delngQy)}))

(defn update! [{:keys [date market_code farm_code total_amount num_deals]}]
  (prn date market_code farm_code)
  (j/update! db :market_prices
             {:total_amount total_amount
              :num_deals    num_deals}
             ["date=? and market_code=? and farm_code=?"
              date market_code farm_code]))

(defn save! [db job]
  (println job)
  (->> (fetch-items job)
       (map preprocess)
       (map normalize)
       process
       (j/insert-multi! db :market_prices)))

(defn batch-daily [date]
  (j/with-db-connection [db db-spec]
    (let [jobs (fetch-jobs db date)]
      (prn "Number of jobs: " (count jobs))
      (run! #(save! db %) jobs))))

(comment
  (time
    (batch-daily "2020-07-10"))
  )