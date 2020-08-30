(ns green.farm.core
  (:require [green.farm.api :as api]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(def all-user-ids
  (->> (api/users)
       (map :userId)))

(def all-cropping-seasons
  (let [f (fn [uid]
            (->> (api/cropping-season uid)
                 (map #(assoc % :userId uid))))]
    (->> all-user-ids
         (mapcat f))))

(defn map->csv [data filename]
  (with-open [writer (io/writer filename)]
    (let [header (keys (first data))
          body   (map vals (next data))]
      (csv/write-csv writer
                     (conj body header)))))

;; dump
(comment)
(let [data (flatten all-cropping-seasons)]
  (map->csv data "작기정보목록.csv"))