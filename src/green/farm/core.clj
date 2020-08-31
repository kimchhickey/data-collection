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
         (map f))))

(defn map->csv [data columns filename]
  (with-open [writer (io/writer filename)]
    (let [header columns
          body   (->> (next data)
                      (map #(mapv % columns)))]
      (csv/write-csv writer
                     (conj body header)))))

;; dump
(comment
  (let [data    (flatten all-cropping-seasons)
        columns [:userId
                 :calCultivationArea
                 :calPlantNum
                 :croppingDate
                 :croppingEndDate
                 :croppingSeasonName
                 :croppingSerlNo
                 :croppingSystem
                 :cultivationArea
                 :floodlightDec
                 :itemCode
                 :leafArea
                 :planSlabNum
                 :plantDensity
                 :plantNum
                 :standardPlantDensity
                 :statusCode
                 :statusMessage
                 :stemSlabNum
                 :stndMeta
                 :stndSolar
                 :stndTemp
                 :stndWeight]]
    (map->csv data columns "작기정보목록.csv")))

(let [data]
  (map->csv data "경영정보목록.csv"))