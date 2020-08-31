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

;; => {:userId "", :croppingSerlNo ""}
(def all-user-cropping
  (->> (flatten all-cropping-seasons)
       (map #(select-keys % [:userId :croppingSerlNo]))
       (filter :croppingSerlNo)))

(def all-management
  (let [f (fn [{:keys [userId croppingSerlNo] :as m}]
            (->> (api/management userId croppingSerlNo)
                 (merge m)))]
    (->> all-user-cropping
         (map f))))


(defn map->csv [data columns filename]
  (with-open [writer (io/writer filename)]
    (let [header (map name columns)
          body   (->> (next data)
                      (map #(mapv % columns)))]
      (csv/write-csv writer
                     (conj body header)))))

;; dump
(comment
  (let [data    (flatten all-cropping-seasons)
        columns [:userId
                 ;;
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
    (map->csv data columns "작기정보목록.csv"))


  (let [data    all-management
        columns [:userId
                 :croppingSerlNo
                 ;;
                 :maintenancePrice
                 :manpowerPrice
                 :materialsPrice
                 :nutrientPrice
                 :shipmentAmt
                 :shipmentPrice
                 :statusCode
                 :statusMessage
                 :preventionPrice]]
    (map->csv data columns "경영정보목록.csv"))


  )