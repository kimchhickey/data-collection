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

(defn get-cultivate-list [api]
  "생육정보 목록"
  (let [startDate "2000-01-01"
        endDate   "2021-01-01"
        f (fn [{:keys [userId croppingSerlNo] :as m}]
            (->> (api userId croppingSerlNo startDate endDate)
                 (filter :itemCode)
                 (map #(merge % m))))]
    (->> all-user-cropping
         (map f)
         (flatten))))

(def all-cultivate-list
  {:main         (get-cultivate-list api/cultivate-list)
   :strawberry   (get-cultivate-list api/strawberry-cultivate-list)
   :korean-melon (get-cultivate-list api/fruit-cultivate-list)})

(defn map->csv [data columns filename]
  (with-open [writer (io/writer filename)]
    (let [header (map name columns)
          body   (->> data ;; (next data)로 했을 때, get-all-env에서 첫번째 row가 실종됨.
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
    (map->csv data columns "1. 작기정보목록.csv"))

  ;; 1. 농가별 작기 현황 정보
  ;; 2. 작기 정보 목록
  ;; 3. 환경 정보 (시설원예)
  (let [data    api/all-cropping-serl-no-with-env
        columns [:croppingSerlNo
                 :itemCode
                 :itemName
                 :fcltyId
                 :addressName
                 :acqAutoYn
                 :croppingEndDate
                 :croppingDate
                 :acqManlYn
                 :acqMgmtYn
                 :acqCultiYn]]
    (map->csv data columns "3-1. 제어생육정보등록유_작기정보목록.csv"))
  
  (let [data    (api/get-all-env 6)
        columns [:itemCode
                 :facilityId
                 :num
                 :measDate
                 :cntCollect
                 :fatrCode
                 :ymd
                 :senVal 
                 :sectCode
                 :fldCode]]
    (map->csv data columns "3. 환경정보_샘플_6.csv"))
  
  
  (let [data    (:main cultivate-list)
        columns [:itemCode
                 :userId
                 :croppingSerlNo
                 :measDate
                 :sampleNum
                 :growLength
                 :flowerTop
                 :stemDiameter
                 :leavesLength
                 :leavesWidth
                 :leavesNum
                 :flowerPosition
                 :fruitsPosition
                 :fruitsNum
                 :harvestPosition
                 :ped
                 :solarCorrection
                 :fruitsWeight]]
    (map->csv data columns "4.생육정보_토마토_파프리카_오이_가지.csv"))
  
  (let [data    (:strawberry cultivate-list)
        columns [:itemCode
                 :userId
                 :croppingSerlNo
                 :measDate
                 :sampleNum
                 ;; 아래 두 필드는 API에서 값이 누락됨.
                 ;; :growLength
                 ;; :flowerTop
                 :leavesNumber
                 :leavesLength
                 :petioleLength
                 :thecaDiameter
                 :fruitClusterNum
                 :firstFlowerNum
                 :secondFlowerNum
                 :thirdFlowerNum
                 :firstFruitsNum
                 :secondFruitsNum
                 :thirdFruitsNum
                 :leavesCutWeight
                 :budding]]
    (map->csv data columns "5.생육정보_딸기.csv"))

  (let [data    (:korean-melon cultivate-list)
        columns [:itemCode
                 :userId
                 :croppingSerlNo
                 :measDate
                 :sampleNum
                 :leavesLength
                 :leavesWidth
                 :thecaDiameter]]
    (map->csv data columns "6. 생육정보_참외.csv"))

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
    (map->csv data columns "7. 경영정보목록.csv"))
  )
