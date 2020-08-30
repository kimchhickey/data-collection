(ns green.farm.api
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))


(def SERVICE_KEY "")

(defn users
  "모든 농가 정보"
  []
  (let [url "http://www.smartfarmkorea.net/Agree_WS/webservices/ProvideRestService/getIdentityDataList"]
    (-> (slurp (str url "/" SERVICE_KEY))
        (json/read-str :key-fn keyword))))


(defn cropping-season
  "농가별 작기현황 정보"
  [user-id]
  (let [url "http://www.smartfarmkorea.net/Agree_WS/webservices/ProvideRestService/getCroppingSeasonDataList"]
    (-> (str url "/" SERVICE_KEY "/" user-id)
        (slurp)
        (json/read-str :key-fn keyword))))


(defn env-list
  "환경 정보(시설원예)

  facilityId: 스마트팜 농장 ID
  fldCode: 분야코드
  measDate: 측정일시
  fatrCode: 항목코드
  itemCode: 품목코드
  sectCode: 분류코드
  "
  [args]
  (let [url  "http://www.smartfarmkorea.net/Agree_WS/webservices/ProvideRestService/getEnvDataList"
        path (->> [:facilityId :measDate :fldCode :sectCode :fatrCode :itemCode]
                  (map args)
                  (str/join "/"))]
    (-> (str url "/" SERVICE_KEY "/" path)
        (slurp)
        (json/read-str :key-fn keyword))))


(comment

  (cropping-season "PF_0000006")

  (env-list
    {:facilityId "PF_0000574_01"
     :fldCode    "FG"
     :measDate   "2017-04-01"
     :fatrCode   "TI"
     :itemCode   "080400"
     :sectCode   "EI"}))