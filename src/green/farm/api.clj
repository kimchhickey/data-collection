(ns green.farm.api
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))


(def SERVICE_KEY "")

(defn base-url [op-name]
  (str "http://www.smartfarmkorea.net/Agree_WS/webservices/ProvideRestService/"
       op-name
       "/"
       SERVICE_KEY))

(defn users
  "모든 농가 정보"
  []
  (-> (base-url "getIdentityDataList")
      (slurp)
      (json/read-str :key-fn keyword)))


(defn cropping-season
  "농가별 작기현황 정보"
  [user-id]
  (-> (str (base-url "getCroppingSeasonDataList") "/" user-id)
      (slurp)
      (json/read-str :key-fn keyword)))

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
  (let [path (->> [:facilityId :measDate :fldCode :sectCode :fatrCode :itemCode]
                  (map args)
                  (str/join "/"))]
    (-> (str (base-url "getEnvDataList") "/" path)
        (slurp)
        (json/read-str :key-fn keyword))))

(defn cultivate-list
  "생육정보 토마토/파프리카/오이/가지"
  [userId croppingSerlNo startDate endDate]
  (let [path (->> [(base-url "getCultivateDataList") userId croppingSerlNo startDate endDate]
                  (str/join "/"))]
    (-> (slurp path)
        (json/read-str :key-fn keyword))))

(defn strawberry-cultivate-list
  "생육정보 딸기"
  [userId croppingSerlNo startDate endDate]
  (let [path (->> [(base-url "getStrbCultivateDataList") userId croppingSerlNo startDate endDate]
                  (str/join "/"))]
    (-> (slurp path)
        (json/read-str :key-fn keyword))))

(defn fruit-cultivate-list
  "생육정보 참외"
  [userId croppingSerlNo startDate endDate]
  (let [path (->> [(base-url "getFruitCultivateDataList") userId croppingSerlNo startDate endDate]
                  (str/join "/"))]
    (-> (slurp path)
        (json/read-str :key-fn keyword))))

(defn management
  "경영정보

  croppingSerlNo: 직기일련번호
  "
  [userId croppingSerlNo]
  (-> (str (base-url "getManagementData") "/" userId "/" croppingSerlNo)
      (slurp)
      (json/read-str :keyfn keyword)))


(comment

  (cropping-season "PF_0000006")

  (env-list {:facilityId "PF_0000574_01"
             :fldCode    "FG"
             :measDate   "2017-04-01"
             :fatrCode   "TI"
             :itemCode   "080400"
             :sectCode   "EI"})

  (cultivate-list "PF_0001396" "321" "2016-09-05" "2017-09-30")

  (strawberry-cultivate-list "PF_0001382" "336" "2016-09-05" "2017-09-04")
  (strawberry-cultivate-list "PF_0001382" "336" "2000-01-01" "2021-01-01")

  (fruit-cultivate-list "PF_0001295" "254" "2016-12-02" "2017-12-02")

  (management "PF_0000265" "74")
  )