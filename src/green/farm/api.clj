(ns green.farm.api
  (:require [clojure.data.json :as json]
            [clojure.string :as str]))


(def SERVICE_KEY "")

(defn base-url [op-name]
  (str "http://www.smartfarmkorea.net/Agree_WS/webservices/ProvideRestService/"
       op-name
       "/"
       SERVICE_KEY))

(defn crop-season-url [op-name]
  (str "http://www.smartfarmkorea.net/Agree_WS/webservices/CropseasonRestService/"
       op-name
       "/"
       SERVICE_KEY))

(def years (range 2000 2020))

(def target-list
  {:참외    "080200"
   :토마토   "080300"
   :딸기    "080400"
   :오이    "090100"
   :파프리카 "132600"
   :가지    "090300"})

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

(def all-cropping-serl-no-with-env
  "2015년부터 2020년까지 우리가 타겟으로 하는 작물을 대상으로 하는 농가 중에, acqManlYn, acqCultiYn이 Y인 데이터"
  (->> years
       (map get-cropping-season-data-by-year)
       (flatten)
       (filter :itemCode)
       (filter #(some (partial = (:itemCode %)) (vals target-list)))
       (filter #(or (= (:acqManlYn %) "Y")
                    (= (:acqCultiYn %) "Y")))
       (sort-by (juxt :itemCode :croppingSerlNo))))

(defn get-cropping-season-env-data-list [croppingSerlNo page]
  "환경 값을 작기일련번호와 페이지로 조회"
  (-> (str (crop-season-url "getCroppingSeasonEnvDataList") "/" croppingSerlNo "/" page)
      (slurp)
      (json/read-str :key-fn keyword)))

(defn get-all-env [croppingSerlNo]
  "첫번째 필드에 totalPage 정보가 있기 때문에, 해당 값을 기반으로 해서 전체 데이터를 조회"
  (let [{:keys [totalPage]} (first (get-cropping-season-env-data-list croppingSerlNo 1))
        pages (range 1 (inc (Integer/parseInt totalPage)))]
    (->> pages
         (map #(get-cropping-season-env-data-list croppingSerlNo %))
         (flatten))))

(defn get-cropping-season-data-list-by-year [year]
  (-> (str (crop-season-url "getCroppingSeasonDataList") "/" year)
      (slurp)
      (json/read-str :key-fn keyword)))



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
      (json/read-str :key-fn keyword)))


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
