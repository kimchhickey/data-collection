(ns green.monthly
  (:require [clojure.java.io :as io]
            [next.jdbc :as j]
            [clojure.data.csv :as csv]
            [green.config :refer [db]]))

(def ^{:doc "관심 컬럼 인덱스"}
  columns
  {"경락일자" 0                                                 ; delngDe
   "시장코드" 4                                                 ; whsalMrktNewCode
   #_#_"경매원표번호" 12
   "부류코드" 14                                                ; catgoryNewCode
   #_#_"품목코드" 18
   "품종코드" 22                                                ; stdSpciesNewCode
   "거래단량" 26                                                ; delngPrut
   "단위코드" 27                                                ; stdUnitNewCode
   #_#_"등급코드" 33
   "거래가격" 37                                                ; sbidPric
   #_#_"산지코드" 40
   #_#_"거래량" 44                                             ; delngQy
   })

;

(def column-indices
  (sort (vals columns)))

(def column-rank
  (zipmap column-indices (range)))

(defn index-by-name [name]
  (column-rank (columns name)))

(defn column-filter [row]
  (mapv #(get row %) column-indices))

(defn normalize [{:keys [date market-code kind-code species-code deal-unit unit-code price] :as record}]
  ; 부류코드 18~93까지는 제외
  (when (< kind-code 18)
    (assert (#{"11" "12" "13"} unit-code) (pr-str "Unknown unit: " record))
    (let [unit-price (/ (cond-> price
                          (= unit-code "11") (* 1000)
                          (= unit-code "13") (/ 1000))
                        deal-unit)]
      {:date         date
       :market-code  market-code
       :species-code species-code
       :unit-price   unit-price})))

(defn mean
  ([vs] (mean (reduce + vs) (count vs)))
  ([sm sz] (/ sm sz)))

(defn aggregate [rows]
  (let [prices (map :unit-price rows)]
    {:price_min     (Math/round (apply min prices))
     :price_max     (Math/round (apply max prices))
     :price_average (Math/round (mean prices))}))

(defn vec->map [row]
  (try
    {:date         (row (index-by-name "경락일자"))
     :market-code  (row (index-by-name "시장코드"))
     :kind-code    (Integer/parseInt (row (index-by-name "부류코드")))
     :species-code (row (index-by-name "품종코드"))
     :deal-unit    (Float/parseFloat (row (index-by-name "거래단량")))
     :unit-code    (row (index-by-name "단위코드"))
     :price        (Float/parseFloat (row (index-by-name "거래가격")))}
    (catch NumberFormatException e
      (prn row)
      (throw e))))

(defn process [data]
  (->> data
       (group-by (juxt :species-code :market-code))
       (mapv (fn [[[species-code market-code] rows]]
               (let [date (:date (first rows))]
                 (assoc (aggregate rows)
                   :date date
                   :market_code market-code
                   :farm_code species-code))))))

(defn insert! [m]
  (prn (:date (first m)))
  #_(time
      (j/insert-multi! mysql-db :market_prices m)))


(defn batch [suffix]
  (let [filename (str "원천실시간경락가격원시데이터_" suffix ".csv")

        ; ~3M rows
        rawdata  (-> filename
                     (io/resource)
                     (io/reader :encoding "CP949")
                     (csv/read-csv))

        header   (first rawdata)
        data     (rest rawdata)]

    ; 10월 부터는 컬럼 헤더가 영어로 되어 assert 주석 처리
    #_(assert (= (map #(get header %) column-indices)
                 (keys columns)))

    (->> data
         (map column-filter)
         (partition-by first)
         (map normalize)
         (remove nil?)
         (map process)
         (run! insert!))
    ))

(comment
  ;; resources 하위에 "원천실시간경락가격원시데이터_yyyyMM.csv" 파일 위치 후
  (batch "201910"))