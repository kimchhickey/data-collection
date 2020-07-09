(ns green.market.monthly
  (:require [clojure.java.io :as io]
            [clojure.java.jdbc :as j]
            [clojure.data.csv :as csv]
            [green.market.core :refer [normalize process]]
            [green.market.config :refer [db]]))

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

(def column-indices
  (sort (vals columns)))

(def column-rank
  (zipmap column-indices (range)))

(defn index-by-name [name]
  (column-rank (columns name)))

(defn column-filter [row]
  (mapv #(get row %) column-indices))

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

(defn preprocess-new [row]
  (->> row
       (column-filter)
       (vec->map)))

(def errors (atom []))

(defn preprocess-old [header cols]
  (try
    (assert (= (count cols) (count header)) "csv parse error.")

    (let [record (zipmap header cols)
          {:keys [DELNG_DE WHSAL_MRKT_NEW_CODE CATGORY_NEW_CODE STD_SPCIES_NEW_CODE DELNG_PRUT STD_UNIT_NEW_CODE SBID_PRIC]} record]
      {:date         DELNG_DE
       :market-code  WHSAL_MRKT_NEW_CODE
       :kind-code    (Integer/parseInt CATGORY_NEW_CODE)
       :species-code STD_SPCIES_NEW_CODE
       :deal-unit    (Float/parseFloat DELNG_PRUT)
       :unit-code    STD_UNIT_NEW_CODE
       :price        (Float/parseFloat SBID_PRIC)})
    (catch AssertionError _
      (swap! errors conj cols)
      nil)))

(defn insert! [m]
  (prn (:date (first m)))
  (time
    (j/insert-multi! db :market_prices m)))

(defn batch [suffix]
  (let [filename (str "원천실시간경락가격원시데이터_" suffix ".csv")

        ; ~3M rows
        rawdata  (-> filename
                     (io/resource)
                     (io/reader :encoding "CP949")
                     (csv/read-csv))

        header   (->> (first rawdata) (map keyword))
        data     (rest rawdata)]

    (assert (= (count header) 45))

    (->> data
         (map #(preprocess-old header %))
         (map normalize)
         (remove nil?)
         (partition-by :date)
         (map process)
         (run! insert!))
    ))

(comment
  (do
    (reset! errors [])
    ;; resources 하위에 "원천실시간경락가격원시데이터_yyyyMM.csv" 파일 위치 후
    (batch "201910"))

  (when (pos? (count @errors))
    (prn "ignored row count: " (count @errors)))
  )