(ns green.market.monthly
  (:require [clojure.java.io :as io]
            [clojure.java.jdbc :as j]
            [clojure.data.csv :as csv]
            [green.market.core :refer [normalize process columns-ko columns-en]]
            [green.market.config :refer [db]]))

(def header-ko [:경락일자 :경매시간 :경매구분코드 :경매구분코드명 :시장코드 :시장명 :구시장코드 :구시장명 :도매시장법인코드 :법인명 :구법인코드 :구법인명 :경매원표번호 :일련번호 :부류코드 :부류명 :구부류코드 :구부류명 :품목코드 :품목명 :구품목코드 :구품목명 :품종코드 :품종명 :구품종코드 :구품종명 :거래단량 :단위코드 :단위명 :포장상태코드 :포장상태명 :크기코드 :크기명 :등급코드 :등급명 :법인사용품목코드 :법인사용품목명 :거래가격 :출하구분코드 :출하구분명 :산지코드 :산지명 :구산지코드 :구산지명 :거래량])
(def header-en [:DELNG_DE :SBID_TIME :AUC_SE_CODE :AUC_SE_NM :WHSAL_MRKT_NEW_CODE :WHSAL_MRKT_NEW_NM :WHSAL_MRKT_CODE :WHSAL_MRKT_NM :CPR_INSTT_NEW_CODE :INSTT_NEW_NM :CPR_INSTT_CODE :INSTT_NM :LEDG_NO :SLE_SEQN :CATGORY_NEW_CODE :CATGORY_NEW_NM :CATGORY_CODE :CATGORY_NM :STD_PRDLST_NEW_CODE :STD_PRDLST_NEW_NM :STD_PRDLST_CODE :STD_PRDLST_NM :STD_SPCIES_NEW_CODE :STD_SPCIES_NEW_NM :STD_SPCIES_CODE :STD_SPCIES_NM :DELNG_PRUT :STD_UNIT_NEW_CODE :STD_UNIT_NEW_NM :STD_FRMLC_NEW_CODE :STD_FRMLC_NEW_NM :STD_MG_NEW_CODE :STD_MG_NEW_NM :STD_QLITY_NEW_CODE :STD_QLITY_NEW_NM :CPR_USE_PRDLST_CODE :CPR_USE_PRDLST_NM :SBID_PRIC :SHIPMNT_SE_CODE :SHIPMNT_SE_NM :STD_MTC_NEW_CODE :STD_MTC_NEW_NM :CPR_MTC_CODE :CPR_MTC_NM :DELNG_QY])

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
   "거래량"  44                                                ; delngQy
   })

(def errors (atom []))

(defn preprocessor [header col-map]
  (fn [cols]
    (try
      (assert (= (count cols) (count header)) "csv parse error.")

      (let [record (zipmap header cols)]
        {:date         (record (:date col-map))
         :market-code  (record (:market-code col-map))
         :kind-code    (Integer/parseInt (record (:kind-code col-map)))
         :species-code (record (:species-code col-map))
         :deal-unit    (Float/parseFloat (record (:deal-unit col-map)))
         :unit-code    (record (:unit-code col-map))
         :price        (Float/parseFloat (record (:price col-map)))})

      (catch AssertionError _
        (swap! errors conj cols)
        nil)
      (catch Throwable t
        (prn cols)
        (throw t)))))

(defn insert! [m]
  (prn (:date (first m)) (count m))
  (time
    (j/insert-multi! db :market_prices m)))

(defn batch [filename]
  (let [rawdata (-> filename
                    (io/resource)
                    (io/reader :encoding "CP949")
                    (csv/read-csv))

        header  (->> (first rawdata) (map keyword))

        ; ~3M rows
        data    (rest rawdata)]

    (assert (= (count header) 45))
    (assert (= header header-ko))

    (->> data
         (map (preprocessor header columns-ko))
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
    (let [filename
          #_"원천실시간경락가격원시데이터_201904.csv"
          "도매시장실시간경락정보_201903.csv"]
      (batch filename)))

  (when (pos? (count @errors))
    (prn "ignored row count: " (count @errors)))
  )