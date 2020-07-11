(ns green.market.core)

(def columns-ko {:date         :경락일자
                 :market-code  :시장코드
                 :kind-code    :부류코드
                 :species-code :품종코드
                 :deal-unit    :거래단량
                 :unit-code    :단위코드
                 :price        :거래가격
                 :amount       :거래량})

(def columns-en {:date         :DELNG_DE
                 :market-code  :WHSAL_MRKT_NEW_CODE
                 :kind-code    :CATGORY_NEW_CODE
                 :species-code :STD_SPCIES_NEW_CODE
                 :deal-unit    :DELNG_PRUT
                 :unit-code    :STD_UNIT_NEW_CODE
                 :price        :SBID_PRIC
                 :amount       :DELNG_QY})

(defn normalize [{:keys [date market-code kind-code species-code deal-unit unit-code price amount] :as record}]
  ; 부류코드 18~93까지는 제외
  (when (and kind-code (< kind-code 18))
    (assert (#{"11" "12" "13"} unit-code) (pr-str "Unknown unit: " record))
    (let [unit-price (/ (cond-> price
                          (= unit-code "11") (* 1000)
                          (= unit-code "13") (/ 1000))
                        deal-unit)]
      {:date         date
       :market-code  market-code
       :species-code species-code
       :unit-price   unit-price
       :amount       (* amount deal-unit)})))

(defn mean
  ([vs] (mean (reduce + vs) (count vs)))
  ([sm sz] (/ sm sz)))

(defn aggregate [rows]
  (let [prices (map :unit-price rows)]
    {:price_min     (Math/round (apply min prices))
     :price_max     (Math/round (apply max prices))
     :price_average (Math/round (mean prices))
     :total_amount  (apply + (map :amount rows))
     :num_deals     (count rows)}))

(defn process [data]
  (->> data
       (group-by (juxt :species-code :market-code))
       (mapv (fn [[[species-code market-code] rows]]
               (let [date (:date (first rows))]
                 (assoc (aggregate rows)
                   :date date
                   :market_code market-code
                   :farm_code species-code))))))