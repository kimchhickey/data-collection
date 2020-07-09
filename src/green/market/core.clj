(ns green.market.core)

(defn normalize [{:keys [date market-code kind-code species-code deal-unit unit-code price] :as record}]
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
       :unit-price   unit-price})))

(defn mean
  ([vs] (mean (reduce + vs) (count vs)))
  ([sm sz] (/ sm sz)))

(defn aggregate [rows]
  (let [prices (map :unit-price rows)]
    {:price_min     (Math/round (apply min prices))
     :price_max     (Math/round (apply max prices))
     :price_average (Math/round (mean prices))}))

(defn process [data]
  (->> data
       (group-by (juxt :species-code :market-code))
       (mapv (fn [[[species-code market-code] rows]]
               (let [date (:date (first rows))]
                 (assoc (aggregate rows)
                   :date date
                   :market_code market-code
                   :farm_code species-code))))))