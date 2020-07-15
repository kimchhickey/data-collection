(ns green.market.config)

(def db-spec {:dbtype         "mysql"
              :dbname         (System/getenv "DB_NAME")
              :host           (System/getenv "DB_HOST")
              :user           (System/getenv "DB_USER")
              :password       (System/getenv "DB_PASSWORD")
              :serverTimezone "Asia/Seoul"
              })
