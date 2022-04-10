;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)
(test-start "pg.sql API features")

(use pg.types)
(use pg.sql)

(test-module 'pg.sql)

(test-section "Parts of SELECT")

(test* "sql:where"
       "(a = '1') AND (b = 2)"
       (sql:where
        `(("a" .  ,((pg:printer-for "text") "1"))
          ("b" .  ,((pg:printer-for "int4") 2))
          )))

(test* "sql-and:"
       "(bla AND bla)"
       (sql-and: "bla" "bla"))


(test* "sql:between"
       "a BETWEEN 1 AND 2"
       (sql:between "a" 1 2))


(test-section "SELECTS")
;;
(test* "sql:select-full"
       "SELECT nome FROM person WHERE eta > 20 ORDER BY altezza LIMIT 10 OFFSET 5"
       (sql:select-full
        "nome"
        ;; from
        "person"
        "eta > 20"
        ;; group-by
        #f
        ;; order-by
        "altezza"
        ;;limit offset
        10 5
        ))

(test* "sql:select-k"
       "SELECT name, surname FROM person WHERE eta>20 GROUP BY country ORDER BY eta"
       (sql:select-k "name, surname"
                                        ; '("name" "surname")
                     "person"
                     ;; :from
                     :where "eta>20"
                     :group-by "country"
                     :order-by "eta"
                     ))

(test* "sql:select-k"
       "SELECT name, surname FROM person WHERE eta>20 GROUP BY country ORDER BY eta"
       (sql:select-k "name, surname"
                                        ; '("name" "surname")
                     :from "person"
                     :where "eta>20"
                     :group-by "country"
                     :order-by "eta"))


(test-section "SELECT functions")
sql:select-function

;;;
(test-section "INSERTs")
(test* "sql:insert"
       "INSERT INTO person\n VALUES (\n1,\n2\n)"
       (sql:insert "person" '("1" "2")))

(test* "sql:insert-alist"
       "INSERT INTO a.b(name)\n VALUES (\n1\n)"
       (sql:insert-alist
        "a.b" '(("name" . "1"))))



(test-section "DELETE")
sql:delete

(test-section "UPDATE")
sql:update

sql:uniquefy-by-attnames-where


(test-end :exit-on-failure #t)
