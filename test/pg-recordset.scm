;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.recordset API features")

(use pg.db)
(use pg.types)
(use pg.sql)
(use pg.recordset)

(test-module 'pg.recordset)

(test-section "Prepare")


;; make a search, turn into <recordset>, iterate?
(define pg-database
  (pg:connect-to-database
   :host (sys-getenv "PGHOST")
   :dbname "maruska"))

(test-section "Create")

(test* "create one"
       "prova postfix"
       (let1 recordset
           (make <recordset>
             :database pg-database
             :relations "person"
             :attributes (string-join '("nome" "cognome" "via") ", ")

             :where (sql:alist->where `(("numero" .
                                         ,((pg:printer-for "int4") 1309))))
             ;; :order "altezza"
             ;; :limit 10
             ;; :offset 1
             )
         (let ((result (rs->result recordset))
               (row (rs-get-row recordset 0)))
           (rs-row-get row "via")))
        )


(test-end :exit-on-failure #t)
