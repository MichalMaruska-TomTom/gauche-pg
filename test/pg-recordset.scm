;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.recordset API features")

(use pg.db)
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
       #t
       (is-a?
        (let* ((recordset
                (make <recordset>
                  :database pg-database
                  :relations "person"
                  :attributes (string-join '("nome" "cognome") ", ")
                  :where "eta > 20"
                  :order "altezza"
                  :limit 10
                  :offset 1)))
          recordset
          )
        <pg-recordset>)
       )
