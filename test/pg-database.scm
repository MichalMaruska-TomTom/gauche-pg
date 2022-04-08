
;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.database API features")

;(use pg)
(use mmc.env)
(use pg.db)
(use pg.database)

(test-module 'pg.database)

(test-section "Open connection")

(define pg-database
  (with-env* "PGDATABASE" "maruska"
    (pg:connect-to-database)))

(test* "pg:connect-to-database"
       #t
       (is-a? pg-database
              <pg-database>))


(test* "Get attribute"
       #t
       (is-a?
        (pg:find-attribute pg-database
                           "public" "person" "cognome")
        <pg-attribute>))
