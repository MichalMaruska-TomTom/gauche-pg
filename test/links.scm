;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.links API features")

(use pg)
(use pg.links)
(use pg-hi)

(test-module 'pg.db)

(test-section "Prepare")

(define pg-database
  (with-env* "PGDATABASE" "test"
    (pg:connect-to-database)))

(test* "pg:connect-to-database"
       #t
       (and
        (pg:prepare-fk-links pg-database)
        #t))
