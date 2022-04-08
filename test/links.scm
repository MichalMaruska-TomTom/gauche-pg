;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.links API features")

(use mmc.env)
(use pg.db)
(use pg.links)

(test-module 'pg.links)

(test-section "Prepare")

(define pg-database
  (with-env* "PGDATABASE" "test"
    (pg:connect-to-database)))


(test* "pg:prepare-fk-links"
       #t
       (and
        (pg:prepare-fk-links pg-database)
        #t))
