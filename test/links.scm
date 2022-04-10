;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.links API features")

(use pg.db)
(use pg.links)

(test-module 'pg.links)

(test-section "Prepare")

(define pg-database
  (pg:connect-to-database :dbname "maruska"))

(test* "pg:prepare-fk-links"
       #t
       (and
        (pg:prepare-fk-links pg-database)
        #t))

(test-end)
