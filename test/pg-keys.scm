;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.keys API features")

(use pg.db)
(use pg.namespace)

(use pg.keys)
(test-module 'pg.keys)

(test-section "Load F keys")
(define pg-database
  ;;(with-env* "PGDATABASE" "maruska"
  (pg:connect-to-database
   (sys-getenv "PGHOST")
   "maruska"))

(test* "pg:load-foreign-keys"
       #t
       (pg:load-foreign-keys (pg:find-namespace pg-database)))

pg:fkey-between

pg:fkey-under

pg:fkey-above
