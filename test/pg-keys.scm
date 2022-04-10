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
   :host (sys-getenv "PGHOST")
   :dbname "maruska"))

(test* "pg:load-foreign-keys"
       #t
       (begin
         (pg:load-foreign-keys (pg:find-namespace pg-database))
         #t))

pg:fkey-between

pg:fkey-under

pg:fkey-above


(test-end :exit-on-failure #t)
