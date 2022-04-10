
;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.db API features")

(use pg)

(use pg.db)
(use pg-hi)

(test-module 'pg.db)

(test-section "Open connection")

(define *database* "test")
(define pg-database
  (pg:connect-to-database
   :host (sys-getenv "PGHOST")
   :dbname *database*))

;(format #t "Got ~s\n" pg-test)
(test* "pg:connect-to-database"
       #t
       (is-a? pg-database
              <pg-database>))


(test* "pg:connect-to-database 2"
       #t
       (is-a? (pg:connect-to-database "/var/run/postgresql" "test")
              <pg-database>))
;;
pg:with-admin-handle
pg:new-handle
pg:dispose-handle
pg:with-private-handle

                                        ;pg:with-admin-handle

pg:add-listener
pg:check-for-notifies

; ???
pg:database-ref
