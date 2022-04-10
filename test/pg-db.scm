
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

(define pgconn (pg-conn-of (ref pg-database 'admin-handle)))
;(format #t "Got ~s\n" pg-test)
(test* "pg:connect-to-database"
       #t
       (is-a? pg-database
              <pg-database>))

(test-section "Connection pool")

(pg:with-private-handle* pg-database handle
  (let1 pgconn1 (pg-conn-of handle)

    (test* "same host"
           #t
           (and
            (string=? (pg-host pgconn) (pg-host pgconn1))
            ))

    (test* "same DB"
           #t
           (and
            (string=? (pg-db pgconn) (pg-db pgconn1))
            (string=? *database* (pg-db pgconn1))
    ))))


pg:with-admin-handle
pg:new-handle
pg:dispose-handle
pg:with-private-handle

                                        ;pg:with-admin-handle

pg:add-listener
pg:check-for-notifies

; ???
pg:database-ref
