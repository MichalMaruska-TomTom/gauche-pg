(use gauche.test)

(test-start "pg/caching API features")

(use pg)
(use pg-low)

(use pg.caching)

(test-module 'pg.caching)

;; have a pgconn
(define *PGPORT* (string->number (or (sys-getenv "PGPORT") "5432")))
;;; Code:
(define pgconn (pg-connect "dbname=test"))

;; resolve the oids to types

(test* "setup for one pgconn"
       #t
       (hash-table?
        (pg-init-types-hash pgconn)))

(let1 result (pg-exec-internal pgconn
                               "SELECT oid, typname FROM pg_type WHERE typname!~'^_.*' limit 20;")
  ;; ask for oid names:
  (pg-foreach-result
        result
        '("oid" "typname")
    (lambda (oid typename)
      (test* "lookup type for oid"
             typename
             (pg-type-name pgconn oid)
             )
      )))

(test-end :exit-on-failure #t)
