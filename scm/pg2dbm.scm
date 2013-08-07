
;; copy  from PG to dbm (could be BDB)

;; fixme: change name!
(define-module pg2dbm
  (export
   copy-pg->dbm)

  (use pg)
  (use pg-hi)

  (use pg.sql)
  (use dbm))

(select-module pg2dbm)


;; todo: @pgconn @query ...
;; the key & value types must match that of the pg result!
(define (copy-pg->dbm query db . args)
  (let-optionals* args
      ((conn (pg-connect ""))
       (fields '(0 1)))
    ;; assert: at least 2 columns.
    (let* ((result (pg-exec conn query)))
      ;; copy:
      (pg-foreach-result result fields
        (lambda (key value)
          (dbm-put! db key value)))

      ;; (db-sync db 0)
      ;; close it?
      )))

(provide "pg2dbm")
