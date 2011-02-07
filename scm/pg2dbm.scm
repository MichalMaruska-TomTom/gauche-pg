
;; copy  from PG to BDB

;; fixme: change name!
(define-module pg2dbm
  (export
   copy-pg->dbm
   )
  (use pg)
  (use pg-hi)

  (use pg.sql)
  (use dbm)
;(use gauche.uvector)
  )
(select-module pg2dbm)


(define (copy-pg->dbm query db . args)
  (let-optionals* args
      ((conn (pg-connect ""))           ; fixme!
       (fields '(0 1)))

    ;; assert
    ;; the key & value types match that of the pg result!
    
    (let* ((result (pg-exec conn query)))
      ;; copy:
                                        ;(pg-ntuples result)
      (pg-foreach-result result fields
        (lambda (key value) 
          (dbm-put! db key value)))
      ;;
      ;; (db-sync db 0)
      )))






(provide "pg2dbm")