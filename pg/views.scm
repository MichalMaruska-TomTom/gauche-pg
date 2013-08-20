
(define-module pg.views
  (export
   pg:reload-view-definition!
   pg:view-definition-sql
   pg:view-tuples
   )
  (use pg.sql)
  (use pg-hi)
  (use pg.types)
  (use pg.sql)

  (use pg.fake)
  (use pg.base)
  (use pg.db)
  (use pg.namespace)
                                        ;(use pg.attribute)
  (use pg.relation)

  ;; New:
  (use pg.result)

  (use adt.string)
  (use macros.assert)
  (use macros.aif)
  (use mmc.log)
  (use mmc.throw)
  )
(select-module pg.views)


(define (pg:reload-view-definition! view)
  ;; todo: check that it is a view!!
  (check-parameter-type view <pg-view> "wrong type: expected ~a, got ~a")
    (lambda (h)
      ;; the real definition is in `pg_rewrite' (pg_rules)
      (let* ((query (s+ "SELECT " "pg_get_viewdef(oid) AS definition"
                        " FROM pg_class " " WHERE "
                        " oid = " (pg:number-printer (ref view 'oid))))
             (res (pg-exec h query)))
        ;;
        (if (zero? (pg-ntuples res))
            (errorf "VIEW ~a was removed (behind our back)" (ref view 'name)))

        ;; We don't keep the trailing ";"
        (let1 s (pg-get-value res 0 0)
          (if (char=? (string-ref s (- (string-length s) 1)) #\;)
              (string-set! s (- (string-length s) 1) #\ ))
          (slot-set! view 'definition s))

        (let* ((query (s+
                       (ref view 'definition)
                       " LIMIT 0"))
               (result (pg-exec h query))
               ;; Bug:
               (tuples (pg:query-extract-tuples (ref view 'database) result)))
          (slot-set! view 'fake-result
                    (pg-fake-result result))
          (slot-set! view 'tuples tuples))))))

(define (pg:view-tuples view)
  (unless (slot-bound? view 'tuples)
    (pg:reload-view-definition! view))
  (slot-ref view 'tuples))


(define (pg:view-definition-sql view)
  (check-parameter-type view <pg-view> "wrong type: expected ~a, got ~a")
  (s+ "CREATE VIEW " (ref view 'name) " AS "
      (ref view 'definition)))

;; (define (pg:view-definition view)
;;   (pg:with-handle-of* view h
;;     (let1 r
;;         (pg-exec h
;;           (sql:select-function
;;            "pg_get_viewdef"
;;            (pg:number-printer (ref view 'oid))
;;            (pg:bool-printer "f")
;;            )))))




(provide "pg/views")
