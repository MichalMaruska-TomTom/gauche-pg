
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

;; @view is `<pg-view>'
;; it updates its slots:
(define (pg:reload-view-definition! view)
  ;; todo: check that it is a view!!
  (check-parameter-type view <pg-view> "wrong type: expected ~a, got ~a")

  (pg:with-admin-handle (ref view 'database)
    ;;fixme: Does this catch the possible error inside?
    (lambda (h)
      ;; the real definition is in `pg_rewrite' (pg_rules)
;;       (sql:select-function
;;            "pg_get_viewdef"
;;            (pg:number-printer (ref view 'oid))
;;            (pg:bool-printer "f")
      (let* ((query (sql:select-k
		     "pg_get_viewdef(oid) AS definition"
		     :from "pg_class"
		     :where
		     (sql:alist->where
		      `(("oid" . ,(pg:number-printer (ref view 'oid)))))))
	     ;; low level execution:
             (res (pg-exec h query)))

        (if (zero? (pg-ntuples res))
            (errorf "VIEW ~a was removed (behind our back)" (ref view 'name)))

        ;; We don't keep the trailing ";"
        (let1 s (pg-get-value res 0 0)
          (if (char=? (string-ref s (- (string-length s) 1)) #\;)

	      ;;(set s (string-drop-right s 1))
	      ;; optimization?
              (string-set! s (- (string-length s) 1) #\ ))
          (slot-set! view 'definition s))

	;; not the types/names etc.:
        (let* ((query (s+
                       (ref view 'definition)
                       " LIMIT 0"))
               (result (pg-exec h query))
               ;; Bug:
               (tuples (pg:query-extract-tuples (ref view 'database) result)))
          (slot-set! view 'fake-result
                    (pg-fake-result result))
          (slot-set! view 'tuples tuples))))))

;; return list of <pg-tuples> in a row of the @view
(define (pg:view-tuples view)
  (unless (slot-bound? view 'tuples)
    (pg:reload-view-definition! view))
  (slot-ref view 'tuples))

;; return as string the Statement to defin the view.
;; mmc: should include the namespace, then permissions etc.
(define (pg:view-definition-sql view)
  (check-parameter-type view <pg-view> "wrong type: expected ~a, got ~a")
  (s+ "CREATE VIEW " (ref view 'name) " AS "
      (ref view 'definition)))

(provide "pg/views")
