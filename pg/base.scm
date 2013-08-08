
;; each DB object can point to a connection.
;; no locking done.

(define-module pg.base
  (export
   pg:with-handle-of
   pg:with-handle-of*
   )

  (use pg)
  (use pg-hi)
  )
(select-module pg.base)


;; (define debug #f)
(define-generic pg:with-handle-of)

;;; macros:
(define-syntax pg:with-handle-of*
  (syntax-rules ()
    ((_ o h body ...)
     (pg:with-handle-of o
          (lambda (h)
            body ...)))))

(provide "pg/base")
