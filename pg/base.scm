
;;; the DB object?
(define-module pg.base
  (export
   ;; generic
   pg:with-handle-of pg:with-handle-of*
   )
  (use pg)
  ;; Fixme!
  (use pg-hi)
  )
(select-module pg.base)
(define debug #f)


(define-generic pg:with-handle-of)

;;; macros:
(define-syntax pg:with-handle-of*
  (syntax-rules ()
    ((_ o h body ...)
     (pg:with-handle-of o
          (lambda (h)
            body ...)))))



(provide "pg/base")
