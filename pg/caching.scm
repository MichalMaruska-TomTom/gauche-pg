
(define-module pg.caching
  (export
   pg-init-types
   pg-init-types-hash
   ;; should be in pg-hi!
   pg-type-name
   )
  (use pg)
  (use pg-low)
  (use pg.types)
  (use adt.alist)
  )
(select-module pg.caching)

;; Caching types: mapping  oid -> typename.

;; lookup standard types, and associate the oid w/ our <pg-type> objects
;; fixme: should be shared by connections?
;; This is probably useless f.  the low leve pg-conn cannot keep this alist, so
(define (pg-init-types pgconn)
  (let1 alist (pg-result->alist
               (pg-exec-internal pgconn "SELECT oid, typname FROM pg_type"))
    (for-each
        (lambda (i)
          (set-car! i (string->number (car i))))
      alist)
    alist))



;; fixme: should be a method?
;; See `pg-type-name' in pg.types
;; return the name of the pg type (given by oid)
(define (pg-type-name db type-oid)         ; here??
  ;; fixme: the connections might share that table?
  ;; -fixme: use the (ref db 'admin-handle)
  (pg:with-admin-handle db
    (lambda (handle)
      (ref
       (pg-find-type handle type-oid)
       'name)
      ;;(pg-exec-internal pgconn "SELECT oid, typname FROM pg_type")

      ;; (hash-table-get
      ;;        (ref handle 'oid->type)
      ;;        type-oid)
      )))

;;  Bootstrap types for a <pg>
;;  create & return hash:  oid -> <pg-type>
(define (pg-init-types-hash pgconn)          ;<pg>
  (let ((result (pg-exec-internal pgconn "SELECT oid, typname FROM pg_type;"))
        (hash (make-hash-table 'eqv?)))
    (pg-foreach-result
        result
        '("oid" "typname")
      (lambda (oid typname)
        (set! oid (string->number oid))
        (let* ((type
                (make <pg-type>
                  :oid oid
                  :name typname
                  :printer (aget pg:type-printers typname)
                  :parser (aget pg:type-parsers typname))))
          (hash-table-put! hash oid type))))
    hash))




(provide "pg.caching")

