
;; This is a table to know the Type name of results.
;; It is bound with a given database! associated with PID of the conn
(define-module pg.caching
  (export
   pg-init-types-hash
   ;; should be in pg-hi!
   pg-type-name
   )
  (use pg)
  (use pg-low)
  (use adt.alist)
  (use scheme.ephemeron)
  )
(select-module pg.caching)

;; fixme: should be a method?

;; See pg.types, `pg-type-name'

;; so I have a "conn"

;; Return the name of the pg type given by oid.
;; @db is <pg> ?


;; I want a weak one. the <pg> object have to point at it to keep it up!
;; todo: (define pg:type-tables (make-hash-table))

(define ephemerons (list ))

(define (retrieve pgconn)
  ;; first implementation:
  ;; (ref handle 'oid->type)
  ;; 2nd .. hash table

  ;; 3rd ...
  (let1 e
      (find
       (lambda (e)
         (eq? (ephemeron-key e)
              pgconn))
       ephemerons)
    (ephemeron-datum e)))

(define (associate pgconn hash)
  ;; todo: trigger GC  (for-each ephemeron-broken? ephemerons)
                                        ;(put pg:type-tables pgconn
  (push! ephemerons
         (make-ephemeron pgconn hash)))


;; how to associated it with conn?  I have <pg> there?
(define (pg-type-name pgconn type-oid)
  ;; fixme: the connections might share that table?
  (hash-table-get
   (retrieve pgconn)
   (x->number type-oid)))


;; mmc: So I want ephemeron, so that the key = pgconn while up,
;; keeps the value = table up!
;; ephemeron-broken?

;; scheme.ephemeron
;; conn -> weak
;; conn -> index -> weak-vector?
;; (weak-vector-ref wvec k :optional fallback)
;; weak-
;; weak-vector-set! wvec k obj
;; (make-weak-vector 10)

;;  Bootstrap types for a <pg>
;;  create & return hash:  oid -> <pg-type>
;;  <pg>
(define (pg-init-types-hash pgconn)
  (let ((result (pg-exec-internal pgconn "SELECT oid, typname FROM pg_type;"))
        (hash-table (make-hash-table 'eqv?)))

    ;pg-result->alist
    (pg-foreach-result
        result
        '("oid" "typname")

      (lambda (oid typename)
        (let1 oid-int (string->number oid)
        (hash-table-put! hash-table oid-int typename)
        #;(let* ((type
                (make <pg-type>
                  :oid oid
                  :name typname
                  ;; These are named types, which I know in Gauche how to print.
                  :printer (aget pg:type-printers typname)
                  :parser (aget pg:type-parsers typname))))
        (hash-table-put! hash-table oid type))
        )))

    (associate pgconn hash-table)
    hash-table))

(provide "pg.caching")
