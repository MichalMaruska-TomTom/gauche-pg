
;;; Operating on pg cursor.
;;; This provides caching. ad-hoc creating

;; todo:  (define-method for-each <pg-cursor> ...)

(define-module pg.cursor
  (export
   <pg-cursor>
   pg:declare-cursor
   pg:fetch-from-cursor
   pg:with-cursor
   )


  (use pg-hi)
  (use pg-low)
  (use adt.string)

  (use mmc.log)

  (use gauche.collection))

(select-module pg.cursor)


(define debug #f)

(define-class <pg-cursor> (<collection>)
  ((handle :init-keyword :handle)
   (name :init-keyword :name)
   (query :init-keyword :query)

   (current-result)
   (current-row)))

;; ((c <pg-cursor>) proc . options)
(define (pg:declare-cursor h name query)
  (DB "pg:declare-cursor: ~d\n")
  (pg-exec h
    (s+ "DECLARE " name
        " CURSOR FOR " query))
  (make <pg-cursor>
    :handle h
    :name name
    :query query))


;; returns 2 values:
;; @result
;; @row-index  ...either a number or #f (if no row availale)!
;; It decouples the 2 amounts -- some `buffering' is used, by default!
(define (pg:fetch-from-cursor cursor . how-many)
  (if (and (slot-bound? cursor 'current-result)
           (> (pg-ntuples (ref cursor 'current-result))
              (+ 1 (ref cursor 'current-row))))
      (begin
        (DB "reusing!\n")
        (update! (ref cursor 'current-row)
                 (lambda (v)
                   (+ 1 v)))
        (values (ref cursor 'current-result)
                (ref cursor 'current-row)))
    (begin
      (DB "pg:fetch-from-cursor ~d ~a\n"
	  (pg-backend-pid (ref (ref cursor 'handle) 'conn))
	  how-many)
      ;; (DB "fetching!\n")
      (let1 r (pg-exec (ref cursor 'handle)
                (s+ "FETCH "
                    (if (null? how-many)
                        "100"		;"NEXT"
                      (number->string (car how-many)))
                    " FROM "
                    (ref cursor 'name) ";"))
        ;; fixme!
        (if (eq? PGRES_TUPLES_OK
                 (pg-result-status (ref r 'result)))
            (begin
              (slot-set! cursor 'current-result r)
              (slot-set! cursor 'current-row 0)
              (if (zero? (pg-ntuples r))
                  (values r #f)
                (values r 0)))
          (values #f #f))))))


;;  call FUNCTION, with 1 argument: a pg cursor, created (and existing) ad hoc.
;;  by query, given NAME.
(define-syntax pg:with-cursor
  (syntax-rules ()
    ((_ handle name query function)
     ;; Cursors need a transaction:
     ;; todo: we might check for one, but needs code in `with-db-transaction*'
     (with-db-transaction* handle
       (function
        (pg:declare-cursor handle name query))))))

(provide "pg/cursor")
