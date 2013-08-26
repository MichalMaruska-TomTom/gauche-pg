

;; the basic tools:
(define-module pg-low
  (export
   pg-dump-explain

   ;; Converting pg-result into Scheme data:
   pg-attribute-indexes
   pg-map-result
   pg-foreach-result
   pg-map-table

   pg-result->alist
   with-db-transaction
   with-db-transaction*
   )
  (use mmc.log)
  (use mmc.exit)
  (use pg)
  (use mmc.simple)
  (use srfi-1)
  )
(select-module pg-low)


(define debug #f)
;; necessary for modules imported by pg-hi.

;; apply to the whole RELATION!
(define (pg-map-table conn relname function)
  (let* ((result (pg-exec conn
		   (string-append "SELECT * from " (pg:name-printer relname) ";")))
	 ;;(nrows (pg-ntuples result))
         (nfields (pg-nfields result)))
    (for-numbers<* row 0 (pg-ntuples result)
      (apply function
             (map-numbers 0 nfields
               (lambda (j)
                 (pg-get-value result row j)))))))

;; names?
(define (pg-attribute-indexes result attributes)
  ;; symbol, string, index
  (let1 max (pg-nfields result)
    (map
        (lambda (attr)
          (cond
           ((integer? attr)             ;integer!
            (if (or (>= attr max)
                    (< attr 0))
                (error "column index out of bounds for pg-result:" attr 0 max)
              attr))
           ((string? attr)
            (pg-fnumber result attr))   ;fixme!
           ((symbol? attr)
            (pg-fnumber result (symbol->string attr)))
           (else
            (errorf "wrong argument type: given ~s as attribute name (should be symbol,fixnum or string" attr))))
      attributes)))



(define-generic pg-foreach-result)
(define-method pg-foreach-result ((result <pg-result>) attributes function)
  ;; run function for each row, w/ ATTRIBUTES values as arguments
  (let1 numbers (if attributes
                    (pg-attribute-indexes result attributes)
                  (iota (pg-nfields result)))
    (for-numbers<* row 0 (pg-ntuples result)
                                        ;(logformat "pg-foreach-result: ~d ~s\n" row numbers)
      (apply function
             (map (cut pg-get-value result row <>)
               numbers)))))

;; fixme: this should use pg-foreach-result
(define (pg-map-result result attributes function)
  (DB "pg-map-result:\n")
  (let ((numbers (map (cut pg-fnumber result <>) attributes)))
    (map-numbers 0 (pg-ntuples result)
      (lambda (row)
        (apply function
               (map (cut pg-get-value result row <>)
                 numbers))))))


;;  text columns !  Not used!
(define (pg-result->alist result)
  (if (= (pg-nfields result) 2)         ;>=
      (map-numbers* i 0 (pg-ntuples result)
        (cons
         (pg-get-value result i 0)
         (pg-get-value result i 1)))
    (error "the result doesn't give a mapping/alist (insufficient columns)" result)))


;;; `sql'
;; todo!!!!
(define (pg-dump-explain result)
  (for-numbers< 0 (pg-ntuples result)
    (lambda (row)
                                        ;(logformat "pg-foreach-result: ~d ~s\n" row numbers)
      (logformat "~a\n"
                 (pg-get-value result row 0)))))



;;; `transactions':  & exceptions
(define (with-db-transaction db thunk)
  (with-chained-exception-handler*
      (lambda (e next)
        (logformat "with-db-transaction: ABORT-ing! ~a\n" (get-error-string e))
        ;; unless the problem is with the connection
        ;;
        (pg-exec db "ABORT;")           ;i need the result!
        (next e))

    (pg-exec db "BEGIN;")
    (begin0
     (thunk)
     (pg-exec db "COMMIT;"))))

(define-syntax with-db-transaction*
  (syntax-rules ()
    ((_ db body ...)
     (with-db-transaction
      db (lambda ()
           body ...)))))

(provide "pg-low")
