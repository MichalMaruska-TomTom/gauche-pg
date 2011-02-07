;; `methods' on the `<pgresult>'


;; having an abstract object (`pg-result' here), we want to extract (uniform) sub-objects, and run  
;; through the interface of `for-each' a function on each sub-object.

;; This module is all to provide this method (for-each) ....

;; how to use it:
;; 1/ export  for-each   &  extend gauche.collection ?
;; 2/ consumers have to include gauche.collection ?



;; how relates to pg.oo ?
;; objects extracted from pg-results
(define-module pg.methods           ;; fixme: obsolete?  but used in foto.db
  (export
   pg-result->objects
   <pg-result-iterator>
   ;; <pg-result-object-injector>
   pg-make-object-iterator              ; result + data ->  iterator
   for-each                             ;                     ....  used here
   ;next                                 ; ???
   )
  (use macros.types)
  (use mmc.log)
  (use pg-hi)
  (use srfi-1)
  (use mmc.exit)
                                        ;(use pg.types)
  (use mmc.simple)                       ;map-numbers
  (use adt.ssm)
  ;; `extend' !!
  (extend gauche.collection)
  )

(select-module pg.methods)


;; i don't use a simple closure:
;; i keep the (pg)-result here.
(define-class <pg-result-iterator> ()
  ((result :init-keyword :result)
   (function :init-keyword :function)

   (index :init-value -1)

   (class :init-keyword :class)
   
   (slots :init-keyword :slots)
   (numbers :init-keyword :numbers)
   ;(type-converters :init-keyword :type-converters)
   ))


;; `used'?

;;  should be in pg.scm ?
(define-generic next)                   ; fixme: shouldn't it be a system wide one? And here we destroy it!
(define-method next ((i <pg-result-iterator>))
  (update! (slot-ref i 'index)
           (lambda (i)
             (+ 1 i)))
  ((slot-ref i 'func)
   (slot-ref i 'result)
   (slot-ref i 'index)))




;; hm, the `<pgresult>' converts automagically
(define (transform-row-into-object result row object numbers slots)
  ;;(logformat "transform-row-into-object: ~s ~s\n" numbers slots)
  (check-type result <pgresult> "transform-row-into-object")

  ;; pg-foreach-result:
                                        ;(logformat "transform-row-into-object ~d ~s ~s\n" row numbers slots)
  (fold
   (lambda (index slot object)
     ;;(logformat "setting ~a (~d) \n" slot index)
     (slot-set! object slot
       (pg-get-value result row index))
     object)
   
   object
   numbers
   slots))


(define (dump-tuple pg-result row port)
  (map-numbers* column 0 (pg-nfields pg-result)
    (format port "~a:\t~a\n"
            (pg-fname pg-result column)
            (pg-get-value pg-result row column))))


;; make a list of objects(of class CLASS), each 'composed' from 1 row of the RESULT. SLOTS is a ssm mapping
;; column-name -> 'slot symbol.
;; make objects of class, and fill in the slots symbols.
(define (pg-result->objects result class slots indexes)
  ;; (logformat "pg-result->objects: ~s\n" (ssm-range slots))
  (let*  (
          ;; The old api:
          ;; (numbers (map (cut pg-fnumber result <>)
          ;;           (ssm-range slots)))
          ;;(slots (ssm-domain slots))

          ;; fixme: This should be called with 2 lists:  slots and indexes!
          ;; b/c we compose the query to have it slot1 = 0, slot2 = 1 ...
          
          (db (slot-ref result 'handle))
          (processed-tuple 0))

    (with-chained-exception-handler*
        (lambda (c next)
          (logformat "error while processing tuple number ~d\n" processed-tuple)
          (logformat "the values in the tuple:\n")
          (dump-tuple (result-of result) processed-tuple (current-error-port))
          (next c))
      (map-numbers* row 0 (pg-ntuples result)
        (set! processed-tuple row)
        (transform-row-into-object result row
                                   (make class)
                                   indexes slots)))))







;;  (for-each function result)
(define-method for-each (function (i <pg-result-iterator>))
                                        ;(logformat "for-each  <pg-result-iterator>\n")
  (let ((result (slot-ref i 'result))
        (class (slot-ref i 'class)))
    (for-numbers<* row 0 (pg-ntuples result)
      (function
       ((slot-ref i 'function)
        result row)))))
        
;        (transform-row-into-object
;         result row
;         (make class) 
;         (slot-ref i 'numbers)
;         (slot-ref i 'slots)
;         (slot-ref i 'type-converters))))))



;; <pg-result-object-injector>


;; fixme: this is exactly `<db-stored-class>'
(define (pg-make-object-iterator result class slot-binding);  produces objects of CLASS by extracting SLOTS from the rows of the RESULT
  ;(logformat "pg-make-object-iterator: ~s ~s ~s\n" result class slot-binding)
  (check-type result <pgresult> "pg-make-object-iterator")
  
  
  (let* ((numbers (map (cut pg-fnumber result <>)
                    (ssm-range slot-binding)))
         (slots (ssm-domain slot-binding)))
    (make <pg-result-iterator>
      :function
      (lambda (result row); result useless....already in closure.
        (transform-row-into-object
         result row
         (make class) 
         numbers slots))
      :result result
      ;; not needed?
      :class class
      )))




; (define (pg-result->objects-collection result class slots)
;   (define (transform-row-into-object result row object slots type-converters)
;     ))

(provide "pg/methods")
