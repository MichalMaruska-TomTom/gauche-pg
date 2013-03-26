
(define-module pg.fake
  (export
   <fake-result>                        ;todo: rename!
   pg-fake-result
   )
  (use mmc.simple)
  (use pg)
  )
(select-module pg.fake)


;;; todo: convert pg-clear to use this:
(define-class <fake-result> ()
  ((names :init-keyword :names)
   (fcolumn :init-keyword :fcolumn)
   (fsource :init-keyword :fsource)
   (ftable :init-keyword :ftable)

   ;; Fixme:
   ;; types
   ;; fmods

   ;; Forced by `../pg-hi.scm'
   (tuples)                             ;list of <pg-tuple> objects
   ))

;;
(define-method pg-ftablecol ((r <fake-result>) i)
  (vector-ref (ref r 'fcolumn) i))
(define-method pg-fsource ((r <fake-result>) i)
  (vector-ref (ref r 'fsource) i))
(define-method pg-ftable ((r <fake-result>) i)
  (vector-ref (ref r 'ftable) i))
(define-method pg-fname ((r <fake-result>) i)
  (vector-ref (ref r 'names) i))


(define (pg-fake-result result)
  (let* ((n (pg-nfields result))
         (fcolumn (make-vector n #f))
         (fsource (make-vector n #f))
         (ftable (make-vector n #f))
         (names (make-vector n #f))
         ;; types
         ;; fmods
         )
    (for-numbers< 0 n
      (lambda (i)
        (vector-set! fcolumn i (pg-ftablecol result i))
        (vector-set! fsource i (pg-fsource result i))
        (vector-set! ftable i (pg-ftable result i))
        (vector-set! names i (pg-fname result i))))
    (let1 ft
        (make <fake-result>
          :names names
          :fcolumn fcolumn
          :fsource fsource
          :ftable ftable)
      (if (slot-bound? result 'tuples)
          (slot-set! ft 'tuples (slot-ref result 'tuples)))
      ft)))


(provide "pg/fake")
