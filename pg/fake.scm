;; Sometimes it's good to make a Scheme only copy
;; of a pg-result, without the data.
;; So just the `header'

(define-module pg.fake
  (export
   <fake-result>  ;;todo: rename!
   pg-fake-result)

  (use macros.vector)
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
   ;; list of <pg-tuple> objects
   (tuples)))

;;
(define-method pg-ftablecol ((r <fake-result>) i)
  (vector-ref (ref r 'fcolumn) i))
(define-method pg-fsource ((r <fake-result>) i)
  (vector-ref (ref r 'fsource) i))
(define-method pg-ftable ((r <fake-result>) i)
  (vector-ref (ref r 'ftable) i))
(define-method pg-fname ((r <fake-result>) i)
  (vector-ref (ref r 'names) i))


;; given a <pg-result> (or <pgresult>?)
;; create a fake one -- one that returns same characteristics,
;; but not the data.
;; (put 'pg-fake-result 'indent- 3)
(define (pg-fake-result result)
  (let-create-fill-vectors (pg-nfields result)
			   (fcolumn fsource ftable names)

			   ((cute pg-ftablecol result <>)
			    (cute pg-fsource result <>)
			    (cute pg-ftable result <>)
			    (cute pg-fname result <>)
			    )
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
