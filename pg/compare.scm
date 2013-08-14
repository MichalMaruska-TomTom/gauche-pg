
;;; Comparing PG data. For ordering.

(define-module pg.compare
  (export
   pg:compare-date
   pg:string-compare
   pg:compare-char
   )
  (use srfi-19)
  (use srfi-13)
  )
(select-module pg.compare)

;; -1 if d1 < d2
(define (pg:compare-date d1 d2)
  (let ((t1 (date->time-utc d1))
        (t2 (date->time-utc d2)))
    (cond
     ((time<? t1 t2)
      -1)
     ((time=? t1 t2)
      0)
     (else
      1))))

(define (pg:string-compare s1 s2)
  (cond
   ((string< s1 s2)
    -1)
   ((string=? s1 s2)
    0)
   (else
    1)))

(define (pg:compare-char c1 c2)
  (cond
   ((char=? c1 c2)
   0)
   ((char<? c1 c2)
    -1)
   (else 1)))

;(compare-char #\a #\b)
;(compare-char #\c #\b)

(provide "pg/compare")
