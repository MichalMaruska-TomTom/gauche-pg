(use gauche.test)

(test-start "pg/result API features")

(use pg)
(use pg.db)
(use pg.result)

(test-module 'pg.result)

(select-module pg.result)
; (set! debug #t)
(use gauche.test)
;;
(test-section "connect & create a result")

(define pg-database
  (pg:connect-to-database :dbname "maruska"))


;; what to do to exploit <db> with a given result?
;; db ---> pg-conn
;;           v
;;   ?<--- result
;; split-result-into-tuples

;; pg:query-extract-tuples
;;

;; low level pg-result
(define result
  (let1 pgconn (pg:new-handle pg-database)
    (pg-set-coding pgconn 'utf8)
    (pg-exec pgconn
      ;; 1,  fails!
      ;; "select 1, * from person A, person B WHERE A.numero = 1309 and B.numero = A.numero -1"
      "select 1, * from person where numero = 1309"
      )))

(test* "split-result-into-tuples"
       1
       (let1 tuples-alist
           (split-result-into-tuples pg-database result)
         ;;(caar tuples-alist)
         (length tuples-alist)))

;; pg:query-extract-tuples


;;  pg-result:find-relation: cannot find tuple from this relation "person"
(test-end :exit-on-failure #t)
