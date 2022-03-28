
;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(use pg)
(use pg-low)

(test-start "pg-low API features")
(test-section "Connection")

;;; Configuration
;(sys-setenv "PG_MMC" "y")




;;; Code:
(define pgcon (pg-connect "dbname=test"))

(define *PGPORT* (string->number (or (sys-getenv "PGPORT") "5432")))

(test* "connection params"
       (list *PGPORT* "michal")

       (list (string->number
              (pg-port pgcon))
             (pg-user pgcon)))

;; todo:
;; async connection.

;; todo:
;; pg-backend-pid
(test-section "pg Exec")
;; todo:  async exec.


;; todo: extract error.
;; pg-result-error-field


;; how to test pg-result->alist ?
;; I need  sql-insert-values for that!
;; or write the SQL here.
(test* "query exec"
       ;; create
       PGRES_COMMAND_OK
       (let1 result (pg-exec pgcon "CREATE SCHEMA gauche_test;")
         (pg-result-status result)))


(test* "query Create table"
       ;; create
       PGRES_COMMAND_OK
       (let1 result
           (pg-exec pgcon
             "create table gauche_test.people (name text, surname varchar, age int);")
         (pg-result-status result)))

(test* "inserting"
       (list PGRES_COMMAND_OK "0")	;why 0?
       ;; not PGRES_TUPLES_OK  why not?
       (let1 result (pg-exec pgcon
                      "insert into gauche_test.people values ('Michal', 'Maruska');")
         (list (pg-result-status result)
               (pg-oid-status result))))


(test-section "pg access result")
(define result (pg-exec pgcon "select name, surname, age from gauche_test.people;"))

(test* "select"
       PGRES_TUPLES_OK
       (pg-result-status result))

; select oid from pg_class where relname="people" and namespace = ;

(test* "result ntuples etc."
       (list 1 3 "surname" 2 1 #f)
       (list
        (pg-ntuples result)
        (pg-nfields result)
        (pg-fname result 1)
        (pg-fnumber result "age")
        (pg-ftablecol result 0)		;fixme: 1
        (pg-ftable result 0)		;
        ))

;; I want to see the error thrown!
(test* "result column indices"
       '(0 1 2)
       (pg-attribute-indexes result '("name" surname 2)))

(let1 result (pg-exec pgcon "select name, surname FROM gauche_test.people;")
  '(("Michal" . "Maruska"))
  (pg-result->alist result))



(define res (pg-exec pgcon "explain select * from gauche_test.people where name = 'Michal';"))
(pg-dump-explain res)


(test* "query exec"
       ;; create
       PGRES_COMMAND_OK
       (let1 result
           (pg-exec pgcon
             "DROP TABLE gauche_test.people;")
         (pg-result-status result)))

(test* "query exec"
       ;; create
       PGRES_COMMAND_OK
       (let1 result
           (pg-exec pgcon
             "drop schema gauche_test CASCADE;")
         (pg-result-status result)))



(test-section "Transaction")
(test* "before"
       PQTRANS_IDLE
       (pg-transaction-status pgcon))

(test* "during"
       PQTRANS_INTRANS
       (with-db-transaction* pgcon
         (pg-transaction-status pgcon)))
