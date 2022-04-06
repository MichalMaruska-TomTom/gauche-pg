
;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg-low API features")

(use pg)
(use adt.string)

(use pg-low)
;(load "pg-low")

(test-module 'pg-low)
;(select-module pg-low)

;;; Configuration
;(sys-setenv "PG_MMC" "y")


(define *PGPORT* (string->number (or (sys-getenv "PGPORT") "5432")))

(define *db-name* "test")
;;; Code:
(define pgcon (pg-connect (s+ "dbname=" *db-name*)))



(test* "connection params"
       (list *PGPORT* "michal" *db-name*)

       (list (string->number (pg-port pgcon))
             (pg-user pgcon)
             (pg-db pgcon)))

;; todo:
;; async connection.
; pg-connect-start
                                        ; pg-connect-poll

;; todo:
(test* "get the backend PID" #t
       (number? (pg-backend-pid pgcon)))

(test-section "PG: exec queries")

(test* "simple SELECT"
       ;; create
       PGRES_TUPLES_OK
       (let1 result (pg-exec pgcon "select 1;")
         (pg-result-status result)))

;; todo:  async exec.


;; todo: extract error.
;; pg-result-error-field


;; how to test pg-result->alist ?
;; I need  sql-insert-values for that!
;; or write the SQL here.
(test* "Command to drop whole schema"
       PGRES_COMMAND_OK
       (let1 result
           (pg-exec pgcon
             "drop schema gauche_test CASCADE;")
         (pg-result-status result)))

(test* "query Command Create"
       PGRES_COMMAND_OK
       (let1 result (pg-exec pgcon "CREATE SCHEMA gauche_test;")
         (pg-result-status result)))

(test* "query Create table"
       ;; create
       PGRES_COMMAND_OK
       (let1 result
           (pg-exec pgcon
             "CREATE TABLE gauche_test.people (name text, surname varchar, age int);")
         (pg-result-status result)))

(test* "INSERT"
       (list PGRES_COMMAND_OK "0")	;why 0?
       ;; not PGRES_TUPLES_OK  why not?
       (let1 result (pg-exec pgcon
                      "INSERT INTO gauche_test.people VALUES ('Michal', 'Maruska');")
         (list (pg-result-status result)
               (pg-oid-status result))))

(test-section "pg: access result -- low-level")

(define result (pg-exec pgcon "SELECT name, surname, age FROM gauche_test.people;"))

(test* "select"
       PGRES_TUPLES_OK
       (pg-result-status result))

; select oid from pg_class where relname="people" and namespace = ;

(test* "pg-ntuples"
       1
       (pg-ntuples result))

(test* "pg-nfields"
       3 (pg-nfields result))

(test* "pg-fname"
       "surname"
       (pg-fname result 1))

(test* "pg-fnumber"
       2
       (pg-fnumber result "age")
       )

(test* "pg-ftablecol"
       1
       (pg-ftablecol result 0)
       )

;; convert it into name!
(test* "pg-ftable"
       #f
       (pg-ftable result 0)		;
       )

(test* "pg-map-result"
       '("Maruska")
       (pg-map-result result '("name" "surname")
         (lambda (name surname)
           surname)))

(test-section "pg: access result -- HI-level")
;; I want to see the error thrown!
(test* "pg-attribute-indexes"
       '(0 1 2)
       (pg-attribute-indexes result '("name" surname 2)))


(define result2 (pg-exec pgcon "select name, surname FROM gauche_test.people;"))

(test* "pg-result->alist"
       '(("Michal" . "Maruska"))
       (pg-result->alist result2))


(test-section "pg: Explain")
(define res (pg-exec pgcon "explain select * from gauche_test.people where name = 'Michal';"))
(pg-dump-explain res)


(test-section "pg: other commands")

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
