(use gauche.test)

(test-start "pg.hi API features")

(use pg.db) ;; <pg-database> will only gete me <pg>, but I need real conn
(use pg.hi)
(use pg-hi)
(use pg.types)
                                        ; (use pg-hi)
(use pg) ;; pg-exec

(test-module 'pg.hi)

(test-section "Prepare connection")

(define *database* "test")
;; make a search, turn into <recordset>, iterate?
(define pg-database
  ;;(with-env* "PGDATABASE" "maruska"
  (pg:connect-to-database
   :host (sys-getenv "PGHOST")
   :dbname *database*))

;; fixme: this is still higher: <pg>
(define pgconn (pg-conn-of (ref pg-database 'admin-handle)));; (->db pg-database)

(test* "Correct database connected"
       *database*
       (pg-db pgconn))

(test-section "Create data")

(test* "drop schema"
       #t
       (and
        (member
         ;; pg-status-status
         (pg-result-status (pg-exec pgconn "DROP SCHEMA gauche_test CASCADE;"))
         (list PGRES_COMMAND_OK  PGRES_NONFATAL_ERROR))
        #t))

(test* "create schema"
       PGRES_COMMAND_OK
       (pg-result-status (pg-exec pgconn "CREATE SCHEMA gauche_test;")))

(test* "create table"
       PGRES_COMMAND_OK
       (pg-result-status (pg-exec pgconn "CREATE TABLE gauche_test.people (name text, surname varchar, age int);")))



(test-section "Insert/update")

(test* "still same database"
       *database*
       (pg:with-private-handle* pg-database handle
         (pg-db (pg-conn-of handle))))


(pg:update-or-insert pg-database "gauche_test.people"
                     ;; wither exists:
                     `(("name" . ,((pg:printer-for "text") "Michal")))
                     ;; or insert:
                     `(("surname" . ,((pg:printer-for "text") "Maruska"))))


(test-end :exit-on-failure #t)
