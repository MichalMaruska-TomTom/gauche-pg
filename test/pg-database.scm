(use gauche.test)

(test-start "pg.database API features")

(use pg.db)
(use pg.database)

(test-module 'pg.database)

(test-section "Open connection")

(define pg-database
  (pg:connect-to-database
   :host (sys-getenv "PGHOST")
   :database "maruska"))

(test* "pg:connect-to-database"
       #t
       (is-a? pg-database
              <pg-database>))


(test* "Get attribute"
       #t
       (is-a?
        (pg:find-attribute pg-database
                           "public" "person" "cognome")
        <pg-attribute>))

(test-end :exit-on-failure #t)
