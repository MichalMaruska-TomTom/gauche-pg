(use gauche.test)

(test-start "pg/relation API features")

(use pg)
(use pg.db)
(use pg.relation)
(use pg.namespace)

(test-module 'pg.relation)

;;
(define pg-database (pg:connect-to-database :dbname "maruska"))

(define namespace (pg:get-namespace pg-database "public"))

(test-section "Get them all")

(test* "pg:namespace-relations"
       #t
       (begin
         (for-each (cute is-a? <> <pg-relation>)
           (pg:namespace-relations namespace))
         #t))

(test* "pg:database-relations"
       #t
       (begin
         (for-each
             (cute is-a? <> <pg-relation>)
           (pg:database-relations pg-database))
         #t))

(test-section "Get one of them")

(test* "pg:get-relation"
       #t
       (is-a?
        (pg:get-relation namespace "person")
         <pg-relation>)
       )


(test* "pg:find-relation"
       #t
       (is-a?
        (pg:find-relation pg-database "person")
        <pg-relation>))

(test* "pg:find-relation"
       #t
       (is-a?
        (pg:find-relation pg-database "person" namespace)
        <pg-relation>))

;; pg:get-relation-by-oid

(test-section "Associate data")

(let ((my-data '(1 2 3))
      (tag 'mydata))
  (test* "pg:relation-put!"
         my-data
         (let1 relation (pg:find-relation pg-database "person" namespace)
           (pg:relation-put! relation tag my-data)
           (pg:relation-get relation tag "wrong-default")
           )))



(define relation-name "person")

(test-section (format #f "Get data about attributes of relation ~a" relation-name))
(define relation-person (pg:find-relation pg-database relation-name))

(test* "pg:primary-key-of"
       (list "numero")
       (let1 key-attributes
           (pg:primary-key-of (pg:find-relation pg-database relation-name))
         (map (cut ref <> 'attname)

           key-attributes)))


(test* "pg:attname->attnum"
       3
       (pg:attname->attnum relation-person "nome")
       )

(test* "pg:attname->attribute"
       "via"
       (ref
        (pg:attname->attribute relation-person "via")
        'attname))


(test-end :exit-on-failure #t)
