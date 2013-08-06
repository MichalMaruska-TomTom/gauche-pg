(define-module pg.tempdrop
  (export
   ;; new power tool:
   pg:drop-constraint
   pg:save-constraints

   ;; Primary key (constraints)
   pg:drop-pkey-for
   pg:save-pkey-for
   ;; pg:release-pkey-for
   ;; pg:restore-pkey-for


   ;;
   pg:release-fk-for pg:release-fk-from
   pg:release-all-fk
   ;; and back!
                                        ;pg:restore-fk
   pg:restore-fk-for

   ;; indexes:
   pg:drop-index
   pg:save-index-definition
   pg:release-indexes-on                ;w/ saving!!
   pg:restore-indexes-on
   ;;
   pg:drop-trigggers-on
   ;;
   pg:save-unique-for pg:drop-unique-for
   ;; drop temporarily:
   ;; SELECT pg_get_indexdef(indexrelid),* from pg_index;
   ;; restores

   )
  (use pg)
  (use pg-hi)
  (use pg.database)
  (use pg.types)
  (use pg.sql)
  (use adt.string)
  )
(select-module pg.tempdrop)


(define-class <pg-constraint> ()
  ((name :init-keyword :name)
   ;; (type :init-keyword :name)

   ;; fixme: (namespace )
   (condeferrable)

   ;; relations:
   ;(master)

   ;(slave)
   ;; keys
   ))



;; note: <db-f-key> in pg.database
;; fixme!
;(define-class <pg-constraint:fk>)

;; todo: this is where polymorphism is needed!
;; just the name:
(define (pg:load-constraint pg oid)
  (let1 r (pg-exec (pg:new-handle pg)
            (sql:select
             '(
               conname contype
                       ;;
               conrelid                 ;contypid
               confrelid
               conkey
               confkey
               )
             "pg_constraint"
             (sql:alist->where
              `(("oid" . ,(number->string oid))))
             ))
    (car (pg-collect-result r "conname"))

    (let1 rs (make <recordset> :result r)
      (map
          (lambda (row)
            (if (string=? (row "contype") "f")
                (make <db-f-key>
                  :name (row "conname")
                  :master
                  :slave
                  m-
                  )
              rs
              ;; <pg-constraint>
              ))))))

;;;
;;; `Re-Create'
;;;
(define (pg:restore-constraint conn namespace relation type)
  ;(let* ((conn (pg:new-handle (ref relation 'database) :user "postgres"))
  (let* ((db (ref namespace 'database))
         (result (pg-exec conn
                   (sql:select
                    (list 'conname conrelid 'definition)
                    ;; from:
                    "admin.constraints"
                    ;; fixme!!! use oid!
                    (string-join-non-f
                        (list
                         (if relation
                             (s+ "conrelid = " (number->string (ref relation 'oid))) ;'relid
                           #f)

                         (s+ "contype = " (if (char? contype) (pg:char-printer contype)
                                            (pg:text-printer contype)))

                         (s+ "connamespace = " (number->string (ref namespace 'oid))))
                      "AND")
                                        ;(s+  "relname = " (pg:text-printer (ref relation 'name)))
                    ))))

    (pg-foreach-result result '("conname" "definition" conrelid)
      (lambda (conname definition conrelid)
        (pg-exec conn
          (s+ "ALTER TABLE ONLY " (pg:name-printer
                                   (if relation
                                       (ref relation 'name)
                                     (pg:get-relation-by-oid db conrelid)))
              "ADD CONSTRAINT " (pg:name-printer conname)
              ;; fixme: quotation?
              (pg:text-printer
               definition)))))))



;;constraint-save
;(define (pg:drop-fk-save db constraint))

;;  drop fk & record them
;;  truncate
;;  re-fill
;;  re-create fk.
;(d (pg:get-relation db "person"))



;; fixme:  namespace!!
(define (pg:drop-constraint handle relname conname)
  ;;(ref relation 'name)  relation
  (logformat "pg:drop-constraint: ~a on ~a\n" conname relname)
  (pg-exec handle ; (pg:new-handle (ref relation 'database) :user "postgres")
    ;; the name of the pkey constraint!!!!
    (s+ "ALTER TABLE ONLY " (pg:name-printer relname)
        ;; fixme:
        " DROP constraint " (pg:name-printer conname) ";")))



;; i want to suspend temporarily all references to the values in `relname'


;;; Fixme: I should delete previous stuff! in `admin.constraints' !

;;; put in a (almost-temp) table !
;; RELATION can be #f ! -> all relations FIXME! (in a namespace)
(define (pg:save-constraints conn relation namespace contype . conname?) ;fixme:  If I want just 1 specific `constraint'!
  (unless namespace
    (if relation
        (set! namespace (ref relation 'namespace))))
  (let1 do-it (lambda (conn)
                (pg-exec conn           ; (pg:new-handle (ref relation 'database) :user "postgres")
                  (sql:insert-into
                   (cons "admin.constraints"
                         "(relname, definition, conname, contype)") ; fixme: should be  relname/namspece !!

                   (sql:select
                    ;; mmc: To restore it, we do:
                    ;; SQL:   ALTER TABLE `relname' ADD CONSTRAINT `conname'  f(definition, contype);
                    '("a.relname" "pg_get_constraintdef(X.oid)" "conname" "contype")
                    ;; `From:'
                    "pg_constraint X join pg_class A on (conrelid = A.oid)"
                    ;; Where
                    (string-join-non-f
                        (list
                         (if (null? conname?)
                             #f
                           (s+ "conname = " (pg:char-printer conname)))
                         (if relation
                             (s+ "conrelid = " (number->string (ref relation 'oid))) ;'relid
                           #f)

                         (s+ "contype = " (if (char? contype) (pg:char-printer contype)
                                            (pg:text-printer contype)))

                         (s+ "connamespace = " (number->string (ref namespace 'oid))))
                      "AND")
                    ";"))))
    (if conn
        (do-it conn)
      ;; fixme: this migh err.
      (pg:with-private-handle (ref relation 'database)
        do-it))))





;;; `bulk'    putting aside to       `admin.constraints'
(define (pg:release-all-constraints relation namespace type)
  (if (and relation (not namespace))
      (set! namespace (ref relation 'oid)))
  (let ((database
         (if namespace
             (ref namespace 'database)
           (ref relation 'database))))

    (pg:with-private-handle database
      (lambda (conn)
        ;;(insert into admin.constraints (relname name definition) values )
        (pg:save-constraints conn relation namespace type)

        ;; Dropping cannot be done in a massive query. ONly 1 table at a time.
        ;; So `pg:drop-constraint' is so simple on its arguments!
        ;; But here we need to get the entire list, and point it at them...
        (let1 result
            (pg-exec conn ;; so that I can TRUNCATE it.
              (sql:select
               '(conname "a.relname as slave")
               ;; FROM:
               "pg_constraint X join pg_class A on (conrelid = A.oid)   join pg_class B on (confrelid = B.oid)"
               ;; confrelid = ~d AND  relid
               (string-join-non-f
                   (list
                    (format #f "contype = '~a' " type)
                    (if relation
                        (format #f "conrelid = ~d" (ref relation 'oid))
                      #f)
                    (format #f "connamespace = ~d" (ref namespace 'oid)))
                 " AND ")))

          (pg-foreach-result result #f
            (lambda (name slave-relname)
              (pg:drop-constraint conn slave-relname ;fixme!  namespace or (pg:get-relation db slave-relname)
                                  name))))
        ;; Truncate
        ;;(pg-exec conn (string-append "TRUNCATE " relname))
        )
      :user "postgres")))


;;;
;;; Specific types
;;;

;;; 1/ `Foreign' keys
;(define (pg:release-all-fk namespace)
;  (pg:release-all-constraints namespace #\f))

(define pg:release-all-fk (cute pg:release-all-constraints <> <> #\f))
;; This is the same, but only for 1 relation!
(define (pg:release-fk-for relation)
  (pg:release-all-constraints relation #f \#f))


;; relname can be a `nsp.relname' & `TRUNCATE'!!
;;fixme:  just 1 argument!!
;; todo: I could to -from-to ?
(define (pg:release-fk-from db relation namespace) ;the relname should be in (nspname relname) form??
  ;(error "not implemented!")
  ;; (let ((namespace (namespace->oid db nspname)))
  (let ((conn-type #\f))
    (pg:with-private-handle database
      (lambda (conn)
        (let1 relid (slot-ref relation 'oid)
          ;; i want to suspend temporarily all references to the values in `relname'
          (let1 result (pg-exec conn ;; so that i can TRUNCATE it.
                         (sql:select
                          '(conname
                            "a.relname as slave"
                            "pg_get_constraintdef(X.oid)")

                          "pg_constraint X join pg_class A on (conrelid = A.oid)   join pg_class B on (confrelid = B.oid)"
                          ;; Where:
                          (format #f "confrelid = ~d AND  contype = '~a' AND connamespace = ~d" relid conn-type namespace)
                          ))
            (pg-foreach-result result #f
              (lambda (name slave-relname def) ;; fixme: take the namespace as well!
                                        ;(insert into admin.constraints (relname name definition) values )
                (let1 relation (pg:get-relation-by-oid db slave-relname)
                (pg:save-constraints conn
                                     relation
                                     (ref relation 'namespace)
                                     conn-type name)
                (pg-exec conn
                  (string-append
                   "ALTER TABLE ONLY "
                   (pg:name-printer (ref relation 'name)) ;fixme: this needs namespace!
                   " DROP CONSTRAINT "
                   (pg:name-printer name)))))))))
      :user "postgres")))



;;; 1/ `Primary' keys
(define (pg:release-pkey-for relation)
  (if (ref relation 'p-key-name)
      (begin
        (pg:save-constraints relation  "p")
        ;; fixme: why do I keep the `p-key-name'?
        (pg:drop-constraint relation (ref relation 'p-key-name)) ;fixme: is the `name' primary key?
        ;; hook?
        (slot-set! relation 'p-key-name #f))
    (warn "pg:drop-pkey-for should be called only when the relation has a primary key. ~a does not have!\n"
          ;; fixme:  name-of
          (slot-ref relation 'name))))


;;; `unique'
(define pg:release-unique    ;all of them!
  (cute pg:save-constraints <> <> #\u))



;;
;; pg:restore-all-fk
;;; `TRIGGERS:'
(define (pg:drop-trigggers-on db relname)
  (error "unimplemented")
  (let* ((relation (if (string? relname)
                       (pg:get-relation db relname)
                     relname))
         ;(namespace (namespace->oid db nspname)) ;
         (conn (pg:new-handle db :user "postgres"))
         (relname (ref relation 'name)))

    (let1 relid (slot-ref relation 'oid)
      ;; i want to suspend temporarily all references to the values in `relname'
      ;;(pg:save-constraints relation "f")

      (let1 result (pg-exec conn ;; so that i can TRUNCATE it.
                     (sql:select
                      '(tgname
                        ;; "a.relname as slave"
                        ;;"pg_get_constraintdef(X.oid)"
                        )
                      "pg_trigger" ; join pg_class A on (conrelid = A.oid)

                      (format #f "tgisconstraint  = 'f' AND tgrelid = ~d"  relid) ;namespace
                                        ;((contype "f")
                                        ; (connamespace  2200))
                      ))
        (pg-foreach-result result #f
          (lambda (name)
                                        ;(insert into admin.constraints (relname name definition) values )
            (logformat "dropping trigger ~a on ~a\n" name relname)
            (pg-exec conn
              (string-append
               "DROP trigger "
               (pg:name-printer name)
               " on "              ; ONLY
               (pg:name-printer relname)
               )))))
      ;; Truncate
                                        ;(pg-exec conn (string-append "TRUNCATE " relname))
      )))



;;;
;;;
;;; todo: an `index' object
;;;
(define (pg:save-index-definition relation index-name)
  (error "unimplemented")
  (pg:with-private-handle (ref relation 'database)

    (lambda (conn)
      (pg-exec conn
        (sql:insert-into
         (cons "admin.indexes"
               "(indexname, relname, definition)")
         (sql:select
          (list
           (pg:text-printer index-name)
           (pg:text-printer (ref relation 'name))
           "pg_get_indexdef(indexrelid)")
          ;; fixme: we don't have the  relid of the index. !!! if we had an index object ...
          "pg_index i join pg_class c on (c.oid = indexrelid) "
          (s+ "relname  = " (pg:text-printer index-name))))))
    :user "postgres"))



(define (pg:drop-index db index-name)   ; handle . verbose?
  (logformat "dropping index ~a\n" index-name)
  (pg:with-private-handle (ref relation 'database)
    (lambda (conn)
      (pg-exec conn
        (s+ "DROP INDEX " (pg:name-printer index-name) ";")))
    :user "postgres"))


;; fixme: with one fixed pg handle?
(define (pg:release-indexes-on relation)
  (for-each
      (lambda (index)
        (pg:save-index-definition relation index)
        (pg:drop-index (ref relation 'database) index))
    (pg:relation->indexes relation)))


;; fixme: i should store oids!!!!
(define (pg:restore-indexes-on relation)
  (pg:with-private-handle (ref relation 'database)
    (lambda (conn)
      (let1 result (pg-exec conn
                     (sql:select
                      '(definition oid)
                      "admin.indexes"
                      (s+ "relname = " (pg:text-printer (ref relation 'name)))))
        (pg-foreach-result result '("definition" "oid")
          (lambda (definition oid)
            (pg-exec conn definition)
            (pg-exec conn (format #f "delete from admin.indexes where oid = ~d;" oid))
            ;; remove the backup-ed definition?
            ))))
    :user "postgres"))


(provide "pg/tempdrop")

