
;; Simply create a relation in a given `namespace' with given  `attributes/types'

(define-module pg.recreate
  (export
   pg:create-table
   )
  (use pg)                              ;pg-exec
  (use pg.types)
  (use pg.database)
  (use pg.sql)
  (use adt.string)
  )
(select-module pg.recreate)



;;; CREATE TABLE ...  (... ..., ...);  out of an alist
(define (create-attributes-statement attributes-types)
  (string-join
      (map (lambda (att-info)
             (let ((attname (x->string (car att-info)))
                   (typname (x->string (cadr att-info)))
                   )
               (s+ (pg:name-printer attname)
                   " "
                   (pg:name-printer typname))))
          attributes-types)
      ", "))


;; imho  bad design:  fixme: should poke `db' as well
(define (pg:create-table db+nspname relname attributes)
  (let ((namespace #f)
        (db db+nspname))

    (when (pair? db+nspname)
      (set! db (car db+nspname))
      (set! namespace (cdr db+nspname)))
    (pg:with-private-handle* db conn
      (pg-exec conn
        (s+
         "CREATE TABLE "
         (if namespace
             (s+ (pg:name-printer namespace) "."
                 (pg:name-printer relname))
           (pg:name-printer relname))
         " " "("                        ;newline
         (create-attributes-statement attributes)
         ");")))))


(provide "pg/recreate")