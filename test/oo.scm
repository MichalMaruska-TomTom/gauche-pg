(use gauche.test)

(test-start "pg.oo API features")

(use pg.oo)
(test-module 'pg.oo)


(select-module pg.oo)
(use gauche.test)


;; <db-stored-class>


(test-section "define mapping from Gauche class to a PG relation")
;; <db-stored-class-info>
(define-class <foto> ()
  (
   ;; fixme: the only p-key for now:
   (fid :init-keyword :fid)
   ;(person :init-keyword :person)
   (numero :init-keyword :numero)
   (fcat   :init-keyword :fcat :init-value #f)
   (findex :init-keyword :findex)
   )
  )

(define pg-db (pg:connect-to-database :dbname "maruska"))
(define foto-raw-relname "foto_raw")
(define foto-relation (pg:find-relation pg-db foto-raw-relname))


(define oo-info (make <db-stored-class-info>
                  :object-class <foto>
                  :handle (pg:new-handle pg-db)
                  ;;:attribute-mapping  added below:
                  :db-relation foto-relation))

#;(slot-set! db-info 'attribute-mapping
  ;; append is not destroying ! fixme:
  (append `((width . ,(pg:lookup-attribute db-info relation "w"))
            (height . ,(pg:lookup-attribute db-info relation "h")))
slots))
(test* "Create <db-stored-class-info>"
       #t
       (is-a?
        oo-info
        <db-stored-class-info>))


(test* "Number of attribute-mapping|s "
       4
       ;; there are 2 mappings:
       ;; fid -> <pg-attribute fid>
       (length
        (ref oo-info 'attribute-mapping)))

;; double-check the `attribute-mapping' is calculated!
(test* "attribute-mapping"
       (let1 names '(fid numero fcat findex) ;; person
         ;; zip
         (map (lambda (name attribute)
                (cons name (x->string attribute)))
           names names))
       ;; there are 2 mappings:
       ;; fid -> <pg-attribute fid>
       (map
           (lambda (pair)
             (cons (car pair)
                   (pg:attribute-name (cdr pair))))
         (ref oo-info 'attribute-mapping)))

;; After defining the mapping, I can:

;; db-insert-object
(test-section "define mapping from Gauche class to a PG relation")

(define test-foto (make <foto>
                    :fid 659
                    :numero 1
                    ;; :findex 1
                    :fcat "p"))

(test* "db-where"
       "fid = 659"
       (and
        (is-a?
         oo-info
         <db-stored-class-info>)
        (db-where oo-info test-foto)))

(test* "find-full-intentification"
       '("fid")
       (map pg:attribute-name
         (find-full-intentification oo-info test-foto)))

(test* "db-delete"
       "DELETE FROM foto_raw WHERE fid = 659;"
       (db-delete oo-info test-foto)
       )

(define test-foto1 (make <foto>
                     :numero 1
                     :findex 1
                     :fcat "p"))

;; todo: try with test-foto2 which has both !


(test* "db-delete"
       "DELETE FROM foto_raw WHERE numero = 1 AND fcat = 'p' AND findex = 1;"
       (db-delete oo-info test-foto1))

#;(test* "db-update"
       #t
       (db-update oo-info test-foto1))

;; (set! debug #t)
(test* "db-insert"
       "INSERT INTO foto_raw(findex, fcat, numero)\n VALUES (\n1,\n'p',\n1\n)"
       (db-insert oo-info test-foto1)
       )


(test-end :exit-on-failure #t)
