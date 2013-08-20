
;;;  Intended as access point for applications.

;;; Concentrates all the apis/modules to mirror objects in DB

;; Todo:  pg:db-admin-handle
;;        given under a mutex.
(define-module pg.database
  (extend pg.base
          pg.db
          pg.namespace
          pg.attribute
          pg.relation
          ;;pg.order
          pg.actions
          pg.keys)

  (export
   ;;; Tree of data:

   ;;  p-key   list of indexes
   ;;  attributes hash   name -> index
   ;;  types   vector/index ->  name & type
   ;;
   ;; type-oid-of-attribute
   pg:find-attribute

   ;; `<db-index>'
   pg:relation->indexes
   )

  ;; todo: I could make this conditional?
  (use gauche.threads)


  (use macros.assert)
  (use macros.aif)

  (use mmc.log)
  (use mmc.simple)
  (use mmc.throw)

  (use pg-hi)                           ;->db
  (use pg-low)
  (use pg.types)
  (use pg.sql)

  (use adt.vector)
  (use adt.alist)
  (use adt.list)
  (use adt.string)
  (use gauche.sequence)
  (use srfi-13)
  (use srfi-1)
  (use util.list)
  )
(select-module pg.database)

;; fixme:
;; (pg:add-database-hook 'namespaces pg:load-namespaces!)
(define debug #f)

(define (pg:find-attribute pg nm rel att)
  ;; error on #f!
  (or (and-let*
        ((namespace (pg:get-namespace pg nm))
         (relation (pg:get-relation namespace rel)) ;
         (attribute (pg:attname->attribute relation att)))
        attribute)
      (error "Cannot find attribute" nm rel att)))
;; 'relations  -> hash   name-> object

(define-class <db-index> ()
  ((name :init-keyword :name)
   (oid :init-keyword :oid)             ;the relation oid of the index
   ;; indexrelid^^
   ;; indrelid vvv
   (relation :init-keyword :relation)
   ;;
   ;; (fields :init-keyword :m-fields)
   ))

(define (pg:relation->indexes relation)
  (pg:with-admin-handle (ref relation 'database)
    (lambda (handle)
      ;; fixme: must have read access to the pg_index !!
      (let* ((query (sql:select
                     '("c.relname as indexname"
                       ;;indexrelid
                       ;; indkey is vector!!
                       )
                     ;; from:
                     "pg_index i JOIN pg_class c on (c.oid = indexrelid)"
                     (format #f "indrelid = ~d" (ref relation 'oid))
                     ))
             (result (pg-exec handle query)))
        (pg-collect-result result "indexname")))))

;;; unique:
'(define-class <db-unique> ()
  ((name :init-keyword :name)
   ;; relation
   (attributes :init-keyword :attributes)
   ))

; fixme!
;(define-method ->db ((pg <pg>))
;  (slot-ref pg 'database))

(provide "pg/database")
