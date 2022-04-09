
;; sequences & converting strings or objects into  objects.

(define-module pg.hi
  (export
   pg:update-or-insert
   ;; sequences
   pg:get-from-sequence
   pg:currval
   ;; normalizing  Attribute
   pg:normalize-relation                ; fixme: should go into pg.database !
   pg:normalize-attribute
   pg:lookup-attribute
   )

  (use pg.sql)
  (use pg.types)

  (use pg-hi)
  (use pg.db)
  ;;(use pg.database)
  ;; or single:
  (use pg.relation)
  (use pg.attribute)

  (use macros.assert)
  (use adt.string)
  (use mmc.check)
  (use mmc.log)
  )
(select-module pg.hi)

(define debug #t)
;; @values is alist ((attribute value) ...)
;; @where-values same
;; pg is ... <pg-database> ?
;; @relation is updated, or values & where-values are inserted as 1 row.
(define (pg:update-or-insert pg relation where-values values)
  (pg:with-private-handle* pg handle
    (with-db-transaction* handle
      (let1 result
          (pg-exec handle
            (sql:select
             '("1")
             relation
             (sql:alist->where where-values)))
        (cond
         ;; fixme: I need it to fail if unique contstraing, rather than waiting!
         ((zero? (pg-ntuples result))
          ;; insert
          (let1 all (append where-values values)
            (pg-exec handle
              (sql:insert-alist relation all))))

         ((= 1 (pg-ntuples result))
          ;; update
          (pg-exec handle
            (sql:update relation
                        values
                        (sql:alist->where
                         where-values))))
         (else
          (error "pg:update-or-insert ?")
          ))))))


;; This should be automatic!
;; rename to pg:nextval
(define (pg:get-from-sequence handle name)
  ;; fixme!  some code depends on old api?
  ;; collect
  (pg-get-value
   (pg-exec
       handle
     (sql:select-function "nextval" (s+ (pg:text-printer name)
                                        ;; fixme: needed?
                                        "::text")))
   0 0))

;; Get the last (already) assigned seq value:
(define (pg:currval pg sequence-name)
  (let1 res (pg-exec pg
              (sql:select-function "currval" (s+ (pg:text-printer sequence-name))))
    (pg-get-value res 0 0)))


;;;  normalizing  Attribute
(define (pg:normalize-relation db id)
  ;; todo:  `pg:->relation'
  (cond
   ((is-a? id <pg-relation>)
    id)
   ((string? id)
    (pg:find-relation db id))
   ((number? id)
    (pg:get-relation-by-oid db id))
   (else
    (error "cannot deduce a pg-relation out of " id))))

(define pg:get-releation pg:normalize-relation)


;; todo: `pg:->attribute'
(define (pg:normalize-attribute relation att)
  (DB "pg:normalize-attribute ~a ~a\n" relation att)
  (check-parameter-type relation <pg-relation>)

  (cond
   ((is-a? att <pg-attribute>)
    att)
   ((string? att)
    (or (pg:attname->attribute relation att)
        ;;(unless (hash-table-exists? (slot-ref relation 'attributes-hash) att)
        (errorf "relation ~a does not have attribute named ~a" relation att)))
   ((number? att)
    (pg:nth-attribute relation att))
   (else
    (error "cannot pg:normalize-attribute" att))))


;; combination of previous 2:
;; return <pg-attribute>
(define (pg:lookup-attribute db relation attribute)
  (let1 relation (pg:normalize-relation db relation)
    (DB "pg:lookup-attribute ~a , ~a\n" relation attribute)
    (pg:normalize-attribute relation attribute)))

(provide "pg/hi")
