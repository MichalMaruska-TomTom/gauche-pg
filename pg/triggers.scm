
(define-module pg.triggers
  (export
   pg:relation-triggers
   pg:trigger-definition
   <pg-trigger>)

  (use pg.sql)
  (use pg-hi)

  ;;(use pg.base)
  ;; (use pg.db)

  (use pg.database)

  (use pg.types)
  (use adt.string)
  (use pg.sql)

  (use macros.assert);; ->
  (use mmc.log)
  (use mmc.simple)
  )
(select-module pg.triggers)

;; `pg_get_viewdef'
;;

;; it's also a <pg-result> w/  <pg-tuples> ...
(define-class <pg-trigger> ()
  (
   (relation :init-keyword :relation)

   (oid :init-keyword :oid)
   (name :init-keyword :name)
   (function :init-keyword :function)
   ;; type
   ;; deffered
   ;; args
   ))

;; Gets names or oid?
(define (pg:relation-triggers relation)
  (pg:with-admin-handle (ref relation 'database)
    (lambda (pg)
      (let1 r (pg-exec pg
                (sql:select
                 '("t.oid" "t.tgname" "t.tgfoid"
                   ;; `constraints'
                   ;; "tgconstrname"
                   ;; tgconstrrelid ... RI
                   ;;
                   "tgdeferrable" "tginitdeferred"
                   )
                 "pg_trigger t join pg_proc p on (p.oid = t.tgfoid)"
                 (s+ "tgrelid = " (pg:number-printer (ref relation 'oid))
                     " AND "
                     "pronamespace = " (pg:number-printer (ref (ref relation 'namespace) 'oid)))))

        (map-numbers* i 0 (pg-ntuples r)
          (make <pg-trigger>
            :relation relation
            :oid (pg-get-value r i 0)
            :name (pg-get-value r i 1)
            :function (pg-get-value r i 2)
            ))))))



(define (pg:trigger-definition trigger)
  (assert (is-a? trigger <pg-trigger>))

  (pg:with-handle-of* trigger h
    (let1 r
        (pg-exec h
          (sql:select-function "pg_get_triggerdef" (pg:number-printer (ref trigger 'oid))))
      (pg-get-value r 0 0))))


(define-method pg:with-handle-of ((trigger <pg-trigger>) function)
  (pg:with-private-handle (slot-ref (slot-ref trigger 'relation) 'database)
    function))


(provide "pg/triggers")
