(define-module pg.keys
  (export 
   pg:pkey-of

   <db-f-key>
   pg:load-foreign-keys
   pg:fkey-between
   pg:fkey-under
   pg:fkey-above
   )
  (use pg)
  (use pg-hi)
  (use pg.types)
  (use pg.sql)


  (use macros.assert)
  (use srfi-1)
  (use pg.base)
  (use pg.db)
  (use pg.relation)

  (use adt.list)
  (use adt.string)
  )
(select-module pg.keys)
;;; Keys

(define (pg:pkey-of relation)
  (ref relation 'p-key))


(define-class <db-f-key> ()
  ((name :init-keyword :name)
   (master :init-keyword :master)
   (slave :init-keyword :slave)
   (m-fields :init-keyword :m-fields)
   (s-fields :init-keyword :s-fields)))

(define-method write-object ((fkey <db-f-key>) port)
  (format port
    "<fkey ~a: ~a/~a -> ~a/~a>"
    (ref fkey 'name)
    (ref fkey 'slave)
    (ref fkey 's-fields)
    (ref fkey 'master)
    (ref fkey 'm-fields)))

;; fixme:  memoize!
;; list!
(define (pg:load-foreign-keys namespace)       ;namespace
  ;;(warn "fixme! pg:load-foreign-keys should consider connamespace\n")
  (pg:with-admin-handle (ref namespace 'database)
    ;(pg:with-handle-of* namespace h
    (lambda (h)
      (let1 result (pg-exec h
                     (sql:select
                      '(conname "a.relname as slave" "b.relname as master" conkey confkey)

                      "pg_constraint join pg_class A on (conrelid = A.oid)   join pg_class B on (confrelid = B.oid)"

                      (s+ "contype = 'f' AND connamespace = " (pg:number-printer (ref namespace 'oid)))))
        ;; fold !
        (let1 f-keys '()
          (pg-foreach-result result #f
            (lambda (name slave master sl-key ma-key)
              (push! f-keys
                     (make <db-f-key>
                       :name name
                       :master (pg:get-relation namespace master) ; pg:get-relation db
                       :slave (pg:get-relation namespace slave)   ;pg:get-relation db
                       ;; fixme:  These should be converted from attnum to  <pg-attribute> !?
                       :m-fields ma-key
                       :s-fields sl-key))
                                        ;(logformat "~a ~a\n" name sl-key)
                                        ;attributes function
              ))
          (slot-set! (ref namespace 'database) 'foreign-keys f-keys)
          f-keys)))))

(define (pg:fkey-between master slave)
  (let ((db (ref master 'database))     ;fixme!
        (oid (ref master 'oid))
        (oid1 (ref slave 'oid)))

    (assert (eq? (ref master 'namespace)
                 (ref slave 'namespace)))
    (let1 fkeys
        (filter
            (lambda (fkey)
              (and (eq? (ref fkey 'master) master)
                   (eq? (ref fkey 'slave) slave)))
          (pg:load-foreign-keys (ref master 'namespace))) ;fixme!(pg:get-namespace db "public")

      (cond
       ((null? fkeys)
        #f)
       ((singleton? fkeys)
        ;; fixme: if more than 1?
        (car fkeys))
       (else
        (errorf "pg:fkey-between: more fkeys between relations ~a ~a, dunno which select" master slave))))))

(define (pg:fkey-under master)
  (let ((db (ref master 'database))     ;fixme!
        (oid (ref master 'oid)))

    (filter
        (lambda (fkey)
          (and (eq? (ref fkey 'master) master)))
      (pg:load-foreign-keys (ref master 'namespace)))))


(define (pg:fkey-above relation)
  (let ((db (ref relation 'database))     ;fixme!
        (oid (ref relation 'oid)))
    (filter
        (lambda (fkey)
          (and (eq? (ref fkey 'slave) relation)))
      (pg:load-foreign-keys (ref relation 'namespace)))))



(provide "pg/keys")
