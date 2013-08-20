

(define-module pg.namespace
  (export
   ;; `Schemas:'
   pg:load-namespaces!                  ;no need to use explicitely!
   <pg-namespace>

   ;; name -> object -> oid
   pg:nspname->namespace
   pg:get-namespace

   pg:namespace->oid
   pg:oid->namespace

   ;; content:
   pg:namespace-viewnames
   pg:namespace-relnames
   pg:find-namespace
   ;; obsolete name:
   pg:extract-namespace


   ;; utils:
   pg:create-namespace
   pg:assure-namespace
   )
  (use pg)
  (use pg-hi)
  (use pg.types)
  (use pg.sql)

  (use pg.base)
  (use pg.db)

  (use gauche.threads)
  (use mmc.threads)
  (use adt.string)

  (use macros.assert)
  (use srfi-13)                         ;prefix
  (use mmc.log)
  )
(select-module pg.namespace)

(define debug #f)

;;; schema
(define-class <pg-namespace> ()
  (;; back
   (database :init-keyword :database)
   ;;
   (name :init-keyword :name)
   (oid :init-keyword :oid)
   ;; Sure not a function?
   (relation-mutex :init-form (make-mutex))

   ;; why not just the keys of the hash?
   (relnames)
   (relations :init-form (make-hash-table 'string=?))))

(define-method write-object ((n <pg-namespace>) port)
  (format port
    "<~a ~a/~a>"
    ;; "<"
    (string-drop-right (string-drop (symbol->string (class-name (class-of n))) 1) 1)
    ;"pg-database"
    (ref n 'name)
    (ref n 'oid)))

(define-method pg:with-handle-of ((ns <pg-namespace>) function)
  (pg:with-private-handle (slot-ref ns 'database)
    function))


;; to use a name & object interchangeably, this the conversion:
(define (normalize-namespace db ns)
  (if (is-a? ns <pg-namespace>)
      ns
    (pg:nspname->namespace db ns)))

;; `by-name'
(define (pg:find-namespace db :optional (namespace "public"))
  (normalize-namespace db namespace))
(define pg:extract-namespace pg:find-namespace)

;; given NAME return `<pg-namespace>' object
(define (pg:nspname->namespace db nspname)
  (with-locking-mutex* (ref db 'namespaces-mutex)
    (or
     (find (lambda (ns)
             ;; fixme: mutex?  either we get the list of schemas immediately, or
             ;;        we need a mutex!
             (string=? nspname (ref ns 'name)))
           (ref db 'namespaces))
     (errorf "cannot find namespace ~a for DB ~a" nspname db))))

(define pg:get-namespace pg:nspname->namespace)


;; returns just the oid
(define (pg:namespace->oid db nspname)
  ;; todo: this constructs & discards. Could be simpler!
  (ref (pg:nspname->namespace db nspname) 'oid))


(define (pg:oid->namespace db oid)
  (DB "pg:oid->namespace: ~d\n")
  (with-locking-mutex* (ref db 'namespaces-mutex)
    (or
     (find (lambda (ns)
             (= oid (ref ns 'oid)))
           (ref db 'namespaces))
     (errorf "cannot find namespace ~a for DB ~a" nspname db))))

;;   Database         hash   -+
;;                           /
;;   Schema/namespace       /
;;      |    ....  \       /
;;   relation     relation/



;;; sets the 'namespaces slot of DB
;;; to an alist (nspname  <pg-namespace>)
;; note: can be used to refresh too!
(define (pg:load-namespaces! db)
  (DB "pg:load-namespaces!\n")
  (if (slot-bound? db 'namespaces)
      (error "pg:load-namespaces! must not be run twice!"))
  (pg:with-admin-handle db
    (lambda (h)
      ;; This waits for Readers to finish:
      (with-locking-mutex* (ref db 'namespaces-mutex)
        (slot-set! db 'namespaces
          (pg-map-result (pg-exec h
                           (sql:select '(nspname oid) "pg_namespace"))
              '("nspname" "oid")
            (lambda (name oid)
              (make <pg-namespace>
                :database db
                :name name
                :oid oid))))))))

;; Trick:
(pg:add-database-hook 'namespaces pg:load-namespaces!)

(define (namespace-relnames-locked h namespace type)
  ;; mutex held!
  (assert (eq? (current-thread)
               (mutex-state (ref namespace 'relation-mutex))))
                                        ;(unless (and (slot-bound? namespace 'relnames)
                                        ;             (not reload?))
  (let1 result
      (pg-exec h
        (sql:select
         '("relname")

         "pg_class"
         (string-append
          "relkind = " (pg:text-printer type) " AND "
          (s+ "relnamespace = " (pg:number-printer (ref namespace 'oid)))
          ;; old:
          ;; "pg_class B join pg_namespace A on (A.oid = B.relnamespace)"
          ;; (string-append "nspname = " (pg:text-printer (ref namespace 'name)))
          )))
    (pg-collect-result result "relname")))



(define (pg:namespace-relnames namespace . reload?)
  (pg:with-admin-handle (ref namespace 'database)
    (lambda (h)
      (with-locking-mutex* (ref namespace 'relation-mutex)
        (if (and (null? reload?)
                 (slot-bound? namespace 'relnames))
            (ref namespace 'relnames)
          (let1 val (namespace-relnames-locked h namespace "r")
            (slot-set! namespace 'relnames
              ;; todo: here we don't need the mutexes.
              val)
            val ;;(ref namespace 'relnames)
            ))))))


(define (pg:namespace-viewnames namespace . reload?) ;fixme: always reloads!
  (pg:with-admin-handle (ref namespace 'database)
    (lambda (h)
      ;; note: we reuse the relation mutex!
      (with-locking-mutex* (ref namespace 'relation-mutex)
        (namespace-relnames-locked h namespace "v")))))


;; todo:
;; create table in namespace

;;; utils:
(define (pg:create-namespace db nspname)
  ;; todo: return the oid &update `DB!'
  ;; fixme: make it generic!
  (pg-exec (pg:new-handle db :user "postgres")
    (format #f "CREATE SCHEMA ~a;" nspname)))

(define (pg:assure-namespace db nspname)
  (or (pg:nspname->namespace db nspname)
      (pg:create-namespace db nspname)))


(provide "pg/namespace")

