
;; `database' concept.
;;

(define-module pg.db
  (export
   <pg-database>
   pg:connect-to-database ; (pg:connect-to-database "linux10" "maruska")

   pg:new-handle pg:dispose-handle

   ;; todo: rename to `pg:with-handle' !!
   pg:with-private-handle pg:with-private-handle*
   pg:with-admin-handle


   ;; Associating `free' info:
   pg:database-set! pg:database-ref
   pg:add-database-hook



   ;; todo: How to use these?
   pg:add-listener pg:check-for-notifies

   ;; should be in pg-hi!
   pg-type-name
   )
  (use pg)
  (use pg-hi)
  (use pg.base)
  (use mmc.common-generics)

  (use mmc.log)
  (use macros.assert)

  (use gauche.net)
  (use gauche.threads)
  (use mmc.threads)

  (use srfi-1)
  (use util.list)                       ;cond-list
  (use srfi-13)                         ;prefix
  )
(select-module pg.db)

(define debug-handles #f)
(define debug #f)

;;;  `database
(define-class <pg-database> ()
  (
   (name :init-keyword :name
         :getter name-of)
   (host :init-keyword :host)
   (port :init-keyword :port)
   ;; host port name  determine this object.

   (listeners :init-value ())
   (listener-mutex :init-value (make-mutex))

   ;; This mutex is for opening/distributing new handles.
   (conn-pool-mutex :init-form (make-mutex))
   (conn-pool
    :init-keyword :conn-pool
    :init-value '())

   ;;todo:  (search-path) ?? handle-local!

   ;;
   (admin-handle :init-keyword :admin-handle)
   (admin-handle-mutex :init-form (make-mutex))

   ;; hashes:
   ;; name -> oid!
   ;; and a simple array ??
   ;; i need oid -> <pg-namespace>
   ;; and name -> <pg-namespace>
   (namespaces)          ;; schemas   list of <pg-namespace>
   (namespaces-mutex :init-form (make-mutex))

   ;; oid -> relation ?
   ;; fixme: This should be accessible via  PUBLIC namespace !?
   (relations :init-keyword :relations)

   (foreign-keys)
   ;; types should be handled at the level of a connection! ??? why?
   ;; (oid -> typename) hash & reverse.
   ;; maybe not?
   ;;
   (dependency-hypergraph)

   (dictionary :init-keyword :dictionary) ; key -> value
   (dictionary-mutex :init-value (make-mutex))))


(define-method write-object ((o <pg-database>) port)
  (format port
    "<~a ~a@~a>"
    ;; "<"
    (string-drop-right
     (string-drop (symbol->string (class-name (class-of o))) 1)
     1)
    ;"pg-database"
    (ref o 'name)
    (ref o 'host)))


;; Do we trust that it is in suitable state?   -- use pg-reset (todo!)
(define (pg:dispose-handle database connection)
  (with-locking-mutex* (ref database 'conn-pool-mutex)
    ;; fixme: Check that it is connected to the correct DB!
    ;; I could have a weak vector of handed-out connections,
    ;; and ask for membership ?

    (if debug-handles
        (logformat-color 'yellow "disposing connection: ~d\n" (pg-backend-pid (ref connection 'conn))))
    ;; What if it is already in?
    (assert (not (member connection (slot-ref database 'conn-pool))))
    ;; Note: This starts a new backend!
    ;;(pg-reset (ref connection 'conn))   ;error!
    (slot-push! database 'conn-pool connection)))


;; returns a pg-handle.
;; The handle is either taken from a pool of
;; already connected handles, or freshly opened.  the handle can be
;; private ....  not private is useful for 1 (independent) query only.
(define (pg:new-handle database . rest)
  ;; Check args:
  (unless (is-a? database <pg-database>)
    (errorf "pg:new-handle: wrong argument type: is not a database ~s" database))

  (let-keywords* rest
      ((user #f)
       (really-new? #f)
       (private #f))                    ;remains in the pool.

    (if debug-handles
            (logformat-color 'yellow "pg:new-handle: looking at mutex\n"))
    (with-locking-mutex (ref database 'conn-pool-mutex)
      (lambda ()
        (if debug-handles
            (logformat-color 'yellow "pg:new-handle: mutex acquired\n"))

        ;; Default is given by ENV, but we change root->mmc!
        (when (and (not user)
                   (string=? (sys-uid->user-name (sys-getuid)) "root")
                   (not (sys-getenv "PGUSER")))
          (set! user "mmc"))



        (let1 connection
	    (if (or (null? (ref database 'conn-pool))
		    ;; fixme!!
		    really-new?
		    user)
		;; The high level! `<pg>'
		(let1 conn (if (or user
                                        ;(slot-bound? database 'name)
				   )
			       (pg-open :host (ref database 'host)
					;; Fixme:  port
					:dbname (ref database 'name)
					:user (or user
                                        ;(ref database 'user)
						  ))
			     (pg-open :host (ref database 'host)
				      :dbname (ref database 'name)))
		  (if (or user debug)
		      (logformat "pg-open as ~a\n" user))

		  (if debug-handles
		      (logformat-color 'red "Another connection opened: ~d\n"
			(pg-backend-pid (ref conn 'conn))))

		  (slot-set! conn 'database database)
		  ;; fixme:  slot-push! ?
		  (slot-push! database 'conn-pool conn)
		  ;;(slot-set! database 'conn-pool (list conn))
		  (if debug-handles
		      (logformat-color 'yellow "pg:new-handle: backend ~d\n"
			(pg-backend-pid
			 (ref conn 'conn))))
		  conn)
	      (car (ref database 'conn-pool)))

          (if private
              ;; fixme: it's at the head, isn't it?
              (slot-set! database 'conn-pool
                (delete! connection (ref database 'conn-pool))))
          connection)))))



;; fixme: this should accept keywords!
(define (pg:with-private-handle pg fun . rest)

  (let1 h #f
    (dynamic-wind
	(lambda ()
	  (if debug (logformat "pg:with-private-handle get private!\n"))
	  (set! h (apply pg:new-handle pg :private #t rest)))

	(lambda ()
	  ;; fixme: this does not work w/ continuations!
	  ;; todo:  `wind'
	  ;; fixme:  with more values!!!
	  (fun h))
	(lambda ()
	  (if debug (logformat "pg:with-private-handle dispose!\n"))
	  (pg:dispose-handle pg h)
	  ;;result
	  ))))

;; Macros first?
(define-syntax pg:with-private-handle*
  (syntax-rules ()
    ((_ pg name body ...)
     (pg:with-private-handle pg
       (lambda (name)
         ;(if debug (logformat "pg:with-private-handle*\n"))
         body ...)))))



(define-method pg:with-handle-of ((o <pg-database>) function)
  (pg:with-private-handle o function))

;; fixme: this should get `private' handle! (to be returned then)
(define-method ->db ((o <pg-database>))
  (logformat "->db returns a non-safe handle! ~a\n" o)
  (pg:new-handle o))





;; (define-macro (pg:with-private-handle* name pg . body)
;;   `(pg:with-private-handle ,pg
;;       (lambda (,name)
;;         ;(if debug (logformat "pg:with-private-handle*\n"))
;;         ,@body)))



;; list of all open/connected databases
(define *databases* ())
(define *databases-mutex* (make-mutex))

(define new-db-hooks ())
(define new-db-hooks-mutex (make-mutex))


;;; Hook is run when a DB is open, or immediately for already open DBs.
;; fixme: the semantic of TAG ..... and btw. there is native <hook> !
(define (pg:add-database-hook tag function)
  ;; run it for open DBs   ... fixme: is it correct?
  (if debug (logformat "pg:add-database-hook ~a\n" tag))
  (with-locking-mutex* *databases-mutex*
    (for-each
        function
      *databases*))

  ;; fixme: these 2 ops should be both protected by one mutex!
  ;; todo: lock this!
  (with-locking-mutex* new-db-hooks-mutex
    (let1 pair (assoc tag new-db-hooks) ;aget by eq? ?
      (if pair
          ;; overwrite:
          (set-cdr! pair function)

        ;; fixme: At the end!
        (let1 this-hook-info (cons tag function)
          (if (null? new-db-hooks)
              (push! new-db-hooks this-hook-info)
            (let1 lp (last-pair new-db-hooks)
              (set-cdr! lp
                        (cons
                         this-hook-info
                         (cdr lp))))
            ))))))


;; how to use notices?
;; pg-set-local-notice-processor (function  pg_conn::<pg-handle>)
(define (pg:add-listener db listen function)
  (with-locking-mutex* (ref db 'listener-mutex)
    (let1 binding (assoc listen (ref db 'listeners))
      (if binding
          (set-cdr! binding
                    (cons function (cdr binding)))
        ;; new one:
        (begin
          (let1 pg (ref db 'admin-handle) ;fixme!
            (pg-exec pg
              (s+ "LISTEN " listen ";")))

          (push! (ref db 'listeners)
                 (list listen function)))))))

;; Todo: Every code using 'admin-handle should collect the `notifies', right?
;; And maybe have a timer?
;;  PQconsumeInput, then check PQnotifies.
(define (pg:check-for-notifies db)
  (with-locking-mutex* (ref db 'listener-mutex)
    ;; fixme: so, under that mutex `admin-handle' must be available!
    ;; It's not designed!
    (let1 pg (ref (ref db 'admin-handle) 'conn)
      (pg-consume-input pg)
      (do ((notify (pg-notifies pg) (pg-notifies pg)))
          ((not notify) #t)
        (let1 listeners (assoc (ref notify 'relname)
                               (ref db 'listeners))
          (if listeners
              (for-each
                  (lambda (listener)
                    (listener notify))
                (cdr listeners))))))))


;; fixme:  pg:database-put! better?
(define (pg:database-set! db key value)
  (with-locking-mutex* (ref db 'dictionary-mutex)
    (hash-table-put!
     (ref db 'dictionary)
     key value)))

(define (pg:database-ref db key)
  (with-locking-mutex* (ref db 'dictionary-mutex)
    (hash-table-get
     (ref db 'dictionary)
     key)))



;;; Connecting:

;; path -> identity
;; hostname -> `fqhn' ?
(define (normalize-hostname host)
  (if (string-prefix? "/" host)         ;local path!
      host
    (let1 host (sys-gethostbyname host)
      ;;(slot-ref host 'aliases)
      (ref host 'name))))

;(normalize-hostname "/tmp")
;(normalize-hostname "linux10")


;; returns either <pg-database> or  <pg-conn> to a newly opened DB!
;; Called w/ *databases-mutex* locked!
;; todo: user?
(define (keep-unique host port database)
  ;; (if (and host port database)
  (if debug-handles (logformat-color 'green "keep-unique: ~a ~a ~a\n"
		      host port database))
  ;; connect
  (let* ((pg (apply pg-open ;;connect
                    (apply
                     append
                     (cond-list
                      (host `(:host ,host))
                      (database `(:dbname ,database)) ;if not given?
                      (port `(:port ,port))))))
         (pgc (ref pg 'conn))
         (hostname (normalize-hostname (pg-host pgc)))
         ;; This might be #f !
         (port (pg-port pgc))
         (database (pg-db pgc))

         (found (find                   ;filter
                 (lambda (db)
                   (and
                    ;; canonic hostname of HOST ?
                    (string=? hostname (ref db 'host))
		    ;; mmc: why ref and not (pg-hostname db)?

                    (string=? port     (ref db 'port))
                    (string=? database (ref db 'name))))
                 *databases*)))

    (if debug (logformat "keep-unique: ~a ~a ~a: ~a\n" hostname port database
			 (if found "FOUND" "NEW")))
    (if found
        (begin
          ;; push pg into the pool of connections
          (pg:dispose-handle found pg)
          ;(push! (ref found 'conn-pool) pg)
          found)
      pg)))


;; port?
(define (pg:connect-to-database . rest)
  (with-locking-mutex* *databases-mutex*
    (let-optionals* rest
        ;; fixme: IS this default used by  libpq ???
        ((host (sys-getenv "PGHOST"))
         (database (sys-getenv "PGDATABASE"))
         ;;
         (user (sys-getenv "PGUSER"))
         (port #f))
      (let1 p (keep-unique host port database) ;fixme:  USER!
        (cond
         ((is-a? p <pg-database>)
          p)
         ((is-a? p <pg>)
          ;; New one:
          (let1 pgc (ref p 'conn)
            (let* ((db (make <pg-database>
                         :name (pg-db pgc)
                         :host (normalize-hostname (pg-host pgc))
                         :port (pg-port pgc)
                         ;;
                         :user user     ;fixme: What's the point here?
                         :relations (make-hash-table 'string=?)
                         :dictionary (make-hash-table 'eq?)
                         :conn-pool ()  ;(list p)
                         :admin-handle p)))
              (push! *databases* db)

              ;; -fixme: lazily!
              ;;(if debug (logformat
	      ;;   "New DB, `pg:load-namespaces' should be lazy!\n"))

              ;; Run all hooks:
              (for-each
                  (cute <> db)
                (map cdr new-db-hooks))
              db)))
         (else
          (error "")))))))


;; Everytime we will (possibly) need the admin handle,
;; We have to acquire this mutex before any other mutex!
(define (pg:with-admin-handle db function)
  ;;  That mutex is handled _only_ by these internal functions!
  ;;  So it is always balanced!
  (if (eq? (current-thread) (mutex-state (ref db 'admin-handle-mutex)))
      (function (ref db 'admin-handle))

    (with-locking-mutex* (ref db 'admin-handle-mutex)
      (function (ref db 'admin-handle))
      ;; todo: Check notifies?
      )))



;; fixme: should be a method?
;; See `pg-type-name' in pg.types
;; return the name of the pg type (given by oid)
(define (pg-type-name db type-oid)         ; here??
  ;; fixme: the connections might share that table?
  ;; -fixme: use the (ref db 'admin-handle)
  (pg:with-admin-handle db
    (lambda (handle)
      (ref
       (pg-find-type handle type-oid)
       'name)
      ;;(pg-exec-internal pgconn "SELECT oid, typname FROM pg_type")

      ;; (hash-table-get
      ;;        (ref handle 'oid->type)
      ;;        type-oid)
      )))


(provide "pg/db")
