
;; My response to the kahua.persistence!

(define-module pg.persistent
  (export
   ;; mmc:  please rename!
   <pg-persistent-tuple-meta>              ; inspired by kahua!
   <pg-tuple-base>

   ;; These generics _must_ be imported from elsewhere, and we merely add methods.
                                        ;find-kahua-tuple
                                        ;store-kahua-instance
   pg:find-tuple
   pg:store-tuple

   pg:row->object

   pkey-separator
   decode-pkey				;fixme:  rename!
   )
  (use pg-hi)
  (use pg.database)
  (use pg.types)
  (use mmc.log)
  (use srfi-13)
  (use srfi-1)

  (use pg.sql)
  )

(select-module pg.persistent)

(define debug #f)
;; todo:
(define pkey-separator "#")


;; This is necessary before defining classes!
(when #f
  (define mdb (pg:connect-to-database "linux6" "maruska"))
  (define class-ticket (pg:find-relation mdb "tickets_new"))
  (pg:namespace-relations
   (pg:nspname->namespace pgd "public")))



;;; mmc: This is my metaclass.
;;; It loads the slots from DB. given as :relation.
(define-class <pg-persistent-tuple-meta> (<class>) ;<kahua-persistent-meta>
  ((relation :init-keyword :relation)   ; -> db
   ))

(define-method compute-slots ((class <pg-persistent-tuple-meta>))
  ;; (logformat "compute-slots\n")
  (let* ((attributes (pg:attributes-of (ref class 'relation))))
    (append
     '((%modified-slots :init-value ())
       (%new :init-value #t)            ;INSERT VALUES, rather than UPDATE!
       (%can-change-key :init-value #f))
     (map (lambda (attribute)
            (list
             (string->symbol attribute)
             :allocation :persistent))
       attributes))))


(define-method compute-get-n-set ((class <pg-persistent-tuple-meta>) slot)
  (define (delete-slot-definition-allocation slot)
    (cons (car slot)
          (delete-keyword :allocation (cdr slot))))

  ;(logformat "compute-get-n-set! ~a\n" slot)
  (let ((alloc (slot-definition-allocation slot))) ;class, instance
    (if (eq? alloc :persistent)
        (let* ( ;; (slot-num (slot-ref class 'num-instance-slots))
               ;; mmc:  ^^^^   does it force a promise ?
               (acc (let1 slot (delete-slot-definition-allocation slot)
                      ;; ??
                      ;; gauche.object non documented?
                      (compute-slot-accessor class slot (next-method class slot)))))

          ;; mmc: so this would be done by the default method:
          (inc! (slot-ref class 'num-instance-slots)) ;mmc: macro!
          (list ;; (make-getter acc class slot)
           (cute slot-ref-using-accessor <> acc)
           (make-tuple-setter acc slot)
           (make-boundp acc)
           #t))
      (next-method))))



(define (make-boundp acc)
  (lambda (o)
    (slot-bound-using-accessor? o acc)
    ;; fixme: This can be done differently:
                                        ;(not (eof-object? (slot-ref-using-accessor o acc)))
    ))


;; How to get the value from a `:persistent' marked slot:
;; look at _another_, slot option: `:out-of-transaction'
;;  that defines what to do if no transaction is current?
;;
;; mmc: slot is irrelevant ?
(define (make-getter acc class slot)


  (let ((aot (slot-definition-option slot :out-of-transaction :read-only))) ;:read-only is default
    ;;  :out-of-transaction  ->
    (lambda (o)
      (if (current-db)                  ; mmc: what if I have 2 different DBs?
          (ensure-transaction o)        ; mmc:  in metainfo!
        (when (eq? aot :denied)
          (error "database not active")))

      ;; Try to see if the object has (in the slot) already a retrieved value
      ;; otherwise there should be a wrapper -> peel-wrapper  runs it?
      ;;
      (let1 val (slot-ref-using-accessor o acc)
        (if (is-a? val <kahua-wrapper>)
            (let1 real (peel-wrapper val)
              ;; mmc: this returnes the tuple!  fixme!
              (slot-set-using-accessor! o acc real) ; mmc: this invokes _our_ setter, w/o invoking class-change?
                                        ; or does it simply set the slot value (w/o using `setter')?
              real)
          ;; not wrapper:
          val)))))


;; mmc: difference:
;;  in both we want ensure-transaction
;;  but for setter we allow setting w/o  DB!
;;
(define (make-tuple-setter acc slot)
  ;;(let1 slot-name (car slot)
  (lambda (o v)
    #;
    (if (and (p-key)
             (not (slot-ref-using-accessor o '%can-change-key)))
        (throw
         (condition
          (
           ))))

    (slot-set-using-accessor! o acc v)
    ;;
    (unless (memq (car slot) (ref o '%modified-slots))
      (push! (ref o '%modified-slots) (car slot))))) ;slot-name  symbol!

;; from kahua, not used here!
;; fixme: So not used anywhere!
(define (make-setter acc slot)
  (let ((aot       (slot-definition-option slot :out-of-transaction :read-only))
        (slot-name (car slot)))
    (lambda (o v)                       ;object value
      (let1 db (current-db)
        (if db
            (begin
              ;; mmc: why transaction ?
              (ensure-transaction o)    ; the class has '%transaction-id .... so all records of the class
                                        ; need to be in the same transaction!  ... consistency!
              ;; mark the instance and be done!
              (unless (memq o (ref db 'modified-instances))
                (push! (ref db 'modified-instances) o))
              ;; accept the value
              (slot-set-using-accessor! o acc v))

          ;; no DB!
          (if (eq? aot :read/write)
              (begin

                ;; mmc: what is this?  For now it seems unused, see kahua-write-syncer
                (unless (assq slot-name (ref o '%in-transaction-cache))
                  (push! (ref o '%in-transaction-cache)
                         (cons slot-name (slot-ref-using-accessor o acc))))
                ;;  mmc: ?
                (floated-instance-touch! o)
                (slot-set-using-accessor! o acc v))

            (error "database not active")))))))



;(define-method initialize ((class <pg-persistent-tuple-meta>) initargs))




;; return a list of values (strings).
(define (decode-pkey key)
  ;; KEY is a string  1309#1##
  ;; pkey is a list of <pg-attribute>  -> <pg-type>

  (unless (string? key)
    (errorf "decode-pkey: need a string, not ~a" key))
  ;; fixme: I have to convert \# -> #
  (let1 strings
      (let step ((vals ())
                 (rest key))
        ;; we know how many values should be found!
	;; (logformat "step: ~a\n" rest)
        (if (string=? rest "")
            vals

          (cond
           ((string-prefix? pkey-separator rest)   ;#\#
            (step (cons "" vals) (string-drop rest 1)))

           (else
	    ;; get the first
	    ;; greedy
            (let1 match (rxmatch #/^((?:[^#\\]+(?=[#\\])|(?:\\.)+)+)#/
                                        ;#/^((?:[^#\\]*|(?:\\.))*)#/

                         ;; [^\\](\\)*\#
                         ;; ([^#] )*
                         ;;
                         ;;   #/^(\#|.*[^\\](\\\\)*#)/
                         rest)
              (if match
                  (step (cons (rxmatch-substring match 1) vals)
                        (string-drop rest (rxmatch-end match 0)))
                (step (cons rest vals) "")))))))
    (reverse
     (map
         (lambda (s)
           ;; unquote:
           (regexp-replace-all #/\\#/ s  pkey-separator))
       strings))))





(define (pg:row->object class result row)
  (if debug (logformat "pg:row->object\n"))
  (let1 o (make class)
    ;; We have got it from DB!
    (slot-set! o '%new #f)

    ;(for-each
    (dotimes (i (pg-nfields result))
      ;; if the slot is desired?
      (let ((val (pg-get-value result row i))
            (slot (string->symbol ;; hm!
                   (pg-fname result i))))
        (unless (eof-object? val)
          (slot-set! o slot val)
          ;; undo the ...
          (slot-set! o '%modified-slots
            ;; fixme: only one so ..delete-first
            (delete slot (slot-ref o '%modified-slots))))))
    o))




;; find the class, <pg-relation> ->  decode the key, do the search
;; either construct the object, or #f! or new objetc?
;; attention! I should recycle!
;; So I need some btree? or hash?

;; (define-method read-kahua-instance
;; See: kahua.pg `read-kahua-instance'
;; (define-method find-kahua-instance ((class <pg-persistent-tuple-meta>) key) ;(key <string>)
;;  (pg:find-tuple class key))
  ;;(define (find-kahua-instance class key)


;; fixme: Should this keep an ADT to cache ?
;; todo:  fixme:this should accept the key as list!  mmc: Done!
;; fixme: returns #f if Not found!  should raise error?
(define (pg:find-tuple class key)
  (let* ((d (ref class 'relation))
         (pkey (ref d 'p-key)))
    ;; look in hash!
    (if debug (logformat "now decode-pkey: ~a\n" key))
    (let1 key-list (if (list? key)
		       key
		     (decode-pkey key))
      (unless (= (length pkey)
                 (length key-list))
        (error "incorrect key-string. wrong number of values"))
      (let1 r
          (pg:with-handle-of* (ref d 'database) handle
            (pg-exec handle
              (sql:select
               "*"
               (ref d 'name)
               (sql:alist->where
		;; fixme: I have to find the printers ... or simply quote!!
                (map cons
                  (map (cut slot-ref <> 'attname) pkey)
                  ;; pkey
                  (map
		      pg:text-printer
		    key-list))))))
	(if debug (logformat "row->object ~a ~d\n" class (pg-ntuples r)))

        (if (zero? (pg-ntuples r))
            #f
          (pg:row->object class r 0))))))


(define-class <pg-tuple-base> ()
  ((%modified-slots :init-value ())
                                        ;:metaclass <pg-persistent-tuple-meta>
   ))



;; how to make it a method?
;; (define-method store-kahua-instance ((object <pg-tuple-base>))
;;   (pg:store-tuple object))
;; see:
;; write-kahua-instance

(define (pg:store-tuple object)
  (let* ((modified-slots (ref object '%modified-slots))
         (class (class-of object))
         (relation (ref class 'relation))
         (db (ref relation 'database)))
    (unless (is-a? class <pg-persistent-tuple-meta>)
      (error "store-kahua-instance  called on a wrong argument" object))

    (unless (ref relation 'p-key)
      (error "cannot update a tuple from a relation without primary key!"))
    (begin0
     (pg:with-handle-of* db handle
       (pg-exec handle
         (if (slot-ref object '%new)
             ;; (let modified-and-bound?
             ;; fixme: If the relation has some default values (and I don't set them here explicitely), I should retrieve them! todo!
             (sql:insert
              (ref relation 'name)
              ;;
              (map
                  (lambda (slot)
                    ;; fixme!
                    (scheme->pg
                     (pg:find-attribute-type relation (symbol->string slot))
                     (slot-ref object slot)))
                modified-slots)

              (map symbol->string modified-slots))

           ;; let's first construct this, as without it it's a no-go situation:
           (let1 where-alist (map
                                 ;; fixme: This must be present! If P-key is not known, we must not run the UPDATE!
                                 (lambda (attribute)
                                   (cons (ref attribute 'attname)
                                         ;; fixme:
                                         (scheme->pg
                                        ;(ref attribute 'attname)
                                          (slot-ref attribute 'type)
                                          (slot-ref object
                                                    (string->symbol (ref attribute 'attname))))))
                               (ref relation 'p-key))
             (sql:update
              (ref relation 'name)
              (map
                  (lambda (slot)
                    (cons (symbol->string slot)
                          ;; fixme!
                          (if (slot-ref object slot)
                              (scheme->pg
                                        ;(pg:attribute-type
                               (pg:find-attribute-type relation (symbol->string slot))
                                        ;(x->string
                               (slot-ref object slot))
                            "NULL")))
                modified-slots)
              ;; where
              (sql:alist->where where-alist))))))
     ;; todo: check that only 1 object is updated indeed?

     ;; Reset:
     (if (slot-ref object '%new)
         (slot-set! object '%new #f))
     (slot-set! object '%modified-slots ()))))


(provide "pg/persistent")
