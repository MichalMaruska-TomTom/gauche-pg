
;; Caching -- representing pg_class info + related.
;;

(define-module pg.relation
  (export
   ;; pg.hi needs is-a?
   <pg-relation>
   ;; fixme: This should not be exported. Otherwise
   ;; we have to check in the initialize method
   ;; if we have all necessary data/slots!
   ;;  (i.e. user is not allowed to do:  (make <pg-relation>)

   ;; User can associate data -- useful?
   pg:relation-put!
   pg:relation-get

   pg:refresh-relation-info
   pg:namespace-relations                    ;Still not used!
   pg:database-relations

   ;; pg:get-relation-view
   pg:get-relation
   pg:find-relation ;; by-name  w/ namespace path?
   pg:get-relation-by-oid



   ;; generic:
   pg:primary-key-of

   ;;; Depends on `pg.attributes'
   ;; type-of-attribute

   pg:attname->attnum

   pg:attname->attribute  pg:get-attribute
                                        ;fixme: This should be a new standard!
   pg:nth-attribute    pg:attnum->attribute

   pg:real-nth-attribute pg:attribute-colnum

   pg:attributes-of
   pg:find-attribute-type

   ;;
   pg:for-namespace
   ;; old:
   pg:for-each-relation

   ;;
   <pg-view>
   pg:is-view?
   )

  (use pg)
  (use pg-hi)
  (use pg.types)
  (use pg.sql)

  (use pg.base)
  (use pg.db)
  (use pg.namespace)
  (use pg.attribute)

  (use mmc.common-generics)

  (use srfi-1)
  (use srfi-11)
  (use gauche.threads)
  (use mmc.threads)

  (use adt.string)
  (use adt.alist)
  (use adt.vector)
  (use mmc.simple)
  (use macros.assert)
  (use macros.aif)
  (use mmc.log)
  (use mmc.throw)
  )

(select-module pg.relation)

(define debug #f)

;;; info on a `relation' (TABLE) in the DB server
(define-class <pg-relation>  ()
  (;; type <pg>  ?? or <pg-handle> ??
   (database :init-keyword :database)
   ;;(db-handle :init-keyword :db-handle); :init-keyword #f
   ;; fixme:  do i need database then ?  virtual?
   (namespace :init-keyword :namespace)

   ;; When we refresh the info:
   (relation-mutex :init-form (make-mutex))

   (oid :init-keyword :oid)
   ;; oid relhasoids relnatts relfilenode

   ;; relid ???
   ;; mmc: shouldn't I use name ??!!!
   (name :init-keyword :name
         :getter name-of
         ;:accessor relname-of
         )
   ;; list of attributes: indexes or names?
   (p-key :init-keyword :p-key
          :getter pg:primary-key-of)

   (p-key-name :init-keyword :p-key-name)

   ;; name -> index
   (attributes-hash)
   (attribute-min :init-value 0)
   ;; normally 0, but could be -2 if -2 is the attnum of
   ;; `oid' and oid is the primary-key !

   ;; This is a vector: i -> `<pg-attribute
   ;; a vector of cons (attribute-name attnum)>'
   (attributes)

   ;; User associated data:
   (data :init-value ())
   ;; make an alias attributes?
   ))

(define (pg:relation-put! rel tag data)
  (slot-set! rel 'data
    (aput (slot-ref rel 'data)
          tag
          data)))

(define (pg:relation-get rel tag . default)
  (apply assoc-ref-car
   (slot-ref rel 'data)
   tag
   ;; fixme: !!!
   ;; eof-object
   default
   ))



(define-method write-object ((o <pg-relation>) port)
  (format port
    "<~a ~a>"
    ;; "<"
    ;(string-drop-right (string-drop (class-name (class-of o)) 1) 1)
    "db-relation"
    (ref o 'name)))

;;;
(define-method pg:with-handle-of ((o <pg-relation>) function)
  (pg:with-private-handle (slot-ref o 'database)
    function))





;; fixme: this should get `private' handle! (to be returned then)
(define-method ->db ((o <pg-relation>))
  (logformat "->db returns a non-safe handle! ~a\n" o)
  (pg:new-handle (slot-ref o 'database)))


;;; View:

(define-class <pg-view> (<pg-relation>)
  ((definition)
   (fake-result)
   ;; (fixme!) For now:
   (tuples)
   )
  )

(define (pg:is-view? r)
  (is-a? r <pg-view>))

(define-method write-object ((o <pg-view>) port)
  (format port
    "<Pg-View ~a>"
    ;; "<"
    ;(string-drop-right (string-drop (class-name (class-of o)) 1) 1)
    (ref o 'name)))


;; isn't this too complicated?
(define-method initialize ((rel <pg-relation>) . initargs)
  (next-method)                         ;initiags)
  ;; fixme: i should enforce the namespace!  if not present, get it from DB !
  (DB "initialize: ~a (in ~a)\n" (ref rel 'name) (ref rel 'namespace))
  (pg:refresh-relation-info rel))


;; populate the p-key related slots:
(define (load-p-key pgh rel)
  (receive (name p-key)			;name is `conname' ...constraint name ?
      (let1 result (pg-exec pgh
                     (sql:select-u
                      '(conkey conname)
                      :from "pg_constraint"
                      :where
                      (s+
                       "conrelid = " (number->string (ref rel 'oid))
                       ;;(relname->relid handle relname)) ;I don't like this!
                       " AND "
                       "contype = 'p' ")
                      ;;" conname='" relname "_pkey'"
                      ))
        (if (zero? (pg-ntuples result))
            (values #f #f)

          ;; First check the minimum:
          (values
           (pg-get-value result 0 1)
           (let1 value (pg-get-value result 0 0)

             ;; Bug:  This is a list of `attnum's !
             (let1 min-attribute (apply min value)
               (when (< min-attribute (ref rel 'attribute-min))
                 ;; I have to reconstruct the attributes vector!
                 (enlarge-n-load-attributes! rel min-attribute pgh)))

             (map (lambda (attnum)
                    ;; fixme: do we start w/ 0 ??
                    (vector-ref (slot-ref rel 'attributes)
                                (-
                                 attnum
                                 (slot-ref rel 'attribute-min)))) ; huh?
               value)))))
    ;; fixme: the p-key should be sorted !
    (slot-set! rel 'p-key p-key)
    (slot-set! rel 'p-key-name name)
    ;;(pg:attribute-indexes->names rel p-key)
    ))


;; fixme: When necessary?  When someone adds columns to a relation
;; fixme: This might create a new one, compare them.
;;  If differ -> copy over and call some hook?
;; fixme: IF the namespace differs???
(define (pg:refresh-relation-info rel)
  ;; fixme: Careful to use this!
  (pg:with-admin-handle (ref rel 'database)
    ;; (pg:with-handle-of* rel pgh
    (lambda (pgh)
      (let ((relname (slot-ref rel 'name)))

        ;; GEt the basic info:    fixme: we already have it!
        (let1 rel-result
            (pg-exec pgh
              (sql:select
               ;; todo:
               ;;  fixme: pg:get-relation  already could get this data!
               '(oid relhasindex relkind relnatts relchecks)
               "pg_class"
               (sql:alist->where
                `(("relname" . ,(pg:text-printer relname))
                  ("relnamespace" . ,(pg:number-printer (ref (ref rel 'namespace) 'oid)))))))

          (unless (= (pg-ntuples rel-result) 1)
            (errorf "pg:refresh-relation-info: more objects with the same relname: ~a" relname))

          (assert (= (pg-ntuples rel-result) 1))

          ;; make it a hash?
          ;;(assert (char=? (pg-get-value-by-name rel-result 0 "relkind") #\r)) ;fixme! "r"

          (unless (let1 kind (pg-get-value-by-name rel-result 0 "relkind")
                    (or
                     (char=? kind #\r)
                     (char=? kind #\v)))
            (error "not a relation!" relname))

          ;; Check the attributes:
          (let1 relnatts (pg-get-value-by-name rel-result 0 "relnatts")
            ;; tuples of a .... should have a gauche class, and we would use the class-modification protocol ??
            ;; we should have a forward pointer from the old `<pg-relation>' ? So GC will clean it, but we can upgrade....?

            ;;
                                        ; (error "not implemented!")

            ;; `types
            (unless (slot-bound? rel 'attributes) ;(slot-ref rel 'types)  ;
              (receive (hash vector)
                  (get-attributes-of! rel pgh 0 relnatts)

                (slot-set! rel 'attributes-hash hash) ; fixme!
                (slot-set! rel 'attributes vector)))))

                                        ;(logformat "attributes: ~a\n" (ref rel 'attributes))
        ;; fixme: (if debug (logformat "types: ~a\n" (ref rel 'types)))
        ;; `p-key
                                        ;(unless (slot-bound? rel 'p-key) ;(slot-ref rel 'types)  ;
                                        ;  (slot-set! rel 'p-key
        ;; decompose the pg-array into a `list' of attributes.
        (unless (slot-bound? rel 'p-key)
          (load-p-key pgh rel)
          ))))
  (DB "p-key: ~a\n" (ref rel 'p-key)))


;;;
;; Create a <pg-relation> out of a row in result (of a suitable query) ... from pg_class!
(define (data->relation namespace result index)
  ;; todo:  class (if () )
  (let1 class
      (if (char=? #\r (pg-get-value-by-name result index "relkind"))
          <pg-relation>
        <pg-view>)
    (let1 relname (pg-get-value-by-name result index "relname")
      ;; fixme: Check!
      (DB "creating ~a: ~a\n" class relname)

      ;; todo: hook?  (reload-view-definition! view)
      (make class ;;<pg-relation>
        :name relname
        :database (ref namespace 'database)
        :namespace namespace
        :oid (pg-get-value-by-name result index "oid")))))




;; throw error if not unique?
;; todo: extract the namespace !     nspname.relname ?  or select * from relname where 1=0; ??
;; note: This might be done w/ defaults (of optional args) directly by data->view !
(define (load-relation-with-query h namespace query)
  ;; fixme: This migh re-create a <pg-relation> object, thus defying the desired unicity!
  (let1 result (pg-exec h query)
    (if (not (= (pg-ntuples result) 1))
        (error "no relation matching query " query)
      (let1 rel (data->relation namespace result 0)
        (hash-table-put! (ref namespace 'relations) (ref rel 'name) rel)
        rel))))



(define condition-for-relation-or-view "( relkind = 'r' OR relkind = 'v')")

;; unless we already have the <pg-relation> object (in a hash table),
;; Create it from data from DB (and put in a hash) and return it.
;; RELOAD? -> even if already in the hash.
(define (pg:get-relation namespace relname . reload?)
  (pg:with-admin-handle (ref namespace 'database)
    (lambda (h)
      ;; note:  h is not used here, just `locked!'
      (with-locking-mutex* (ref namespace 'relation-mutex)
        ;; See if we already have it!
        (let1 current (hash-table-get (ref namespace 'relations) relname #f)
          (cond
           ((and (null? reload?)
                 current)
            current)

           (else
            ;; BUG: this query could conflict w/
            ;; (aif relation (catch 'found
            ;;                             (hash-table-for-each (ref namespace 'relations)
            ;;                               (lambda (relname relation)
            ;;                                 (if (string=? (ref relation 'name) relname)
            ;;                                     (throw 'found relation))))
            ;;                             #f)
            ;;                  ;(error "BUG: attempting to recreate a pg relation!")
            ;;                  (begin
            ;;                    (logformat-color 'red "BUG: attempting to re-create a pg relation! ~a\n" reload?)
            ;;                    (hash-table-put! (ref namespace 'relations) relname relation)
            ;;                    relation)
            (load-relation-with-query
             h
             namespace
             (sql:select
              '(oid relname relkind relnatts relfilenode relnamespace)
              ;; oid! ;reltuples relpages relhasindex
              "pg_class"
              (s+ condition-for-relation-or-view
                  " AND relname = " (pg:text-printer relname)
                  ;; AND rel
                  " AND relnamespace = " (pg:number-printer (ref namespace 'oid))))))))))))



;; todo:  namespace search path
;; fixme: if I have namespace, i don't need DB!
(define (pg:find-relation db relname :optional namespace) ; fixme: reload!
  (let1 namespace (pg:extract-namespace db namespace)
    ;; todo: look at the list of relnames ... if member !?
    (pg:get-relation namespace relname)
    ;;(let1 hash (ref db 'relations)
    ;; todo: I should be able to refresh!
    ;;(if (hash-table-exists? hash relname)
    ;; (hash-table-get hash relname)
    ;; fixme: should i update (re-select)?
    ;; this is non-sense!
    ))

;; Todo: namespace should have more prominent role!
;; fixme!
;; i don't like this: i would like to throw a new object....        ???
(define (pg:get-relation-by-oid db oid)
  ;; (logformat "pg:get-relation-by-oid is buggy!?\n")
  ;; fixme:  It might be not in "public", but in: SHOW search_path;
  (pg:with-admin-handle db
    (lambda (h)
      ;; Fixme: this should get the relation, and only then search for namespace BUG!
      ;;
      (let1 r (pg-exec h
                (sql:select
                 '(oid relname relnatts relfilenode relnamespace)
                 ;;oid! ;reltuples relpages relhasindex
                 "pg_class"
                 (s+
                  condition-for-relation-or-view
                  " AND oid = " (pg:number-printer oid))))
        (if (zero? (pg-ntuples r))
            (errorf "pg:get-relation-by-oid:  relation of oid ~d does not exist!" oid))
        (let1 namespace (pg:oid->namespace db (pg-get-value-by-name r 0 "relnamespace"))
          ;; (let1 namespace (pg:nspname->namespace db "public")
          (with-locking-mutex* (ref namespace 'relation-mutex)
            (or (catch found
                  (hash-table-for-each (ref namespace 'relations)
                    (lambda (key value)
                      (if (= (ref value 'oid) oid)
                          (found value))))
                  #f)
                ;; we have to load it!
                ;; fixme: maybe I should load the entire namespace?
                (load-relation-with-query
                 h
                 namespace
                 (sql:select
                  '(oid relname relkind relnatts relfilenode relnamespace) ;oid! ;reltuples relpages relhasindex
                  "pg_class"
                  (s+
                   condition-for-relation-or-view
                   " AND oid = " (pg:number-printer oid)
                      ;; AND rel
                      " AND relnamespace = " (pg:number-printer (ref namespace 'oid))))))))))))

;;;
;;;  __Attributes__
;;; Keeping the attributes list:
(define (enlarge-n-load-attributes! relation min-attribute pg)
  (let (;(new-attributes (shift-vector-by (slot-ref relation 'attributes) (- min-attribute)))
        (old-minimum (ref relation 'attribute-min)))
    (if debug
        (logformat "I have to get more attributes for ~a, since attribute ~d is in p-key!\n" (ref relation 'name) min-attribute))
    ;; fill-in the attribute info on new-minimum .... old-minimum!
    (slot-set! relation 'attributes
      (shift-vector-by  (slot-ref relation 'attributes) (- old-minimum min-attribute)))
    (slot-set! relation 'attribute-min min-attribute)
    ;; mmc: oh!
    (error "mess!")
    (get-attributes-of! relation pg min-attribute (- old-minimum 1))))


;; internal function:
;; returns 2 values:
;; @hash  (attname->index mapping)
;; @vector
(define (get-attributes-of! relation handle from to) ;handle is <pg> !!
  (unless (eq? (class-of handle) <pg>)
    (error "get-attributes-of!: wrong-type for handle, should be <pg>"
           handle (class-of handle)))
  (assert (slot-bound? relation 'attribute-min))
  (let* ((result (pg-exec handle
                   ;;  (column  attname  type)
                   (s+
                    ;; we don't rely on (attnum = row)
                    "SELECT A.attnum, A.attname, A.atttypid "
                    "FROM pg_attribute A JOIN pg_class C ON (A.attrelid = C.oid) "
                    "WHERE attnum between "
                    (number->string from) " and " (number->string to)
                    " AND  relname = "
                    (pg:text-printer (slot-ref relation 'name))
                    "  AND NOT attisdropped " ;fixme!
                    "ORDER BY attnum;")))
         (count (pg-ntuples result))
         (offset (- (slot-ref relation 'attribute-min))))

    ;; Fixme: I should check, that `attnum's are 0,1,
    ;         otherwise I reserve more than needed?
    (let-values (((info-vector attributes-hash)
                  (if (slot-bound? relation 'attributes)
                      (values
                       (slot-ref relation 'attributes)
                       (slot-ref relation 'attributes-hash))
                    (values
                     (make-vector (+ (max to (- from to))
                                     ;; (pg-get-value result (- count 1) 0)
                                     1)   #f) ; bug: count
                     (make-hash-table 'string=?))))
                 )
      (DB "count is ~d\n" count)

      ;; fixme: This should be a method!
      (pg-foreach-result result
          (list "attnum" "attname" "atttypid")
        (lambda (num name type-oid)
          (hash-table-put! attributes-hash
                           name num)
          (let* ((type (pg-find-type handle type-oid))
                 (attribute (make <pg-attribute>
                              :relation relation
                              :attnum num
                              :attname name
                              :atttyp type)))
            (vector-set! info-vector (+ offset num)
                         attribute))))

      (values attributes-hash info-vector))))



;; fixme: i could keep 2 lists/vectors instead of 1 alist
(define (pg:attributes-of relation)        ;names?
  (filter-map
   (lambda (i)
     (and i
          (ref i 'attname)))
   ;; fixme: no need to filter?
   (vector->list (slot-ref relation 'attributes))))

;; `pg:attnum->attribute'
(define (pg:nth-attribute relation n)
  (if (< n (ref relation 'attribute-min))
      ;; we have to load info on this one!
      ;; I should LOCK...
      (enlarge-n-load-attributes! relation n (->db relation)))
  (vector-ref (ref relation 'attributes)
              (- n
                 (ref relation 'attribute-min))))

;;; Fighting some bug!s
(define (pg:real-nth-attribute relation n)
  (let* ((att-vector (ref relation 'attributes))
         (limit (vector-length att-vector)))
    (unless (< -1 n limit)
      (error "Attribute index out of bounds."))

    (let step ((j (ref relation 'attribute-min))
               (k 0))
      (cond
       ((= j (+ (ref relation 'attribute-min)
                limit))
        (error "does not exist"))

       ((not (vector-ref att-vector j))
        (step (+ j 1) k))
       (else
        (if (= n k)
            ;; note: found!
            j
          (step (+ j 1) (+ k 1))))))))


;; colnum in the result "select * from relation"
(define (pg:attribute-colnum relation attnum)
  (let* ((att-vector (ref relation 'attributes))
         (limit (vector-length att-vector)))
    ;; count the number of #f in the vector!
    (let1 number-of-false
        (let step ((i (ref relation 'attribute-min))
                   (j 0))
          (cond
           ((= i (+ (ref relation 'attribute-min) attnum))
            j)

          ((vector-ref att-vector i)
           (step (+ i 1) j))
          (else
           (step (+ i 1) (+ 1 j)))))

      (- attnum number-of-false))))


(define pg:attnum->attribute pg:nth-attribute)

;; The real `attnum', not the position!
(define (pg:attname->attnum relation attname)
  (aif a (pg:attname->attribute relation attname)
       (ref a 'attnum)
    #f))

(define (pg:attname->attribute relation attname)
  (let1 hash (ref relation 'attributes-hash)
    ;; fixme:
    (aif num (hash-table-get hash attname #f)
         (pg:nth-attribute relation num)
      #f)))

;; legacy api?
(define pg:get-attribute pg:attname->attribute)

;; composite:
(define (pg:find-attribute-type relation attname)
  (pg:attribute-type
   (pg:attname->attribute relation attname)))

;;; `Collection' of relations   __namespace__

;; This is wrong!
(define (pg:for-namespace db namespace function)
  ;; fixme: mutex! bug!
  (DB "pg:for-namespace is buggy: needs a mutex!\n")
  (for-each
      (lambda (relname)
        (function (pg:get-relation namespace relname)))
    (pg:namespace-relnames namespace)))


(define pg:for-each-relation pg:for-namespace)

;; todo: I should keep all these objects somewhere!
;; in a hash?  oid -> <pg-relation> ?
(define (pg:namespace-relations namespace)
  (map (cute pg:get-relation namespace <>)
    (pg:namespace-relnames namespace)))



;; return the list of relations in @db
(define (pg:database-relations db . refresh?)
  ;; This is run in a hook
  ;; (pg:load-namespaces! db)
  ;; Make sure (ref db 'relations)
  (let1 namespaces (ref db 'namespaces)
    (unless (null? refresh?)
      (for-each
          (lambda (ns)
            (pg:namespace-relations ns))
        namespaces))

    (append-map
     (lambda (namespace)
       ;; pg:namespace-relations
       (hash-table-values (ref namespace 'relations)))
     namespaces)))

(provide "pg/relation")
