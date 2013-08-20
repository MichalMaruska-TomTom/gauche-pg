;;; this needs redesign/review


;;  tight  binding between  DB `tuples' (objects)  and gauche `objecs'


;; I have 2 ways:  either
;; *  symbolic attribute names   -- for now this one is implemented!
;; * or  indexes, w/ a hook for when something changes.


;; fixme:  should be    db.objects
(define-module pg.oo
  (export
   <db-stored-class>                    ;mixin:   some slots go to the DB. methods: save. update. where ?
   <db-stored-class-info>
   slots-of-mapping


   ;; fixme: generic !!
   db-insert
   db-insert-object
   db-update-object
   db-where
   db-delete
   db-lookup
   )

  (use pg-hi)                           ;exec
  (use pg.sql)                          ; s+  or adt.string
  (use pg.database)
  (use pg.types)


  (use srfi-1)
  (use util.list)                       ;rassoc!
  (use gauche.sequence)

  (use mmc.common-generics)
  (use mmc.simple)
  (use mmc.log)
  (use macros.aif)

  (use adt.alist)
  (use adt.ssm)
  )

(select-module pg.oo)

(define debug #f)

;; todo: I would like to define it as a metaclass. But i'm not good at the oo system yet.

;;; metaclass  where attributes have a specification for the backing  DB attribute

;; then I can have   methods  db-remove  db-insert ...

;; class-variable points here:

;; i don't use this anymore ?  b/c the entire class has the _same_ settting.
(define-class <db-stored-class> ()
  (
   ;; this works w/ only 1 relation backing it.
   ;; not more complex.
   (db-relation :init-keyword :db-relation
                :allocation  :each-subclass) ;when we derive ...
   ;;  ssm:
   (attribute-mapping :allocation   :each-subclass)
   ;;(type-printers
   ))


(define (slots-of-mapping mapping)
  (map car mapping))

(define (pg-attribute->symbol text)
  (string->symbol text))


;; metaclass?    ... not used in `gauche-foto'!
(define-method initialize ((object <db-stored-class>) . initiags)
  (next-method)

  ;;  intersection of the  DB  attributes &  slot names ??
  (unless (slot-bound? object 'attribute-mapping)
    (let* ((db-relation (slot-ref object 'db-relation))
           (attributes (pg:attributes-of db-relation))
           (slots (map slot-definition-name (class-slots (class-of object)))))

      ;(logformat "initiags:  attributes-of returned ~a\n" attributes)

      (slot-set! object 'attribute-mapping
        (intersection
         (map pg-attribute->symbol attributes)
         slots))))
  object)


;;; Generics
(define-method ->db ((o <db-stored-class>))
  (logformat "-db of <db-stored-class>\n")
  (->db (slot-ref o 'db-relation)))


;;  fixme:  p-key all bound?
;type-printer
(define-method db-insert ((object <db-stored-class>))
  (let* ((rel-info (slot-ref object 'db-relation))
         (mapping (slot-ref object 'attribute-mapping))
         ;(db (->db object))             ; get it !!

         (interesting
          (filter (lambda (a)
                    (slot-bound? object a))
                  (ssm-domain mapping))))

    (db-insert-object object rel-info interesting mapping)))

'(define-method db-modify ((object <db-stored-class>) alist)
   (let* ((rel-info (slot-ref object 'db-relation))
          (mapping (slot-ref object 'attribute-mapping))
          ;(db (->db object))            ; get it !!

          (interesting
           (filter (lambda (a)
                     (slot-bound? object a))
                   (ssm-domain mapping))))
     (string-append
      "INSERT INTO " (name-of rel-info)
      " ("
      (string-join (ssm-map mapping interesting) ; symbol -> string
          ", ")
      ")"
      " VALUES ("
      (string-join
          (map
              ;; type !!!!
              (lambda (symbol)
                (let1 type (pg:find-attribute-type rel-info (car (ssm-map mapping (list symbol))))
                  (scheme->pg type (slot-ref object symbol))))
            interesting)
          ", "
          )
      ");")))


(define-generic db-delete)

;; (define (slot-values object slot-list) 1)
(define-method db-delete ((object <db-stored-class>))
  (let* ((rel-info (slot-ref object 'db-relation))
         (mapping (slot-ref object 'attribute-mapping))
         ;(db (->db object))             ; get it !!

         (p-key (slot-ref rel-info 'p-key)) ;not bound ->  error !

         (slots (project p-key (ssm-inverse mapping))))
    ;;  primary key known?
    ;; all
    (string-append
     "DELETE FROM " (name-of rel-info)
     " WHERE "
     (string-join
         (map
             ;; type !!!!
             (lambda (symbol)
               (let* ((attribute (car (ssm-map mapping (list symbol))))
                      (type (pg:find-attribute-type rel-info attribute)))
                 ;;
                 (if (slot-bound? object symbol)
                     (string-append
                      attribute
                      " = "
                      (scheme->pg type (slot-ref object symbol)))
                   (error "db-delete: attribute from primary key is not bound:" symbol))))
           slots)
         " AND ")
     ";")))


;;;
(define-class <db-stored-class-info> ()
  ((db-relation :init-keyword :db-relation
                ;:allocation  :each-subclass
                )
   (handle :init-keyword :handle)
   (object-class :init-keyword :object-class)
   ;;  ssm:
   ;; :allocation   :each-subclass
   (attribute-mapping)
   ;;(type-printers
   )
  )

(define-method initialize ((object <db-stored-class-info>) . initiags)
  (next-method)

  ;;  intersection of the  DB  attributes &  slot names ??
  (unless (slot-bound? object 'attribute-mapping)
    (let* ((db-relation (slot-ref object 'db-relation))
           (attributes (pg:attributes-of db-relation))
           (slots (map slot-definition-name (class-slots (ref object 'object-class)))))

                                        ;(logformat "initiags:  attributes-of returned ~a\n" attributes)
      (slot-set! object 'attribute-mapping
        (filter-map
         (lambda (slot)
           (aif i (pg:attname->attnum db-relation (symbol->string slot))
                (cons slot
                      (pg:nth-attribute db-relation i))
             #f))
           slots))
      (if debug (logformat "the slot <-> pg-attribute mapping is: ~a\n" (slot-ref object 'attribute-mapping)))

      ;;       (intersection
      ;;        (map pg-attribute->symbol attributes)
      ;;        slots)
      ))
  object)


;;;  SQL
(define-generic db-insert)


;; given a gauche `object' (w/ slots),  store the values of `slots' in `relation'.
;; Mapping is ((slot . pg-attribute)...)
;; todo: SLOTS and MAPPING should be provided by the class?
(define (db-insert-object object relation slots mapping) ;;db
  (pg:with-handle-of* relation pg
                                        ;(let1 pg (->db relation)
    ;; I want:  convert the slot-values, and get the attnames:
    (let* ((attnames ())
           (value-list
            ;; sequentially
            (fold
             (lambda (slot value-list)
               ;; Of course car is <pg-attribute>
               (let* ((attribute (aget mapping slot))
                      (type (pg:attribute-type attribute)))

                 (push! attnames (ref attribute 'attname))
                 (cons
                  ((ref type 'printer)
                   (slot-ref object slot))
                  value-list)))
             ()
             slots))
           )
      (pg-exec
          pg
        (sql:insert
         (name-of relation)

         value-list
         attnames)))))

;; (sql:update relname values-alist where)
;; `slot-values' is   ((slot . new-value) ....)
(define (db-update-object object relation slot-values mapping) ;;db
  (if debug (logformat "db-update-object: ~s\n" slot-values))
  ;(logformat "db-update-object: ~a\n" mapping)
  ;(let1 pg (->db relation)
  (pg:with-handle-of* relation pg
    (pg-exec pg
      (sql:update
       (name-of relation)

       ;; Alist   attname value
       (map
           (lambda (slot-value)
             (let* ((attribute (aget mapping (car slot-value)))
                    (type (pg:attribute-type attribute)))
               (logformat "should update slot: ")
               (cons
                (ref attribute 'attname)
                (scheme->pg type (cdr slot-value))))) ;(slot-ref object slot)
         slot-values)
       (db-where relation mapping object))))
  ;; now:
  ;; update the object itself!
  (for-each
      (lambda (slot-value)
        (logformat-color 49 "db-update-object: ~a ~a <- ~a\n" ;'orange
          (car slot-value)
          (cdr slot-value)
          (slot-ref object (car slot-value)))
        (slot-set! object (car slot-value) (cdr slot-value)))
    slot-values))




;;  DESC is  db-stored-class-info:
;;  data gives the p-key data!
;;  and the connection!
(define (db-where relation mapping object)
  ;;(let ((mapping (slot-ref desc 'attribute-mapping)))
  (if debug (logformat "db-where: ~a and complete: ~a\n" mapping (ssm-complete mapping)))
  (let1 p-key (pg:primary-key-of relation)
    (string-join
        (map
            ;; type !!!!
            (lambda (attribute)
              ;; Get the slot:
              ;;(logformat "db-where: looking for ~s\n" attribute)
              (let* ((slot (car (rassoc attribute mapping))) ; getting #f without problem is not good!
                     (type (pg:attribute-type  attribute)))
                ;;
                (if (slot-bound? object slot)
                    (string-append
                     (pg:name-printer (ref attribute 'attname))
                     " = "
                     (scheme->pg type (slot-ref object slot)))
                  (error "db-where: attribute from primary key is not bound:" slot))))
          p-key)
        " AND ")))


;; todo:    db-delete-and-backup
;;
(define-method db-delete ((object <db-stored-class-info>) data-object)
  (logformat "->db in db-delete\n")
  (let* ((rel-info (slot-ref object 'db-relation))
         (mapping (slot-ref object 'attribute-mapping))
         (attributes (ref rel-info 'types)) ;'attributes
         (db (->db data-object))        ; get it !!

         (p-key (pg:attribute-indexes->names
                 rel-info
                 (slot-ref rel-info 'p-key))) ;not bound ->  error !
         (slots (project p-key (ssm-inverse mapping))))
    ;;  primary key known?
    ;; all
    (string-append
     "DELETE FROM " (name-of rel-info)
     " WHERE "
     (db-where object data-object)
     ";")))



;; this is like   find, delete -> not remove (takes predicate)!

;; query to
;; REL-INFO   +  alist  ((attname  value) ....)   ->   pg result
;;
;; Fixme: I should hand it (optionally) a pg handle!
(define (db-lookup relation data-alist select-attributes . rest)
  (if debug
      (logformat "db-lookup in ~a: ~s.\n\tGet ~a\n" relation data-alist select-attributes))
  (let-optionals* rest
      ((order-list ()))
    ;; canonize:     ; condone #f
    (if (not order-list) (set! order-list ()))
    ;;(logformat "db-lookup ~s ~s ~s\n" rel-info data-alist select-slots)
    (pg:with-handle-of* relation handle
      (let (                            ;(db (->db relation))
            (query
             (sql:select
              (map (lambda (a)
                     (slot-ref a 'attname))
                select-attributes)
              (name-of relation)
              ;;
              (sql:alist->where
               (map ;; type !!!!
                   (lambda (attribute-value)
                     ;; Of course car is <pg-attribute>

                     (let1 type (pg:attribute-type (car attribute-value))
                                        ;(pg:find-attribute-type relation (car attribute-value))
                       (cons
                        (slot-ref (car attribute-value) 'attname)

                        ((ref type 'printer)
                         (cdr attribute-value)))))
                 data-alist))

              (if (null? order-list)
                  #f
                (begin
                  ;; (logformat "ORDER BY ~s\n" order-list)
                  (string-join order-list ", "))))))
        ;;(logformat "query: ~a\n" query)
        (pg-exec handle query)))))


(provide "pg/oo")
