;;; this needs redesign/review


;;  tight  binding between  DB `tuples' (objects)  and gauche `objects'


;; I have 2 ways:  either
;; *  symbolic attribute names   -- for now this one is implemented!
;; * or  indexes, w/ a hook for when something changes.


;; fixme:  should be    db.objects
(define-module pg.oo
  (export
   ;; mixin:   some slots go to the DB. methods: save. update. where ?
   <db-stored-class-info>

   ;; fixme: generic !!
   ;; db-insert
   db-insert
   db-update
   db-where
   db-delete
   db-lookup

   find-full-intentification
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
  (use macros.assert)

  (use alg.find)
  (use adt.alist)
  (use adt.ssm)
  )

(select-module pg.oo)


(define debug #f)

(define-generic db-delete)
(define-generic db-update)
(define-generic db-where)
(define-generic db-insert)

;;;
(define-class <db-stored-class-info> ()
  ((db-relation :init-keyword :db-relation
                ;:allocation  :each-subclass
                )
   ;; fixme: why is this here?
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

  ;; primary/unique keys?
  object)


;; given a gauche `object' (w/ slots),  store the values of `slots' in `relation'.
;; Mapping is ((slot . pg-attribute)...)
;; todo: SLOTS and MAPPING should be provided by the class?
(define-method db-insert ((object <db-stored-class-info>) data-object)
  (let ((mapping (slot-ref object 'attribute-mapping))
        (relation (ref object 'db-relation))
        (identificating-slots (find-full-intentification object data-object))
        )
    (let1 bound-mapping (filter (lambda (pair)
                                  (slot-bound? data-object (car pair)))
                          mapping)

      (assert (not (null? identificating-slots)))

      ;;db
      ;; todo: check that at least identifying set is provided!

      ;; I want:  convert the slot-values, and get the attnames:
      (let* ((attnames ())
             (value-list
              ;; sequentially
              (fold
               (lambda (pair value-list)
                 ;; Of course car is <pg-attribute>
                 (let* ((attribute (cdr pair)) ; (aget mapping slot))
                        (type (pg:attribute-type (cdr pair)))
                        (slot (car pair))
                        )

                   ;; insert into xx (......this....) VALUES (..that...)
                   (push! attnames (ref attribute 'attname))
                   (cons ((pg:printer-for type)
                          (slot-ref data-object slot))
                         value-list)))
               ()
               bound-mapping
               ))
             )
                                        ;(pg:with-handle-of* relation pg
                                        ;(pg-exec pg
        (sql:insert
         (name-of relation)

         value-list
         attnames)))))

;; `slot-values' is   ((slot . new-value) ....)
;; fixme: should accept a pg-handle
;; @returns SQL or commits it?
(define-method db-update ((object <db-stored-class-info>) data-object slot-values pg-handle)
  (DB "db-update: ~s\n" slot-values)
  (let ((relation (slot-ref object 'db-relation))
         (mapping (slot-ref object 'attribute-mapping))
         (identificating-slots (find-full-intentification object data-object))
         )
  ;; fixme: maybe this should avoid using those which change? or not!
  ;(logformat "db-update: ~a\n" mapping)
    (let1 query
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
         ;; where:
         (db-where-internal relation mapping data-object identificating-slots))

      (pg-exec pg-handle query)
      ;; now:
      ;; update the object itself!
      (for-each
          (lambda (slot-value)
            (DB "db-update: ~a ~a <- ~a\n"
                (car slot-value)
                (cdr slot-value)
                (slot-ref object (car slot-value)))
            ;; Overwrite:
            (slot-set! object (car slot-value) (cdr slot-value)))
        slot-values))))

;;  DESC is  db-stored-class-info:
;;  data gives the p-key data!
;;  and the connection!
(define (db-where-internal relation mapping object :optional slot-subset)
  ;;(let ((mapping (slot-ref desc 'attribute-mapping)))
  (DB "db-where-internal: ~a and complete: ~a\n" mapping (ssm-complete mapping))
  (if (undefined? slot-subset)
      (set! slot-subset (pg:primary-key-of relation)))

  (string-join
      (map
          ;; type !!!!
          (lambda (attribute)
            (DB "db-where-internal looking for ~s\n" attribute)
            (let* ((slot (car (rassoc attribute mapping))) ; getting #f without problem is not good!
                   (type (pg:attribute-type  attribute)))
              ;;
              (if (slot-bound? object slot)
                  (string-append
                   (pg:name-printer (ref attribute 'attname))
                   " = "
                   (scheme->pg type (slot-ref object slot)))
                (error "db-where-internal attribute from primary key is not bound:" slot))))
        slot-subset)
      " AND "))

;; returns: list of pg-attributes
;; db-coordinates
(define (find-full-intentification object data-object)
  (assert (is-a? object <db-stored-class-info>))
  ;; find the list of populated slots in `data-object' which can locate it in the PG relation.
  ;; so data-object answers -- is the slot bound?

  (let ((relation (slot-ref object 'db-relation))
        ;;  (attribute . slot-name)
        (mapping (slot-ref object 'attribute-mapping)))

    (let1 available-mapping
        (filter (lambda (pair)
                  (slot-bound? data-object (car pair)))
          mapping)

      (DB "available-slots ~s\n" available-mapping)

      (let1 unique-keys
             ;; (pg:with-handle-of rel-info handle
             ;; fixme: this should memoize!
          (pg:load-unique-indices (slot-ref object 'handle)
                                  relation)
        ;; filter
        (find
         (lambda (tuple)
           (DB "testing ~a\n" tuple)
           ;; subset of available-slots
           (DB "against ~a\n" (alist->values available-mapping))
           (lset<= eq? tuple (alist->values available-mapping)))
         unique-keys)))))

(define-method db-where ((object <db-stored-class-info>) data-object)
  (db-where-internal (slot-ref object 'db-relation)
                     (slot-ref object 'attribute-mapping) data-object))

;; todo:    db-delete-and-backup
;;
(define-method db-delete ((object <db-stored-class-info>) data-object)
  (logformat "db-delete ~s\n" data-object)
  (let* ((relation (slot-ref object 'db-relation))
         (mapping (slot-ref object 'attribute-mapping))
         )
    (let1 identificating-slots (find-full-intentification object data-object)
      (string-append
       "DELETE FROM " (name-of relation)
       " WHERE "
       ;; identificating-slots
       ;; relation mapping object :optional slot-subset
       (db-where-internal relation mapping data-object identificating-slots)
       ";")
                                        ;(error "cannot delete")
      )))


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

                        ((pg:printer-for type)
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
