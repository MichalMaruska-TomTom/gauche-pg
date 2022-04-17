(define-module pg.db-stored-class
  (export
   <db-stored-class>
   slots-of-mapping

   db-delete
   )
  (use adt.alist)
  )

(select-module pg.db-stored-class)

;; todo: I would like to define it as a metaclass. But I'm not good at the OO system yet.

;;; metaclass  where attributes have a specification for the backing  DB attribute

;; then I can have methods:  db-remove, db-insert ...

;; class-variable points here:

;; I don't use this anymore ?  b/c the entire class has the _same_ settting.
;; unused!
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


;; what is mapping?  Alist?
(define slots-of-mapping alist->keys)


;; `Unused!'
;; return a Scheme symbol
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


;; (define (slot-values object slot-list) 1)
(define-method db-delete ((object <db-stored-class>))
  (let* ((rel-info (slot-ref object 'db-relation))
         (mapping (slot-ref object 'attribute-mapping))
         ;(db (->db object))             ; get it !!

         (p-key (slot-ref rel-info 'p-key)) ;not bound ->  error !

         (slots (alist-project p-key (ssm-inverse mapping))))
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

(provide "pg/db-stored-class")
