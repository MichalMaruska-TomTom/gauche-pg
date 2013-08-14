
;; Caching of pg_attribute.

;; todo: protocol to flush it.
;; server side generated Notices...

(define-module pg.attribute
  (export
   <pg-attribute>
   pg:attribute-name
   pg:attribute-type
   relation-of

   pg-attribute<
   )
  )
(select-module pg.attribute)

;;; todo: this might be done in C
(define-class <pg-attribute> ()
  ((relation :init-keyword :relation :accessor relation-of)
   (attnum :init-keyword :attnum)

   (attname :init-keyword :attname :accessor pg:attribute-name)
   (type :init-keyword :atttyp :accessor pg:attribute-type)))

(define (attribute->relid attr)
  (ref (ref attr 'relation) 'oid))

;; Ordering of <pg-attribute>s `inside' a pg-database !
(define (pg-attribute< attribute1 attribute2)
  ;; (assert same-databases todo!
  (unless (is-a? attribute1 <pg-attribute>)
    (errorf "pg-attribute<: ~a is not <pg-attribute>\n" attribute1))
  (let ((rel-oid1 (attribute->relid attribute1))
        (rel-oid2 (attribute->relid attribute2)))
    (or
     (< rel-oid1 rel-oid2)
     (and (= rel-oid1 rel-oid2)
          (< (ref attribute1 'attnum)
             (ref attribute2 'attnum))))))

(define-method write-object ((o <pg-attribute>) port)
  (format port
    "<~a: ~d/~a in ~a>"
    ;; "<"
    ;(string-drop-right (string-drop (class-name (class-of o)) 1) 1)
    "pg-attribute"

    (ref o 'attnum)
    (ref o 'attname)
    (or
     (ref (ref o 'relation) 'name)
     "??")))

(provide "pg/attribute")
