
;;; Why we keep `more' hypergraphs:
;; I have a set of rules, functional dependencies between <pg-attributes>

;; Todo: This modules creates some data types, which might be isolated
;; from the code which invokes `adt.hypergraph' ... which I might avoid using!

;; fixme:  this part is obsolete?
;; Some attributes are special, and have some info attached.
;; I am given a set of pg-attributes, and want to obtain:
;;  -  all the information reachable
;;  -  the paths. i.e the functions of the f. dependencies.
;;
;; Since the set of rules can be enormous
;;   (think that every primary key is a "key -> attribute" rule)
;; I want to keep the hypergraph limited to relevant nodes /directions.
;; That means only directions which bring us to some information attached.

;; So, I should keep separate hypergraphs for different domains of information!


;; This file is (now) `only' about Foreign-keys!

;; todo:  Change the data from (relid . attnum)  to (<pg-relation> attnum) !


;; points  should not be a list. just 1 element!

;; `Functional-dependency':

(define-module pg.links
  (export
   pg:record-link	     ;; (pg:record-link db '(("person" "numero")) contatti-di)
   pg:find-links
   ;; obsolete: pg:find-reached
   ;;
   pg-make-hypergraph-on-attributes
   pg:convert-to-attributes
   ;;
   pg:prepare-fk-links
   ;;
   <pg-link>
   <pg-link-fkey> function-of
   )
  (use mmc.log)
  (use macros.reverse)
  (use adt.list)

  (use adt.hypergraph)                  ; hypergraph-connect  and  hypergraph-deduce or hyper-closure

  (use pg.database)
  (use pg.hi)
  ;; (use pg.recordset)
  (use srfi-1)
  )
(select-module pg.links)


(define debug #f)

(define-generic function-of)


;; `Directions' are
;; NODES are  (relid attnum)   fixme: why not <pg-relation> <pg-attribute> ??
;; this means, we are prone to renaming which is an easy op.
;; Changing attnum to data is difficult.

;; (relid attnum) is a domain?
;; (relid attnum ...attnum) ->   something ... recordset (possibly guaranteed to be a row).
;; ((relid attnum ...attnum) (relid attnum ...attnum)) ->


;; db could be provided by the relation, but we accept relnames.

;;; Utils:

;; help convert SPEC to <pg-attibutes>
;; SPEC is a list of:
;; -  <pg-attribute>  ..
;; - (relation/relname attname/attnum ....)
(define (pg:convert-to-attributes db spec)
  (DB "pg:convert-to-attributes")
  (append-map
   (lambda (dep)
     (if (is-a? dep <pg-attribute>)
         (list dep)                     ;fixme!
       (let1 relation (pg:normalize-relation db (car dep))
         (map (cute pg:normalize-attribute relation <>)
           (cdr dep)))))
   spec))



;;; I have several (disjoint) `hypergraphs' built on `attributes':
;;  mmc: true?
(define (pg-make-hypergraph-on-attributes)
  (hypergraph-new
   ;; ordering
   pg-attribute<
   ;; Equality:
   ;; fixme:
   eq?
   ;; Test ?
   (cute is-a? <> <pg-attribute>)))


;; What kind of links? between recordsets?
;; todo: make it a local variable

;;; The most generic
(define-class <pg-link> (<hypergraph-rule>)
  ((name)
   (recordset)
   ;; relation
   (init-recordset)
   ;; function, which takes the input values, and produces the WHERE... recordset.
   ;; attributes.
   ))


;; DEPENDENCY is a list of <pg-attributes>!          fixme: not yet! relation + indices
;; LINK is other set of attributes (each one is deducible)
;; INFO is <pg-link>
(define (pg:record-link db dependency points info)
  ;; fixme:  mutex!
    ;; todo: (with-locking-mutex* (ref db 'dependency-mutex) ; or some universal mutex of the DB?
    (unless (slot-bound? db 'dependency-hypergraph)
      (slot-set! db 'dependency-hypergraph
        (pg-make-hypergraph-on-attributes)))

  (let1 directions (pg:convert-to-attributes db dependency)
    (DB "pg:record-link: ~a  ---- > ~a   (~a)\n" directions points info)
    (let1 rule (make <hypergraph-rule>
                 :set directions
                 :provided points
                 :info info)
      ;; dependency is a list:
      ;;  ( (items) (items) ..)
      ;; items is relation spec & attribute(s) spec:
                                        ;(logformat "pg:record-link connecting ~a ->  ~a\n" dependency points)
      (hypergraph-connect
          (ref db 'dependency-hypergraph)
        rule))))


;; This is about our standard hypergraph: which contains just FK and
(define (pg:find-links db attributes)
  (if (slot-bound? db 'dependency-hypergraph)
      (receive (atts links)
          (hypergraph-reached (ref db 'dependency-hypergraph)
                              (pg:convert-to-attributes db attributes))
        (if debug (logformat "pg:find-links: ~a -> ~a\n" attributes links))
        links)
    ()))

;;fixme:  Is this obsolete?d  by pg:find-links?
;; (define (pg:find-reached db attributes)
;;   (hypergraph-reached (ref db 'dependency-hypergraph)
;;                       (pg:convert-to-attributes db attributes)))


;;; `Fkeys:'

;; specialized:
(define-class <pg-link-fkey> (<hypergraph-rule>)
  (
   (fkey :init-keyword :fkey)                               ;<db-f-key>
   ;(name)
   ;(recordset)
   ;; relation
   ;(init-recordset)
   ;; function, which takes the input values, and produces the WHERE... recordset.
   ;; attributes.
   )
  )


(define-method write-object ((hr <pg-link-fkey>) port)
  (format port
    "<fk: master ~a slave ~a>"
    (ref (slot-ref hr 'fkey) 'master)
    (ref (slot-ref hr 'fkey) 'slave)))


(define-class <pg-link-fkey-slave> (<hypergraph-rule>)
  ((fkey :init-keyword :fkey)))


(define-method write-object ((hr <pg-link-fkey-slave>) port)
  (display "<sla-master link: " port)
  ;; write-object
  (display (slot-ref hr 'fkey) port)
  (display ">" port))

(define-method function-of ((link <pg-link-fkey>))
  (if (= 1 (length (ref (ref link 'fkey) 's-fields)))
      identity
    (lambda args
      (apply values args))))

(define-method function-of ((link <pg-link-fkey-slave>))
  (if (= 1 (length (ref (ref link 'fkey) 's-fields)))
      identity
    (lambda args
      (apply values args))))



;; primary keys determine the rest of attributes  !  I could model it here too.

;; todo: This should accept the pg-links object !
;; useless for now?
(define (pg:prepare-fk-links db)
  (DB "pg:prepare-fk-links: using only the PUBLIC namespace!\n")
  ;; f-key:  slave -> foreign/master
  ;;         foreign/master -> recordset  names {relname}-of
  (for-each-reverse
      (pg:load-foreign-keys (pg:get-namespace db "public"))
    (lambda (fkey)
      (DB "exploring the F-key ~a\n" fkey)
      ;; fixme: this should be a link
      ;; fixme!
      (let ((s-fields (ref fkey 's-fields))
            (m-fields (ref fkey 'm-fields)))

        ;; mmc: now I insist on acyclicity!
        (when #f
          (pg:record-link
           db
           ;; from this set
           (list
            (cons
             (ref fkey 'master)
             (ref fkey 'm-fields)))
           ;; we get this _set_:    This is a problem:
           ;; but more give a `recordset'
           (if (singleton? s-fields)
               (list
                (pg:lookup-attribute db
                  (ref (ref fkey 'slave) 'oid)
                  (car s-fields)))
             '())
           (make <pg-link-fkey>
             :fkey fkey)))

        ;; link back:
        (pg:record-link
         db
         ;; from this set
         (list
          (cons
           (ref fkey 'slave)
           s-fields))

         (if (singleton? m-fields)
             (list
              (pg:lookup-attribute db
                (ref (ref fkey 'master) 'oid)
                (car m-fields)))
           '())
         (make <pg-link-fkey-slave>
           :fkey fkey))
        ;; (logformat "~a is not 1. Might be a recordset...\n" (ref fkey 'name))
        ;; recordset might be marked as row!
        '(if (and (singleton? s-fields)
                  (pkey? m-keys)))))))


;; Is it ok? it might be run more times!
(pg:add-database-hook "links"
    pg:prepare-fk-links)

(provide "pg/links")
