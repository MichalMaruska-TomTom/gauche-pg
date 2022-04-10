
;;;  Analyzing the pgresult object

;; Columns can be determined to come from table rows:
;; so we have a list of `<pg-tuple>'s and the mapping
;; pg-tuple & index (attribute thereof) -> index in the result (columns).

;; This is the mapping the user is interested in.
;; `pg-tuple:->result-column' maps  (N, I) -> X, so that
;;  pg-fcolumn(X) = I  AND pg_fsource(X) = N
;; to use, map over the list of <pg-tuples> !


;; The reverse mapping, the one we can build (at start):
;;  `split-result-into-tuples' pg:query-extract-tuples
;; So, for I-th column in the result, we can obtain:
;;  pg-fsource gives us the <pg-tuple>, and its attribute!
;;  pg-fcolumn  gives the index into ... <pg-tuple>
;;
;; What else about the pg-tuples's?
;; I can get: is-key-available? <pg-attribute>
;;


;; |-----| tupleA        |--------| tupleB
;; |-----------------------------------------------| result row

;;; old doc:
;;  look at the vector of columns.
;;  if a column is an attribute (not a complex expression), see, if there's a primary key.
;;  if yes, form an object `tuple', which has the pkey associated. (as alist?)


(define-module pg.result
  (export
   <pg-tuple>                           ;todo  don't export!
   ;; should be internal:
   ;; pg-result->tuple-order!

   pg:find-relation-tuple               ;hopefully not used!

   pg-tuple:solve-value
   pg-tuple:->result-column

   pg:attributes-in-tuple

   pg:query-extract-tuples
   pg-result:find-relation

   pg:nth-tuple pg:solve-tuple
   pg:solve-tuple-tower
   )
  (use mmc.log)

  (use util.list)                       ;rassoc
  (use pg-hi)
  (use pg)
  (use pg.database)                     ;What do we need?
  (use pg.relation)                     ;fixme: do we need only this one?


  ;; fixme:  this will need pg.views  also!
  ;; I could push the <pg-view> into  `database'
  ;; and have some method in a higher module, which uses this one: `result'.
  ;;
  (use mmc.simple)
  (use adt.alist)
  (use macros.reverse)
  (use macros.aif)
  (use srfi-1)
  )
(select-module pg.result)

(define debug #f)

;; Records presence of a subtuple (of a table in database) in a row of a query result
;; (`<pg-result>').
(define-class <pg-tuple> ()
  (;; Does this have the query (plan tree or plain sql)?
   (result :init-keyword :result)       ;back-link
   (index :init-keyword :index)         ; pg-fsource

   ;; This tells us if it's a VIEW! <pg-relation>
   (relation :init-keyword :relation
             :setter tuple-set-relation)

   ;; has-p-key? (boolean)  By default not present.
   (p-key :init-value #f)

   ;; This cannot be got from the <pg-result>. But maybe from the Query.
   ;; WHERE gender='m'  -> gender is fixed!
   ;; alist   ((<pg-attribute> . value) ...)
   (fixed-attributes)

   ;; The core:
   ;; ((index .  <pg-attribute>) ...) where INDEX indicates the `<pg-result>' columns.
   ;; <pg-attribute> is from the <pg-relation>
   ;; possibly ordered!
   ;; fixme: Should not be accessible! ...?
   ;; Should be named attribute-alist !?
   (attribute-index-alist :init-keyword :attribute-index-alist)

   ;; note: EXPLANATION
   ;; the <pg-attribute>s present in the tuple can be ordered.
   ;; Here we keep a mapping from 1,2...N to the position in the `result' associate
   ;; i.e.       (i,j) i-th from the tuple is @ j-th position in the result?

   ;; `pg:query-extract-tuples' scans the result, and constructs
   ;; this `attribute-index-alist'.
   ;; But then there is another pass, which sorts this alist
   ;; rewrites the slot
   ;; and make a `reverse'(??) mapping stored in `tuple->result-map':
   (tuple->result-map)

   ;; If the relation is a View (or once we walk the PLAN, rather than simple
   ;; `pg-fsource'), then this gives the next level:
   (subtuples :init-keyword :subtuples)))


;; mmc: I should find out, where we need these 2 ADTs.
(define-method write-object ((object <pg-tuple>) port)
  (format port "<pg-tuple::~a~a: ~a>"
          (slot-ref object 'relation)   ;name!
          (if (slot-ref object 'p-key)
              "-RW" "-RO")
          (slot-ref object 'attribute-index-alist)
          ;; attribute-index-alist
          ))

;;; Accessors:

;; return the column index in the pg-result, which contains the N-th attribute
;; of the @tuple.
(define (pg-tuple:->result-column tuple n)
  ;; fixme:
  (unless (slot-bound? tuple 'tuple->result-map)
    (DB "pg-tuple:->result-column: the tuple has no 'tuple->result-map\n")
    (pg-result->tuple-order! (ref tuple 'result) tuple))

  (vector-ref (ref tuple 'tuple->result-map) n))

;; return <pg-attribute>s in the relation of the tuple:
(define (pg:attributes-in-tuple tuple)
  ;; Fixme: and (fixed-attributes)?
  (alist->list-values (ref tuple 'attribute-index-alist)))



;;; Selecting one tuple out of the set:
(define (pg:nth-tuple tuple-set n)
  ;; or maybe (aget ts n)  !!!
  (cdr (list-ref tuple-set n)))

;; `Ugly_hack:' Given `pg-tuples' (of some pg-result), find the first one
;; which comes from RELNAME.
;; mmc: see `rs-get-tuple' in recordset.scm

;; todo: This should use a "finger" pointing at 1 result column.

;; @tuples is ((tuple-index . <pg-tuple>) ...)
;; returns first (tuple-index . <pg-tuple) which is from @relname
(define (pg:find-relation-tuple tuple-set relname)
  ;; bug!  this should at least raise error if more than 1 found!
  (logformat "pg:find-relation-tuple still HACKISH to search by relname only (~a)\n"
    relname)
  (let* ((db (ref
              (ref (car tuple-set) 'relation)
              'database))
         (relation (pg:find-relation db relname)))

    (find
     (lambda (info)
       (let ((index (car info))
             (pg-tuple (cdr info)))
         (eq? relation
              (slot-ref pg-tuple 'relation))))
     tuple-set)))


;; Given Attributes in the first tuple of TOWER,
;; return the `indices' of the last tuple of TOWER. (that's  attnums or pg-fnumber)
;; ex:

;;
;;     pg:solve-tuple
;; atts ------> numbers
;;                v    pg:get-attribute
;;              attributs
;;                 v    pg:solve-tuple
;;              numbers
;;


;; Given a TUPLE (from result to RELATION), and a list of ATTRIBUTES of relation
;; return list of the (column) indices in the result (whose pg-relcolumn are ATTRIBUTES).
(define (pg:tuple-2-result tuple attributes)
  (map (lambda (attribute)
         ;; r(a)get
         (car (rassq attribute (ref tuple 'attribute-index-alist))))
    attributes))

(define pg:solve-tuple pg:tuple-2-result)


;; Todo: This is inverse of ... and I should remove both ... thus the numbering
;; in views would be by colnums, not

;;   attnums!  i.e.  ((0 . <pg-attribute>) ...)  would be ((1 . <pg-attribute>)...)
;;

;; tower is a list of tuples. Immagine Views -- one base on the following one.
;; colnums is the initial list of indices.
;; returns the indices in the last tuple, which would map to the (initial) colnums.

;; So the view would be a result with Holes for missing (Dropped) attributes! + zero.
;; btw. what is attribute w/ attnum ZERO?  todo!
;; works with ATTNUMS! returns `COLNUMS'
(define (pg:solve-tuple-tower tower colnums)
  (DB "pg:solve-tuple-tower: ~a\n" colnums)
  ;; project by alist (and iterate)
  (fold (lambda (tuple colnums)
          ;; map atts through ... to numbers!
          ;; Now map the number to atts?
          (let1 attributes (map-reverse colnums

                             ;; Bug: This is again buggy!     colnum != attnum
                             (lambda (colnum)
                               (let1 relation (ref tuple 'relation)
                                 ;; fixme: Maybe this should be done?
                                 (pg:nth-attribute
                                  relation
                                  (pg:real-nth-attribute
                                   ;; (pg:attnum->attribute
                                   relation colnum)))))
            ;; fixme: this is  !
            (pg:tuple-2-result tuple attributes)))
   colnums
   tower))

(define (same-relation-attribute< a b)
  (< (slot-ref a 'attnum)
     (slot-ref b 'attnum)))

(define (same-relation-attribute= a b)
  (= (slot-ref a 'attnum)
     (slot-ref b 'attnum)))


;; fixme: not efficient. When needed, prepare the indices and then use them in bulk!
;; attributes is either N for N-th attribute in the tuple
;; or <pg-attribute>, then ....
(define (pg-tuple:solve-value tuple row-index attribute)
  ;; attribute is <pg-attribute> (& must be in the tuple!)
  (let* ((tuple-attributes (ref tuple 'attribute-index-alist))
         (column-index
          (if (number? attribute)
              (pg-tuple:->result-column tuple attribute)
            ;; rassoc ?
            (let1 info (rassq attribute tuple-attributes)
              (unless info
                ;; not found. so 2 different object and rassq is using eq?...
                (let1 guess (cdr (find
                                  (lambda (i)
                                    (same-relation-attribute= (cdr i) attribute))
                                  tuple-attributes))
                  (errorf "Could not find: ~a in ~a\n Only ~a\n"
                          attribute
                          tuple-attributes (eq? guess attribute))))
              (car info))))
         (value (pg-get-value
                 (ref tuple 'result)
                 row-index
                 ;; fixme: or tuple->result-map ?
                 column-index)))
    (DB "pg-tuple:solve-value: ~a ~a/~a ->~a\n"
        row-index column-index (pg-fname (ref tuple 'result) column-index)
        value)
    value))


;;; Constructing the `<pg-tuple>:'

;;; Sometimes/always? we need to quickly get the column index of a given (tuple) attribute.
;;  pg-attributes can be thought as ordered ... by `attnum'
;;; So construct a vector, which maps:
;;  attribute -> index in pg-result columns.

;; Given a <pg-tuple> (relation & indices) order the indices.
;;
;; They are (index-in-result . atribute) .... unordered-- well, they might be
;; in descending order.
;;
;; so, we want, these mapping quick:
;;  index -> attribute
;; the `reverse' mapping:
;; pg-attribute -> result column
;;
;; todo: This might be optimized:
;; the pg-attributes have the same relation, the order function does not need
;; to check that coordinate!


;; Also, the order might be re-used by other functions!?
(define (pg-result->tuple-order! result tuple) ;fixme: (eq tuple 'result)
  (define get-result-index car)
  (define get-attribute cdr)

  (if (slot-bound? tuple 'tuple->result-map)
      (logformat "pg-result->tuple-order called another time! Useless!"))
  ;; attribute -> position in the RESULT
  (let* ((attribute-alist (ref tuple 'attribute-index-alist))
         ;;  ((index . attribute) ....)   [index as the column in the result]
         (rev-mapping (make-vector (length attribute-alist) #f))
         (ordered (sort
                   attribute-alist
                   (lambda (a b)
                     ;; same relation, so no point in using the generic pg-attribute<.
                     (same-relation-attribute<
                      (get-attribute a)
                      (get-attribute b))))))
    ;; in this `rev-mapping':
    ;; N-th attributes  is  ({index in the result} . <pg-attribute>)
    ;; So, i put in the vector at N the {index...}
    ;; So, now I can just get the formula in terms of Ns, the ordering of pg-attributes

    ;; why is this `useful'?  1st attribute of the tuple is at the position
    ;; rev-mapping[1] in the result but what is the 1st attribute!? it's by the
    ;; ordering of <pg-attributes> of the relation.
    (fold
     (lambda (next index)
       (vector-set! rev-mapping
                    index
                    (get-result-index next))
       (+ 1 index))
     0
     ordered)
    ;; mmc: there probably is no need to keep this alist in order of index-into-pgresult, so:
    (slot-set! tuple 'attribute-index-alist ordered)
    (slot-set! tuple 'tuple->result-map rev-mapping)))


;; Return an alist
;; ((1 tuple) ....)
(define (split-result-into-tuples database result)
  ;; handy function used during the first scan.
  ;; Add into TUPLE, the fact, that column number INDEX (in `result'),
  ;; is attnum (of the relation of the TUPLE).
  ;; of course, tuple has a `back-link' to the <pg-result> ???
  (define (tuple-add-index tuple index attnum)
    ;; if twice? cannot happen!
    ;; (logformat "tuple-add-index\n")
    (slot-push! tuple 'attribute-index-alist (cons index attnum)))


  (define (report-found-attribute relation-oid table-col i relation-index)
    (let1 relation (if (zero? relation-oid)
                       "-UNKNOWN RELATION-"
                     (pg:get-relation-by-oid database relation-oid))
      (logformat-color 227
          "column ~d is from (index ~d): ~a ~a\n" i relation-index relation
          (if (is-a? relation <pg-relation>)
              (pg:nth-attribute relation table-col)
            "---"))))


  (let1 tuples '()
    ;; fold!!
    (DB "Extract (keyed) tuples from ~a\n" result)
    (for-numbers<* i 0 (pg-nfields result)
      ;; pg-fsource is method!
      (let1 relation-index (pg-fsource result i)

        (when debug
          (report-found-attribute (pg-ftable result i) (pg-ftablecol result i)
                                  i relation-index))
        (when (and (> relation-index -1) ;; fixme:
                   (> (pg-ftable result i) 0))
          (let1 tuple (aget tuples relation-index) ; could be a vector?

            ;; we have attributes!
            ;; Also, we want to know if some other (non-selected) attributes are Fixed,
            ;; hence of known value!
            ;; we keep:   (fsource  (index . attname-in-the-table) ....)
            ;;
            (let1 relation (if tuple
                               (ref tuple 'relation)
                             (pg:get-relation-by-oid
                              database
                              (pg-ftable result i)))

              ;; fixme: <pg-view> fits too!
              (if (not (is-a? relation <pg-relation>))
                  (DB "result: ~d is not from a relation, it is from: ~a\n" i relation)
                ;;
                (begin
                  (let1 attribute (pg:nth-attribute relation (pg-ftablecol result i))
                    (unless tuple
                      (let1 new-tuple (make <pg-tuple>
                                        :result result ; link back!  fixme: needed?
                                        :index relation-index   ;; pg-fsource number
                                        :relation relation
                                        ;; I'll add it below:
                                        :attribute-index-alist ())
                        (DB "pushing new tuple! ~a ~a\n" relation-index new-tuple)
                        (push! tuples (cons relation-index new-tuple))
                        (set! tuple new-tuple)))

                    ;; add the index to that tuple
                    (DB "result: ~d is from ~a\n" i attribute)
                    ;; fixme:  So we provide *i* rather than ????
                    (tuple-add-index tuple i attribute))

                  (when #f ;; too noise/verbose
                    (DB "\t~a -> tuple ~d (reliod ~d): ~a\n"
                        (pg-fname result i)
                        (pg-fsource result i)
                        (pg-ftable result i)
                        tuple)))))))))
    tuples))

;; Find the relation & `pkey'-ness.
(define (analyze-tuple! tuple)
  ;;(logformat "processing tuple number ~d, looking for the p-key:\n" (car tuple))
  ;;(logformat "taking its first attribute ~d -> \n"
  ;;    (cdar (ref tuple 'attribute-index-alist)))
  (let* ((relation (slot-ref tuple 'relation))
         ;;(rel-oid (pg-ftable result (caar (ref tuple 'attribute-index-alist)))) ;fixme!
         ;;(relation (pg:get-relation-by-oid database rel-oid))
         )
    ;;(logformat "-> ~d\n" (ref relation 'name))
    ;; find the primary key. and the relation!
    ;;(tuple-set-relation tuple relation)
    ;; is the p-key among he attributes?
    (let ((p-key (ref relation 'p-key)))
      (DB "Testing p-key presence! ~a\n" p-key)
      (when (and p-key
                 (lset<= eq? p-key      ;subset
                         (map cdr (ref tuple 'attribute-index-alist))))
        (slot-set! tuple 'p-key #t)
        (DB "tuple has primary key!\n")))))


;; returns an alist of tuples:
;;  ((tuple-index  .  <pg-tuple>) ...)
;;  `tuple-index' comes from the interval   1 ... number of participating tables
;;  in the projection that is the `pg-fsource'
(define (pg:query-extract-tuples database result)
  ;;fixme: isn't database provided by result?

  ;; get a list of tuples, and a list of non-assigned attributes.
  ;; Should a column be an object?

  ;; (if (is-a? result <pgresult>)
  ;;    (set! result (result-of result)))
  (unless (is-a? result <pgresult>)
    (errorf "pg:query-extract-tuples on ~a\n" result))

  (let1 tuples (split-result-into-tuples database result)

    (DB "\n")

    ;; Find primary keys & ...
    (for-each (lambda (tuple)
                (analyze-tuple! (cdr tuple)))
      tuples)
    ;; fixme:
    ;; I want to associate this analysis with the result!  here, not in the caller!
    (slot-set! result 'tuples tuples)
    tuples))


;; Todo: walk the tree (breadth first) and FIND!
(define (pg-result:find-relation db result relname)
  (let* ((relation (pg:find-relation db relname)) ;fixme: normalize!
         (tuples (pg:query-extract-tuples
                  db
                  result)))
    (aif i (find
            (lambda (i)
              (eq? (ref (cdr i) 'relation)
                   relation))
            tuples)
         (cdr i)
      ;; (aget tuples relname)
      (error "pg-result:find-relation: cannot find tuple from this relation" relname))))



(provide "pg/result")
