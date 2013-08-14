;; hierarchical model!
(define-module pg.recordset
  (export
   <recordset>
   rs-get-row
   rs-for-each
   rs-map
   rs-compose-query
   rs-force                             ;fixme!  remove this?
   rs->result
   ;;
   <rs-row>                             ;data ?
   rs-row-get rs-row-resolve
   rs-row-inserted?
   attribute-present?
   ;;
   rs-get-link

   rs-get-tuple
   ;;
   pg:link+row->rs
   pg:get-fk-linked-rs

   ;;
   get-linked-rs get-linked-rs-1
   ;;
   ;; recordset-get


   ;;

   pg:row+tuple->key
   )

  (use mmc.log)
  (use mmc.simple)
  (use adt.string)
  (use adt.alist)
  ;(use macros.reverse)
  (use pg.database)
  (use pg.result)
  (use pg.links) ;; it seems unused!  no `<pg-link-fkey>' !!
  (use pg.sql)
  (use pg-hi)
  (use pg.types)
  (use util.list)

  ;;
  (use gauche.collection)
  ;; (use gauche.sequence)
  )

(select-module pg.recordset)

(define debug #f)

(define (possibly-join v)
  (if (pair? v)
      (string-join
          v
          ", ")
    v))


;; Getting the data:

;;(get p "nome")

(define-class <recordset> (<collection>)
  (
   ;; when we search for a value, it might have been fixed in the parent, hence somewhat available!?
   (parent :init-keyword :parent)

   ;;
   (database :init-keyword :database)


   ;;; These form the `query:'
   (query)
   ;; either list or alist ?  aliases is just a complication! I want numbers?
   ;;
   (relations :init-keyword :relations)

   ;; mmc: Useful?
   ;;(join)

   ;; SELECT  `what'
   (select :init-keyword :select)                               ; general expression
   (attributes :init-keyword :attributes) ;list of (relid . attnum) or attnames, or A.attname, or ?
   ;;
   ;;
   (qbe :init-keyword :qbe)             ; ((attnum  . value) ...)
   (where :init-keyword :where)
   ;;
   (order :init-keyword :order)
   (group-by :init-keyword :group-by)
   ;;
   (limit :init-keyword :limit)
   (offset :init-keyword :offset)



   ;;; These keep the `result' and calculations on it:
   ;; post-query-execution:
   (result :init-keyword :result)
   ;; executed recordsets ?
   (links)

   ;; hacks:
   (inserted-rows) ;; list of <rs-inserted> ?
   ))



;;; Composing the query:
(define-method object-apply ((rs <recordset>))
  (rs-force rs))

(define (rs-force rs . really?)
  (unless (and (slot-bound? rs 'result)
               (null? really?))
    (let1 query (rs-compose-query rs)
      (if debug (logformat "rs-foce: query = ~a\n" query))
      (pg:with-handle-of* (ref rs 'database) handle
        (slot-set! rs 'result (pg-exec handle query)))
      ;; Analyze
      )))




;; fixme: Why here?
(define (qbe->where relations qbe)
  (if debug (logformat "qbe->where: ~a\n" qbe))
  (string-join
      (map
          (lambda (i)
            (let ((attnum (car i))
                  (value (cdr i)))
              (let ((attribute (pg:nth-attribute relations attnum)))
                (s+
                 (pg:name-printer (pg:attribute-name attribute))
                 " = "
                 (scheme->pg (pg:attribute-type attribute) value)))))
        qbe)
      " AND "))





(define (ref-valid rs slot default)
  (if (and (slot-bound? rs slot)
           (slot-ref rs slot))
      (slot-ref rs slot)
    default))

;;; The Search
(define (rs-compose-query rs)
  (if (slot-bound? rs 'query)
      (slot-ref rs 'query)
    ;; (if 'where bound but also some other .... error?

    (begin
      (if debug (logformat "rs-compose-query: ~a\n" rs))
      (let ( ;; Get the select part:
	    (select
	     (cond
	      ((slot-bound? rs 'select)
	       ;;
	       (possibly-join (slot-ref rs 'select)))
	      ((slot-bound? rs 'attributes)
	       (possibly-join (slot-ref rs 'attributes)))

	      ;; fixme!  This should remove Fixed attributes! And somehow i should add them
	      ;;  to the hash of
	      (else "*")))
	    ;; Get the relations
	    (relations
	     (cond
	      ((slot-bound? rs 'relations)
	       (let1 rel (slot-ref rs 'relations)
		 (cond
		  ((string? rel)
		   rel)
		  ((is-a? rel <pg-relation>)
		   (slot-ref rel 'name))
		  ((pair? rel)
		   (string-join rel ", "))

		  (else
		   ;; not a list:
		   '(map
			(lambda (r)
			  (ref r 'name))
		      (slot-ref rs 'relations))
                                        ;(slot-ref rs 'relations)
		   (error "what relation?")
		   ))))
	      ;; 'join ?
	      (else
	       #f)))

	    ;; Get the Where
	    (where
	     (cond
	      ((slot-bound? rs 'where)
	       (possibly-join
		(slot-ref rs 'where)))
	      ;;
	      ;; query-by-example
	      ;; ((numero 1309) ....)
	      ((slot-bound? rs 'qbe)	; list of constraint on
                                        ;(logformat "qbe: ~a\n" (slot-ref rs 'qbe))
                                        ;(logformat "is:\n ~a\n"(qbe->where (slot-ref rs 'relations) (slot-ref rs 'qbe)))
	       (qbe->where
		(slot-ref rs 'relations)
		(slot-ref rs 'qbe))
                                        ;(sql:alist->where
                                        ;(slot-ref rs 'qbe))        ; attnums? or attnames?
	       )
	      ;; scheme QL ?
	      (else
	       #f)))
	    ;; order!
	    (order (and (slot-bound? rs 'order)
			(slot-ref rs 'order)))

	    (group-by
	     (and (slot-bound? rs 'group-by)
		  (slot-ref rs 'group-by))))
	;; simple:
	(let1 query (sql:select
		     select
		     ;;
		     relations
		     where
		     order
		     (ref-valid rs 'limit #f)
		     (ref-valid rs 'offset #f)
		     group-by)
	  (slot-set! rs 'query query)
	  query)))))


;;; Processing the result:

(define (rs->result rs)
  (rs-force rs)
  (if debug (logformat  "rs->result ~a\n"
              (pg-status-status
               (pg-result-status (ref (ref rs 'result) 'result)))))
  (ref rs 'result))

;;; The  <collection> API implementation:
(define-method call-with-iterator ((rs <recordset>) proc . args)
  ;;
  (rs-force rs)
  (if debug (logformat "call-with-iterator on ~a! ~d tuples\n" rs (pg-ntuples (ref rs 'result))))
  (let ((max-row (pg-ntuples (ref rs 'result)))
        (i 0))
    (proc
     ;; test
     (cut >= i max-row)
     ;; get
     (lambda ()
       (if debug (logformat "call-with-iterator tuple ~d\n" i))
       (begin0
        (rs-get-row rs i)
        (inc! i))))))


;; run   (function row)
;; fixme: This should be  (for-each-with-index function rs) !!!  But the args are reversed!
(define (rs-for-each rs function)
  (if #t
      (for-each-with-index
          (lambda (index row)
            (function row index))
          rs)
    (begin
      (rs-force rs)
      (for-numbers<* i 0 (pg-ntuples (ref rs 'result))
        (function
         (rs-get-row rs i))))))


;; collect (function row)
(define (rs-map rs function)
  (if #t
      (map
          function
        rs)
    (begin
      (rs-force rs)
      (map-numbers* i 0 (pg-ntuples (ref rs 'result))
        (function (rs-get-row rs i))))))

;; fixme:  Can i collect into a collection?







;;; hacks:

(define(rs-row-inserted? row)
  #f)




;;; Row
(define-class <rs-row> ()
  (
   (convert-eof-to-empty-string :init-value #t)
   (recordset :init-keyword :recordset)
   (index :init-keyword :index)
   ))

(define (rs-get-row rs i)
  ;; should force the search!
  (rs-force rs)
  (if (> i (pg-ntuples (ref rs 'result)))
      (errorf "rs-get-row: row number ~d not available!" i)
    (make <rs-row>
      :recordset rs
      :index i)))


;;; Row:
(define (rs-row-get row colname)        ;colname !
  (if debug (logformat "rs-row-get: ~a\n" colname))
  (let1 result (ref (ref row 'recordset) 'result)
    (pg-get-value result (ref row 'index) (pg-fnumber result colname))))


(define-generic attribute-present?)

;; is the attribute name "Not NULL" ?
(define-method attribute-present? (name (row <rs-row>))
  (let* ((result (ref (ref row 'recordset) 'result))
         (index (pg-fnumber result name)))
    (and (not (= index -1))
         (not (pg-get-isnull result (ref row 'index) index)))))




(define-method object-apply ((row <rs-row>) name . strict?) ; #f -> strict ? #t -> push ""
  (let* ((result (ref (ref row 'recordset) 'result))
         (index (if (number? name)
                    name
                  (pg-fnumber result (x->string name)))))
    (if (not (= index -1))
        (let1 val (pg-get-value result (ref row 'index) index)
          ;; fixme: do it only if some row attribute, or some parameter requests it.
          (if (eof-object? val)
              (if (if (null? strict?)
                      (slot-ref row 'convert-eof-to-empty-string)
                    (car strict?))
                  ;; [21 mag 06]    When I ask for numbers, there is no difference between testing for EOF and "".
                  ;; This trick helps only for string, which is not worth it!
                  ""
                val)
            val))
      (errorf "the <rs-row> does not have column ~a" name)
                                        ;(rs-get-link row relation fattribute attribute)
      ;; link ->
      )))



;; out-of-the result .... get the value of relation/attnum.
;; why is this useful?  fixme!  todo: I should use an index rather than relation. Think about
;;     2 same relations joined!!

;; fixme: should the result have a hashtable?
;; we need tuple!
(define (rs-row-resolve tuple row attribute) ;relation attnum
  ;; if more than 1  error!
  ;; look at the result:
  (pg-tuple:solve-value tuple (ref row 'index) attribute)

  #;
  (let* ((result (ref (ref row 'recordset) 'result))
         (relid (ref relation 'oid))
         (column
          ;; (find-in-numbers
          (let step ((column 0))
            (cond
             ((=  column (pg-nfields result))
              #f)
             ((and
               (= relid
                  (pg-ftable result column))
               (= attnum (pg-ftablecol (ref result 'result) column)))
              column)
             (else
              (step (+ 1 column)))))))
    (if column
        ;; -string
        (pg-get-value result (ref row 'index) column)
      ;; fixme: throw! not error?
      (error "couldn't resolve")))
  )



;; obsolete:
;; mmc: but might be mutually-recursively called from `rs-row-get' !
;(define (link? a)
;  #f)

#;
;; (define (rs-get data path)
;;   (rs-force rs)

;;   ;; path is ("-lingue" 2 "quanto")
;;   (let1 direction (if (pair? path) (car path) path)
;;     (cond
;;      ((link? direction)
;;       (recordset-get
;;        ))
;;      ((number? direction)
;;       ;; recordset tuple.
;;       )
;;      (else
;;       (let1 result (ref data 'result)
;;         (pg-get-value* result 0 (pg-fnumber result direction)))
;;       )))))

;(define recordset-get rs-get)


;;;  links need hypergraph ?.


(define db-programs (list ))

;; todo:
;; This is my 0-generation  hack.  will be obsoleted!
(define (rs-get-link row relation fattribute attribute)
  (make <recordset>
    :database (ref (ref row 'recordset) 'database)
    :relations relation
    :attributes "*"
    :where (s+ fattribute " = "
               ;; pg:print
               (x->string (rs-row-get row attribute)))))

;;; tuples & links
;; This is for recordsets!
(define-generic pg:link+row->rs)

;; todo: for now i create <pg-link-fkey> explicitely, i don't use the hypergraph yet.
;; note:  pg:plug  is a nicer name!
(define-method pg:link+row->rs ((link <pg-link-fkey>) tuple row) ;fixme: This should accept  rtuple!
  (let* ((fkey (slot-ref link 'fkey))
         (slave-r (slot-ref fkey 'slave))
         (master-r (slot-ref fkey 'master)))
    ;; fixme: (assert (eq? (ref tuple 'relation) master-r)
    (make <recordset>
      :database (ref (ref row 'recordset) 'database)
      :attributes                       ;all but the fixed ones!
      "*"
      ;(lset-difference
      ; (attributes-of slave-r)
      ; (ref fkey 's-fields))
      :relations   slave-r              ;the slave one!
      :parent row
      :qbe (map
               (lambda (att master)
                 (cons att
                       ;; value of master in the row:
                       ;; fixme: this depends on the path (in the hypergraph) !
                       (rs-row-resolve  ;fixme: as-string!
                        tuple
                        row
                        ;; master-r master)
                        (pg:nth-attribute master-r master)
                        ;; att
                        )))
             (ref fkey 's-fields)
             (ref fkey 'm-fields))      ;fixme: This is as `attnum'!
      ;; :order
      )))



;;; Hi-level access to datasets --- links (by foreign key?)
;; RTUPLE:     rtuple->row->recordset
;;                   ->tuple
;; `So:'
;; ROW is from recordset
;; TUPLE is the number ........ todo: it's limited to 1 tuple only, now.
;; LINK-RELATION  ... fixme: should be name of the FK constraint! or better?
(define (pg:get-fk-linked-rs row tuple link-relation) ;rtuple
  (let* (;;; (row (ref rtuple 'row))
         (rs (ref row 'recordset))
         (database (ref rs 'database))
         ;; fixme!
         (result (ref rs 'result))
         (master (ref tuple 'relation))) ;(pg:find-relation database "person")
    (pg:link+row->rs
     (make <pg-link-fkey>
       :fkey
       (pg:fkey-between master
                        ;; (ref master 'database)
                        (pg:find-relation database link-relation)))
     tuple
     ;; fixme: This should get it from the rtuple!
     row)))



 ;;  ((1 . <pg-tuple>) ..... )


;;; Hi-level access to datasets --- links (by foreign key?)
(define (get-linked-rs-1 row tuple link-relation)
  (let* ((rs (ref row 'recordset))
         ;; (tuples (pg:query-extract-tuples
;;                   (ref rs 'database)
;;                   (ref rs 'result)))
         ;(tuple (cdr (pg:find-relation-tuple tuples "person")))
         ) ;fixme!
  (pg:get-fk-linked-rs row tuple link-relation)))


(define (rs-get-tuple rs relname)
  (pg-result:find-relation (ref rs 'database) (rs->result rs) relname)
  ;; (let* ((db (ref rs 'database))
  ;;          (relation (pg:find-relation db relname)) ;fixme: normalize!
  ;;          (tuples (pg:query-extract-tuples
  ;;                   db
  ;;                   (rs->result rs))))
  ;;     (aif i (find
  ;;             (lambda (i)
  ;;               (eq? (ref (cdr i) 'relation)
  ;;                    relation))
  ;;             tuples)
  ;;          (cdr i)
  ;;       ;; (aget tuples relname)
  ;;       (error "rs-get-tuple cannot find tuple from this relation" relname)
  ;;       ))
  )


(define-generic get-linked-rs)
(define-method get-linked-rs ((row <rs-row>) tuple-relname link-relation)
  (let* ((rs (ref row 'recordset))
         ;; (tuples (pg:query-extract-tuples
;;                   (ref rs 'database)
;;                   (ref rs 'result)))
         (tuple (rs-get-tuple rs tuple-relname)
                                        ;(cdr (pg:find-relation-tuple tuples tuple-relname))
                )) ;fixme!
  (pg:get-fk-linked-rs row tuple link-relation)))



;; attributes ->  query (recordset)
'(define (define-link name . keywords)
  ;;add-to-list
  (push db-programs
        (make
            )
        ))




;;   val1#val2#val3
(define (pg:row+tuple->key tuple row)       ;row is from recordset!
  (let* ((relation (ref tuple 'relation))
         (pkey (pg:pkey-of relation)))
    (string-join
        (map
            (lambda (pg-attribute)
              ;; fixme:
              ;;(row pg-attribute)
              (x->string                ;fixme! date!
               ;;
               (pg-tuple:solve-value
                tuple (ref row 'index)
                pg-attribute)))
          pkey)
        "#")))





(provide "pg/recordset")
