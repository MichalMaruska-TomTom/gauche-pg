;;  from `mdb' ??

;; todo:  relname could be <pg-relation>

;; todo: drop the sql: prefix.
;;
;; transformations:
;;  scheme -> string
;;  alist -> conditions
;;  and or
;; so for now...not done implicitely!
;; todo:  -q version which quotes.
(define-module pg.sql
  (export

   sql:where
   ;; Conditions:
   sql-and:
   sql-or:
   sql:alist->where
   sql:between
   sql-condition:one-of
   sql-condition:char-in-string

   ;; Queries:
   sql:update
   ;; sql:update-old
   sql:delete

   sql:select-function
   sql:select-full
   sql:select sql:select-k

   sql:insert
   sql:insert-alist

   sql:insert-select
   ;; obsolete:
   sql:insert-into


   ;; hi-level:
   sql:get-tuples-alist
   sql:uniquefy-by-attnames-where
   )
  (use adt.string)
  (use adt.list)
  (use pg)
  (use pg-hi)
  (use pg.types)
  (use mmc.log)
  )
(select-module pg.sql)


(define (wrap-in-parens s)
  (s+ "( " s ")"))

;; fixme! I could use `fast-path' C-functionality.
(define (sql:select-function fname . args)
  (s+ "SELECT "
      fname "(" (string-join args ",") ");"))


;;; SELECTION
;; find tuples, which have certain FIXED attributes, and
;; THEN test some other condition from SCHEME
;; project on constant attnames (alist)

;; So, this function does not take care of the value formatting.
;; the @alist:  cdrs are taken literally, not even quoted!
;; so I can update  attr1 = attr2!
(define (sql:update relname values-alist where)
  (s+ "UPDATE " relname
      " SET\n"
      (string-join
          (map
              (lambda (item)
                (s+ (car item) " = " (cdr item)))
            values-alist)
          ",\n")
      "\n WHERE " where))


;; obsolete:  (att value . "-- comment")
#;
(define (sql:update-old relname values-alist where)
  (s+ "UPDATE " relname
      " SET\n"
      (string-join-2
       (map
           (lambda (item)
             ;; mmc: So, this function does not take care of the value formatting.
             ;; This must be solved elsewhere!
             (s+ (car item) " = " (cadr item) ""))
         values-alist)
       ","
       (map
           cddr
         values-alist)
       "\n"
       )

      "\n WHERE " where))

(define (and-where where)
  (if where (s+ " AND " where) ""))

;; a semi-tuple as alist constants !!!
(define (sql:get-tuples-alist relname tuple
			      :key (what "*")
			      where sort-by
			      :rest args)
  (let-optionals* args
      ((what-2 "*")
       (and-where-condition-2 #f)
       (sort-by-2 #f))
    ;; fixme:  
    (s+ "SELECT " what " FROM " relname
	" WHERE ( " (sql:alist->where tuple) " )"
	(and-where and-where-condition)
	(if sort-by (format #f " ORDER BY ~a" sort-by) "")
	";")))



;; fixme:
(define (sql:quote-name name)
  (cond
   ((string? name)
    name)
   ((symbol? name)
    (pg:name-printer
     ;(s+
     ;"\""
     (symbol->string name)
     ;"\""
     ))
   (else
    (x->string name))))


(define (sql:string-or-list->string info)
  (cond
   ((string? info)
    info)
   ((list? info)
    ;; fixme:
    (string-join (map sql:quote-name  info) ", "))))

'(define (sql:where info)
  (cond
   ((string? info) info)
   ))

;; fixme:   select relation what where !!! seems better!


;; todo:  version with keywords:
;; either
;; (sql:select-k WHAT)
;; (sql:select-k WHAT FROM)
;; (sql:select-k WHAT FROM :where WHERE ....)
(define (sql:select-full what from where group-by order-by limit offset)
  (DB "sql:select-full: what ~a\n\tfrom: ~a\n" what from)
  (string-join-non-f
   (list
    "SELECT "
    (sql:string-or-list->string what)
    ;; new-line
    ;; fixme: could be non-necessary! bug!
    " FROM " (sql:string-or-list->string from)
    (and-s+ " WHERE " where)
    (and-s+ " GROUP BY " group-by)
    (and-s+ " ORDER BY " order-by)
    (and-s+ " LIMIT " (and limit (number->string limit)))
    (and-s+ " OFFSET " (and offset (number->string offset)))) ""))

;; I think the best API:
(define (sql:select-u what
		      :optional relname
		      :key from where group-by order-by limit offset
		      :rest args)
  ;; so args could be backup for keyword params. todo!
  (if (and from relname) (error ""))
  (sql:select-full what (or relname from)
		   where group-by order-by limit))

(define (sql:select-k what . rest)
  (let ((from #f)
        (k-rest ()))
    ;; take the FROM part:
    (unless (null? rest)
      (set! from (car rest))
      (set! k-rest (cdr rest)))
    (let-keywords* k-rest
        ((where #f)
         (group-by #f)
         (order-by #f)
         (limit #f)
         (offset #f))
      (sql:select-full what from where group-by order-by limit offset))))

;; WHAT can be a list of symbols/strings.     links ?
;; FROM is a list of relnames: symbols   todo:  <pg-relation> ?
;; WHERE
(define (sql:select what from . rest)
  (let-optionals* rest
      ((where #f)
       (order-by #f)
       ;; (more #f)
       (limit #f)
       (offset #f)
       (group-by #f))
    ;; (logformat "what: ~a\nfrom: ~a\nwhere: ~a\n order ~a\n" what from where order-by)
    (sql:select-full what from where group-by order-by limit offset)))


;; user specifies WHERE, but we need to add something.
(define (where-and where)
  (if where
      (s+ where " AND ")
    ""))

(define (subquery query)
  (wrap-in-parens query))

;; I want this api:
;; (relname where)
;; (relname :where where)
(define (sql:delete relname where)
  (s+ "DELETE FROM " relname " WHERE " where))


(define (aliased relname alias)
  (s+ relname " " alias))

;;; Hi-level

;; return a DELETE query, which will turn RELNAME
;; into unique by ATTNAMES.  those with least oid survive.
;; todo: make it generic.  not oid.
(define (sql:uniquefy-by-attnames-where relname where attnames)
  (let ((condition-and (where-and where)))
    (sql:delete
     relname
     :where
     (s+
      condition-and "exists "
      (subquery
       (sql:select "1"
		   :from (aliased relname "B")
		   :where
		   (s+
		    condition-and
		    (string-join
			(map (lambda (attname)
			       (format #f "( B.~a = ~a.~a ) " attname relname attname))
			  attnames) " AND ")
		    (format #f "AND B.oid> ~a.oid" relname))))))))

;;; `Insert'
(define (list->comma-string list)
  (string-join list ", "))


;;; Given values, add them to relname. possibly specify attnames:
(define (sql:insert relname values . rest)
  (let-optionals* rest
      ((attnames #f))
					;(logformat-color 'yellow "sql-insert: ~s\n" attnames)
    (s+ "INSERT INTO " (pg:name-printer relname)
	;;`pg:text-printer'
	(if (not attnames) ""           ;null?
	  (wrap-in-parens (list->comma-string
			   (map pg:name-printer attnames))))
	"\n VALUES (\n"
	(string-join values
	    ",\n")
	"\n)")))

;; I'd say
(define sql:insert-values sql:insert)


;; values given as ALIST:
;; separate alist -> list  list
(define (sql:insert-alist relname alist)
  (sql:insert
   relname
   ;; filter just  attnames
   (map cdr alist)
   ;; filter just  values
   (map car alist)))


;; @relname can be pair:  (relname . "(attrib1, attrib2)")
(define (sql:insert-select relname select-query)
  (s+
   "INSERT INTO "
   (cond
    ((string? relname)
     ;; fixme: counter the agreement here!
     (pg:name-printer relname))
    ((pair? relname)
     (s+
      (car relname)
      (cdr relname))))
   select-query))

(define sql:insert-into sql:insert-select)


;;; WHERE -- Conditions:

;; The conversion (scheme -> pg values-as-strings must be done before)
(define (sql:alist->where alist)
  (string-join
      (map (^i (s+ "(" (car i) " = " (cdr i) ")"))
	alist)
      " AND "))

(define sql:where sql:alist->where)


;;; Conditions:
(define (join-by l j)
  ;; note: was  `string-join'
  (wrap-in-parens (string-join-non-f l j)))


(define (sql-and: . rest)
  (join-by rest " AND "))

(define (sql-or: . rest)
  (join-by rest " OR "))

;; fixme: between works with non-numbers, doesn't it?
;; fixme: is the convention to create the conditions inside parens?
(define (sql:between attname min max)
  (s+
   attname ;; (pg:name-printer attname)
   " BETWEEN "
   (number->string min)
   " AND "
   (number->string max)))

;; returns SQL condition: ATTRIBUTE is/= one of POSSIBILITIES
;; Note: attribute (name) is quoted, and possibilities as well
(define (sql-condition:one-of attribute possibilities)
  ;;(logformat "sql-one-of: ~a\n" attribute)
  ;; (set! attribute (pg:name-printer attribute)
  (let1 attname-quoted (pg:name-printer attname)
    (wrap-in-parens
     (string-join
	 ;; todo: optimize as string tree + 1 composition!
	 (map (lambda (possible)
		;;(format #f "(~a = ~a)" attribute possible)
		(wrap-in-parens
		 (s+ attname-quoted " = " (pg:text-printer (x->string possible)))))
	   possibilities) " OR "))))



;; (define (sql:element-of attname set)    ;I should work with <pg-attribute> !!
;;   ;; if the attribute is a char, combine the set to a  string!
;;   (if (null? set)
;;       (begin
;;         (if debug (logformat "sql:element-of ~a\ ()!\n" attname))
;;         #f)
;;     (let1 attname-quoted (pg:name-printer attname)
;;       (s+
;;        "("
;;        (string-join
;;            (map
;;                (lambda (value)
;;                  (s+ attname-quoted " = "
;;                      (pg:text-printer   ;fixme!  I should avoid this now/here ?
;;                       (x->string value))))
;;              set)
;;            " OR ")
;;        ")"))))


;; SET can be a string, or a list:
(define (sql-condition:char-in-string attname set)
  (let1 attname-quoted (pg:name-printer attname)
    (cond
     ((not set)
      ;; no test/condition needed.
      #f)

     ;; Optimization:  special set: only 1 string, better use = instead of `text_contains'
     ((singleton? set)
      (s+ attname " = "  (pg:char-printer (car set))))

     ((or (equal? set ())
          (equal? set ""))
      #f)

     (else
      (s+
       "text_contains(\'"
       (pg:text-printer
	(if (string? set)
	    set
	  (apply s+ set)))
       ", "
       attname-quoted ")")))))


(provide "pg/sql")
