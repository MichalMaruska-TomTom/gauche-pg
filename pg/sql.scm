;;  from `mdb' ??

(define-module pg.sql
  (export
   sql:get-tuples-alist
   sql:select-function
   ;;
   ; sql-delete-alist-tuple
   ;; insert:
   ; insert-or-retrieve insert-overwrite ; insert-and-return
   sql:where
   
   sql:update   sql:update-old
   ;;
   sql:select sql:select-k
   ;;
   sql:insert
   sql:insert-alist                     ;(sql-insert-alist relname alist)
   sql:insert-into
   ;;
   sql:delete
   sql:uniquefy-by-attnames-where

   ;; Conditions:
   sql-and:
   sql-or:
   sql:alist->where
   sql:between
   sql-condition:one-of
   sql-condition:char-in-string
   )
  (use adt.string)
  (use adt.list)
  (use pg)
  (use pg-hi)
  (use pg.types)
                                        ;quoting!!!
  (use mmc.log)
  )
(select-module pg.sql)



;; fixme! I could use  fast-path ...to call a function inside the pg server!
(define (sql:select-function fname . args)
  (s+ "SELECT " fname
      "("
      (string-join args ",")
      ");"))


;;; SELECTION
;; find tuples, which have certain FIXED attributes, and THEN test some other condition from SCHEME
;; project on constant attnames (alist)


;; fixme: a hack    obsoleted by pg-escape-string or pg:text-printer !
(define (sql:quote object)
  (cond
   ((number? object)
    (number->string object))
   ((string? object)
    (pg:text-printer object))
   (else
    (error "sql:quote cannot quote object: " object))))


(define (sql:update relname values-alist where)
  (s+ "UPDATE " relname
      " SET\n"
      (string-join
          (map
              (lambda (item)
                ;; mmc: So, this function does not take care of the value formatting.
                ;; This must be solved elsewhere!
                ;; fixme:  (sql:quote )
                (s+ (car item) " = " (cdr item) "")) 
            values-alist)
          ",\n")                        ;fixme?
      "\n WHERE " where))


;; move to adt.strings?
(define (string-join-2 strings separator addends separator2)
  ;; empty?
  (let step
      ;; make a list  (s1 sep a1 sep2 s2 ..... aN)
      ((total-list ())
       (rest strings)
       (resta addends))

    (cond
     ((null? rest)
      (apply s+
             (reverse total-list)))
     (else
      
      (step
       (append!
        (if (null? (cdr rest))
            (list (car resta) (car rest))

          (list separator2 (car resta)
                separator (car rest)))
        total-list)

       (cdr rest)
       (cdr resta)
       ))
     )))

;; (string-join-2 '("strings" "a") "," '("-- a" "--b") "\n")


(define (sql:update-old relname values-alist where)
  (s+ "UPDATE " relname
      " SET\n"
      (string-join-2

       (map
           (lambda (item)
             ;; mmc: So, this function does not take care of the value formatting.
             ;; This must be solved elsewhere!
             ;; fixme:  (sql:quote )
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
(define (sql:get-tuples-alist relname tuple . args)
                                        ; verbose ?
  (let-optionals* args
      ((what "*")
       (and-where-condition #f)
       (sort-by #f))
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

;(sql:quote-name 

(define (sql:string-or-list->string info)
  (cond
   ((string? info)
    info)
   ((list? info)
    ;; fixme:  
    (string-join (map sql:quote-name  info) ", "))))



(define-syntax null-unless 
  (syntax-rules ()
    ((_ expr body ...)
     (if (not expr)
         ""
       (begin
         body ...)))))

'(define (sql:where info)
  (cond
   ((string? info) info)
   ))

;; fixme:   select relation what where !!! seems better!

;; todo:  version with keywords:
(define (sql:select-k what . rest)
  (let ((from #f)
        (k-rest ()))
    (unless (null? rest)
      (set! from (car rest))
      (set! k-rest (cdr rest)))
    (let-keywords* k-rest
        ((where #f)
         (group-by #f)
         (order-by #f)
         (limit #f)
         (offset #f))
      (let1 query
          (s+
           "SELECT "
           (sql:string-or-list->string what)
           ;; new-line
           " FROM "
           (sql:string-or-list->string from)

           (null-unless where
             (s+
              ;; fixme: 
              " WHERE " where))

           (null-unless group-by
             (s+
              ;; fixme:
              " GROUP BY " group-by))

           (null-unless order-by
             (s+
              ;; fixme:
              " ORDER BY " order-by))
           
           (null-unless limit
             (s+
              ;; fixme:
              " LIMIT " (number->string limit)))

           (null-unless offset
             (s+
              ;; fixme:
              " OFFSET " (number->string offset))))
        query)
      )))




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
       (group-by #f)
       )

    ;; (logformat "what: ~a\nfrom: ~a\nwhere: ~a\n order ~a\n" what from where order-by)
    (let1 query
        (s+
         "SELECT "
         (sql:string-or-list->string what)
         ;; new-line
         " FROM "
         (sql:string-or-list->string from)

         (null-unless where
           (s+
            ;; fixme: 
            " WHERE " where))

         (null-unless group-by
           (s+
            ;; fixme:
            " GROUP BY " group-by))

         (null-unless order-by
           (s+
            ;; fixme:
            " ORDER BY " order-by))

         

         (null-unless limit
           (s+
            ;; fixme:
            " LIMIT " (number->string limit)))

         (null-unless offset
           (s+
            ;; fixme:
            " OFFSET " (number->string offset)))
         )
      query)))

;; user specifies WHERE, but we need to add something.
(define (where-and where)
  (if where
      (s+ where " AND ")
    ""))

(define (sql:uniquefy-by-attnames-where relname where attnames)
  (let ((condition (where-and where)))
    (s+
     "DELETE from " relname " WHERE "
     condition
     "exists ( SELECT 1 from " relname " B "
     " WHERE " condition
     (string-join
	 (map (lambda (attname)
		(format #f "( B.~a = ~a.~a ) " attname relname attname))
	      attnames) " AND ")
     (format #f "AND B.oid> ~a.oid" relname)
     ") ;")))



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
	  (s+ "(" (list->comma-string
			      (map pg:name-printer attnames))
			 ")"))
	"\n VALUES (\n"
	(string-join values
	    ",\n")
	"\n)")))
      ;;(ignore-errors ;(display query)				;return #f ??


(define (sql:delete relname where)
  (s+ "DELETE FROM " relname " WHERE " where))

; values given as ALIST:
; separate alist -> list  list
(define (sql:insert-alist relname alist)
  (sql:insert
   relname
   ;; filter just  attnames
   (map cdr alist)
   ;; filter just  values
   (map car alist)))


(define (sql:insert-into relname select-query)
  (s+
   "INSERT INTO "
   (cond
    ((string? relname)
     (pg:name-printer relname))
    ((pair? relname)
     (s+
      ;; symbol?
					;(pg:name-printer (car relname))
      ;; fixme:   namespace.relname -> "namespace"."relname"
      (car relname)
      (cdr relname)
      )))
   select-query))







;;; (where) Conditions:

;; fixme: This needs strings as the values.
(define (sql:alist->where alist)
  (string-join (map
                   (lambda (item)
                     (s+ "(" (car item) "= " (cdr item) ")")) ;fixme:  (sql:quote )
                 alist)
      " AND "))

(define sql:where sql:alist->where)


;;; Conditions:

(define (join-by l j)
  ;; note: was  `string-join'
  (s+ "(" (string-join-non-#f l j) ")"))


(define (sql-and: . rest)
  (join-by rest " AND "))

(define (sql-or: . rest)
  (join-by rest " OR "))
  


(define (sql:between attname min max)
  (s+
   attname ;; (pg:name-printer attname)
   " BETWEEN "
   ;; fixme: 
   (number->string min)
   " AND "
   (number->string max)))


;; make an SQL condition:  ATTRIBUTE is/= one of POSSIBILITIES
;; Note: attribute (name) is quoted, and possibilities as well
(define (sql-condition:one-of attribute possibilities)
  ;(logformat "sql-one-of: ~a\n" attribute)
  (s+
   "( "
   (string-join
       ;; todo: optimize as string tree + 1 composition!
       (map (lambda (possible)
              (s+ "(" (pg:name-printer attribute) " = " (pg:text-printer (x->string possible)) ")")
                                        ;(format #f "(~a = ~a)" attribute possible)
              )
         possibilities) " OR ")
   ")"))



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
      #f)
     ;; Optimization:  special set: only 1 string, better use = instead of `text_contains'
     ((singleton? set)
      (s+ attname " = '" (car set) "'")) ; 
     
     ((or (and (pair? set)
               (null? set))
          (and (string? set)
               (string=? set "")))
      #f)

     (else
      (s+
       "text_contains(\'"
       (if (string? set)
           set
         (apply s+ set))     ;list->string
       "\', "
       attname-quoted ")"
       )))))


(provide "pg/sql")
