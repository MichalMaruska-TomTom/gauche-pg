
;; hard-wired  conversions between Scheme & Pg types.

(define-module pg.types
  (use pg)
  (use pg-low)
  (use adt.alist)
  (use adt.string)
  (use srfi-19)
  (use srfi-13)
  (use macros.assert)

  (export
   <pg-type>
   pg-init-types
   pg-type-hash

   ;; obsolete?
   pg:isodate-parser
   pg:timestamptz-parser
   pg:text-printer pg:char-printer  pg:number-printer
   pg:name-printer
   pg:bool-printer
   pg:array-printer

   pg:date-printer pg:isodate-printer
   ;;
   pg-array->list
   pg-type-array?
   pg-numeric-type? pg-date-type? pg-char-type?
   ;; non-automatic conversions:
   ; pg:parse
   ; scheme->pg
   scheme->pg
   )
  )
(select-module pg.types)

(define debug #f)

;; So i want the <pg-result> to have a vector of these:
(define-class <pg-type> ()
  ((oid :init-keyword :oid)
   (name :init-keyword :name)
   (printer :init-keyword :printer)
   (parser :init-keyword :parser)
   ;; other info:  (array?)
   ))

(define-method write-object ((type <pg-type>) port)
  (format port "<pg-type: ~a/~d>"
          (ref type 'name)
          (ref type 'oid)
          ;(length (ref result 'types)
          ))



; pg-type-name
; pg-result-prepare
; pg-printer
; pg-converter


;; how to add later new <pg-type>s?

;; lookup standard types, and associate the oid w/ our <pg-type> objects
;; fixme: should be shared by connections?
;; This is probably useless f.  the low leve pg-conn cannot keep this alist, so
(define (pg-init-types pgconn)
  (let1 alist (pg-result->alist
               (pg-exec-internal pgconn "SELECT oid, typname FROM pg_type"))
    (for-each
        (lambda (i)
          (set-car! i (string->number (car i))))
      alist)
    alist))

;; pg is low level! No. This function is obsolete!
#;
(define (pg-type-name pg oid)
  ;(logformat "pg-type-name: ~d\n" oid)
  (let1 h (slot-ref pg 'oid->type)
    (if (hash-table-exists? h oid)
        (hash-table-get h oid)
      (errorf "pg-type-name: unknown type, its oid is: ~d" oid))))
;; pg-find-type



;;; Bootstrap types for a <pg>
;;  create & return hash:  oid -> <pg-type>
(define (pg-type-hash pgconn)          ;<pg>
  (let ((result (pg-exec-internal pgconn "SELECT oid, typname FROM pg_type;"))
        (hash (make-hash-table 'eqv?)))
    (pg-foreach-result
        result
        '("oid" "typname")
      (lambda (oid typname)
        (set! oid (string->number oid))
        (let* ((type
                (make <pg-type>
                  :oid oid
                  :name typname
                  :printer (aget pg:type-printers typname)
                  :parser (aget pg:type-parsers typname))))
          (hash-table-put! hash oid type))))
    hash))


;;;  `standard' parsers:
(define (pg:bool-parser str)
  (cond ((string=? "t" str) #t)
        ((string=? "f" str) #f)
        (else (error "Badly formed boolean from backend" str))))


(define (pg:text-parser str) str)

(define (pg:character-parser str)
  (string-ref str 0))


(define (pg:number-parser str) (string->number str))

(define (pg:isodate-parser str)
  (let ((year    (string->number (substring str 0 4)))
        (month   (string->number (substring str 5 7)))
        (day     (string->number (substring str 8 10)))
        (hours   (string->number (substring str 11 13)))
        (minutes (string->number (substring str 14 16)))
        (seconds (string->number (substring str 17 19)))

;;  "2004-10-11 13:00:57.558093+02"

        ;; has occured: 1901-12-13 20:45:52
        (tz      (string->number (substring str 19 22))))
    ;; gauche wants  nanosec
    ;; but this needs the TZ with 00 appended:
    ;; (string->date   str "~Y-~m-~d ~k:~M:~S~z")
    (make-date 0 seconds minutes hours day month year (* 3600 tz))))
					;; mmc: (- year 1900) (- month 1)


(define (pg:timestamptz-parser str)
  ;(logformat "pg:timestamptz-parser\n")
  (let ((year    (string->number (substring str 0 4)))
        (month   (string->number (substring str 5 7)))
        (day     (string->number (substring str 8 10)))
        (hours   (string->number (substring str 11 13)))
        (minutes (string->number (substring str 14 16)))
        ;(seconds (string->number (substring str 17 19)))

;;  "2004-10-11 13:00:57.558093+02"
        ;string start end
        )
    (receive  (seconds tz)
        (string-scan (substring/shared str 17) "+" 'both)
        ;; has occured: 1901-12-13 20:45:52
      ;(tz      (string->number (substring str 19 22))))
    ;; gauche wants  nanosec
      (make-date 0 (string->number seconds)
                 minutes hours day month year (* 3600 (string->number tz))))))



(define (pg:date-parser str)
  (let ((year    (string->number (substring str 0 4)))
        (month   (string->number (substring str 5 7)))
        (day     (string->number (substring str 8 10))))
    (make-date 0 0 0 0  day month year 0))) ;mmc: (- year 1900) (- month 1)


;; numbers
(define (pg-array->list-number string-value)
    ;;(use srfi-
         ; string-tokenize
  (map string->number
    (pg-array->list string-value)))


(define (skip-over-comma s)
  (if (string=? s "")
      s
    ;; assert (char=?
    (begin
      ;;(assert (char=? (string-ref s 0) #\,))
      (unless (char=? (string-ref s 0) #\,)
        (error "expected comma in string" s))
      (substring/shared s 1))))

;; (skip-over-comma ",abc")
;; (skip-over-comma "abc")
;; (skip-over-comma "")

;; fixme: reverse!
(define (pg-array->list string-value)
  ;; 1/ remove {}
  (reverse
   (let1 s (substring string-value 1    ;skip 2 chars  {.......}
                      (- (string-length string-value) 1))
     (let read-next ((s s)
                     (so-far ()))
       ;;(logformat "read-next from: ~a\n" s)
       (if (string=? s "")
           ;; end:
           so-far
         ;;
         (if (string-prefix? "\"" s)
             ;; (char=? (string-ref s 0) #\")
             ;; I have to skip to the closing ", not escaped!
             (let dequote ((s (substring/shared s 1)) ;skip over "
                           (pieces ()))               ;start

               (let1 next-quote-pos (string-index s #\" 0) ;fixme: correct?
                                        ;(logformat "found '' ~d\n" next)
                 (cond
                  ((not next-quote-pos)
                   (error "not terminated string!"))

                  ((or (= next-quote-pos 0) ; 1 is bug!
                       (not (char=? (string-ref s (- next-quote-pos 1)) #\\)))
                   ;; not quoted
                   (read-next
                    (skip-over-comma
                     (substring/shared s (+ next-quote-pos 1))) ;fixme! skip over ,

                    ;; fixme: replace \" -> "
                    (cons
                     (string-append (string-join
                                        (reverse pieces) "\"")
                                    (substring/shared s 0 next-quote-pos))
                     so-far)))
                  (else
                   ;; quoted -> step ahead, with `start.'
                   (dequote
                    (substring/shared s (+ next-quote-pos 1))
                    (cons
                     (substring/shared s 0 (- next-quote-pos 1))
                     pieces))))))

           ;; get until first ","
           (let1 next (string-index s #\,)
             ;; fixme: ^^ at the end?
                                        ;(logformat "found , ~d\n" next)
             (if (not next)
                 (cons s so-far)
               (read-next
                (skip-over-comma
                 (substring/shared s next
                                        ;(+ next 1)
                                   ))   ;fixme! skip over ,
                ;; fixme: replace \" -> "
                (cons (substring/shared s 0 next)
                      so-far))))))))))

;; (pg-array->list "{\"RU, Mosca\",\"RU, nord ovest\",\"RU, bacino Mosca\"}")
;; (pg-array->list "{RU Mosca,RU nord ovest,RU, bacino Mosca}")
;; (pg-array->list "{\"RU Mosca\"}")

;;; `TABLE'

(define pg:type-parsers
  `(("bool"      . ,pg:bool-parser)

    ("char"      . ,pg:character-parser)     ;fixme!

    ("bpchar"      . ,pg:character-parser) ;fixme! pg:text-parser
    ;("char2"     . ,pg:text-parser)
    ;("char4"     . ,pg:text-parser)
    ;("char8"     . ,pg:text-parser)
    ;("char16"    . ,pg:text-parser)
    ("text"      . ,pg:text-parser)
    ("varchar"   . ,pg:text-parser)     ;??
    ("name"   . ,pg:text-parser)

    ("int2"      . ,pg:number-parser)
    ;("int28"     . ,pg:number-parser)
    ("int4"      . ,pg:number-parser)
    ("int8"     . ,pg:number-parser)
    ("oid"       . ,pg:number-parser)

    ("float4"    . ,pg:number-parser)
    ("float8"    . ,pg:number-parser)
    ("money"     . ,pg:number-parser)

    ("abstime"   . ,pg:isodate-parser)
    ("date"      . ,pg:date-parser)
    ("timestamp" . ,pg:isodate-parser)
    ("timestamptz" . ,pg:timestamptz-parser)
    ("datetime"  . ,pg:isodate-parser)
    ("time"      . ,pg:text-parser)     ; preparsed "15:32:45"
    ("reltime"   . ,pg:text-parser)     ; don't know how to parse these
    ("timespan"  . ,pg:text-parser)
    ("tinterval" . ,pg:text-parser)


    ;("_text"      . ,pg:text-parser)
    ("_text"      . ,pg-array->list)
    ;("_int2"      . ,pg:text-parser)
    ("_int2"      . ,pg-array->list-number)
    ("_int"      . ,pg-array->list-number)

    ("varbit"      . ,pg:text-parser)
    ))


;;; `printers'
(define (pg:bool-printer value)
  (if value "'true'"
    "'false'"))


(define (pg:text-printer obj)           ;; pg gives  "i'm" ->
  (string-append
   "'"
   ;; (string-join (string-split obj  #\') "''")
   (pg-escape-string obj)
   "'"))


(define (pg:name-printer name)
  (rxmatch-if
      (rxmatch #/^[a-zA-Z_][a-zA-Z_0-9]*$/ name)
      (all)
    name

    (string-append "\"" (pg-escape-string name)
                   "\"")
    ))

;; (pg:text-printer (pg:text-printer ""))

;;  (string-append "'" (string-replace "'" "''" obj) "'"))


(define (pg:char-printer value)
  (string #\' value #\'))

;; fixme:   in WHERE  i need x IS NULL, rather than x=NULL !
(define (pg:number-printer value)
  (if value
      (number->string value)
    "NULL"))

;; fixme!!!
(define (pg:isodate-printer value)
  (string-append "'" (date->string value) "'"))

;; same
(define (pg:date-printer value)
  (string-append "'" (date->string value) "'"))


(define (pg:array-printer value-list)
  ;; strings!
  (if (or (null? value-list)
          (not value-list))
      "NULL"
    (s+ "'{"
        (string-join
            (map
                (lambda (v)
                  (if (string-scan v #\,)
                                        ;(s+ "\""
                      (pg:name-printer v)
                                        ;"\"")
                    v))
              value-list)
            ",")
        "}'")))

;(pg:name-printer "ua, centro")
;(pg:array-printer '("ua, centro"))
;;(pg:array-printer '("1 b" "a,b"))



;; Fixme:  Just a hack
(define (pg-type-array? type)
  (string-prefix? "_" (ref type 'name)))


;;; The table:
(define pg:type-printers
  `(("bool"      . ,pg:bool-printer)
    ("char"      . ,pg:char-printer)

    ("bpchar"      . ,pg:char-printer)
    ;("char2"     . ,pg:text-printer)
    ;("char4"     . ,pg:text-printer)
    ;("char8"     . ,pg:text-printer)
    ;("char16"    . ,pg:text-printer)
    ("text"      . ,pg:text-printer)
    ("varchar"   . ,pg:text-printer)

    ("name"   . ,pg:text-printer)       ; only limited length ??


    ("int2"      . ,pg:number-printer)
    ("int28"     . ,pg:number-printer)
    ("int4"      . ,pg:number-printer)
    ("oid"       . ,pg:number-printer)

    ("float4"    . ,pg:number-printer)
    ("float8"    . ,pg:number-printer)
    ("money"     . ,pg:number-printer)

    ;; new:
    ("_text"    .  ,pg:array-printer)

    ("abstime"   . ,pg:isodate-printer)
    ("date"      . ,pg:date-printer)
    ("timestamp" . ,pg:isodate-printer)
    ("timestamptz" . ,pg:isodate-printer)
    ("datetime"  . ,pg:isodate-printer)
    ("time"      . ,pg:text-printer)     ; preparsed "15:32:45"
    ("reltime"   . ,pg:text-printer)     ; don't know how to parse these
    ("timespan"  . ,pg:text-printer)
    ("tinterval" . ,pg:text-printer)))

;;;  `hi-level:'

;; for the low-level  pgconn ?
;; scheme object -> string
(define (pg-converter pgconn oid)       ;fixme: Unused!
  ;(logformat "pg-printer: searching for ~d\n" oid)
  ;; fixme: and-let
  (let* ((name (pg-type-name pgconn oid)) ;fixme: This is available for the High <pg> ! Which has an hash of <pg-type> !
         (info (assoc oid name pg:type-parsers)))
    ;; oid (slot-ref pgconn 'converters))
    (if info
        (cdr info)
      (if name
          (error "pg-converter: cannot find for" name)
        (error "pg-converter: cannot find for" oid)))))



;; scheme object -> string
(define (pg-printer pgconn oid)
  ;(logformat "pg-printer: searching for ~d\n" oid)
  (let* ((h (slot-ref pgconn 'oid->type))
         (info (assoc (hash-table-get h oid) pg:type-printers)))
    ;; oid (slot-ref pgconn 'converters))
    (if info
        (cdr info)
      (error "pg-printer: cannot find for " oid))))


;;; What is the gauche-type (going to be)

;; fixme!
(define (pg-numeric-type? pgtype)
  (if debug (logformat "numeric? ~a\n" (ref pgtype 'name)))
  (or (member
       (ref pgtype 'name)
       '("int4" "int"
         "bigint" "int8"
         "smallint" "int2"))))



(define (pg-char-type? pgtype)
  (if debug (logformat "numeric? ~a\n" (ref pgtype 'name)))
  (or (member
       (ref pgtype 'name)
       '("char" "bpchar"))))


;; fixme!
(define (pg-date-type? pgtype)
  (if debug (logformat "date? ~a\n" (ref pgtype 'name)))
  (or (member
       (ref pgtype 'name)
       '("date"
         "abstime"
         "date"
         "timestamp"
         "timestamptz"
         "datetime"
         "time"
         "reltime"
         "timespan"
         "tinterval"
         ))))


;;; Actual conversion:

;; STR is an attribute's value retrieved as a string from the backend.
;; OID is the associated oid returned with the metadata. If we find a
;; parser for this type, we apply it, otherwise just return the
;; unparsed value.
(define (pg:parse str oid)              ;mmc: oid is number.   assq should be faster
  (let ((parser (assoc oid pg:*parsers*)))
    (if (pair? parser)
        ((cadr parser) str)
        str)))


;;; `types' from e.marsden's code:
#;
(define (pg-get-value* result tuple index)
  ;; result ->  handle ->
    ;; fixme:
    (pg:parse                           ; pg-convert
     (pg-get-value result tuple index)
     (pg-ftype result index)))

;;;
(define (scheme->pg pg-type value)
  ;(logformat "scheme->pg: ~a for ~a\n" value pg-type)
  (cond
   ;;
   ((string? value)
    (pg:text-printer value))

   ((ref pg-type 'printer)
    =>(lambda (p)
        ;(logformat "using the type's printer\n")
        (p value)))
   (else
    ;(logformat "fallback to x->string\n")
    (x->string value))))



(provide "pg/types")
