
;; how about naming  pgresult (w/o the dash) the low-level object,
;; and pg-result the hi-level?
;; for now:
;;    HI               lo
;;  <pgresult>   <pg-result>
;;  <pg>         <pg-handle>
;;  <pg-type>        oid
;;
(define-module pg-hi
  (extend pg
          pg-low)

  (export
   pg-deleted

   <pgresult> result-of
   <pg>
   pg-backend-pid1
   pg-open
   ->db
   pg-conn-of
   pg-find-type
                                        ;pg-set-notice-processor
   ;; macros:
   with-pg-handle*
                                        ;sync & general
   pg-exec ;; pg-exec-hi

   pg-status
   ;; result:
   pg-result->hiresult
   pg-result-prepare!
   ;; help:

   pg-result-has-attribute?             ;mmc: might be moved !
   pg-attribute-indexes
   ;; hi-level
   pg-collect-result pg-collect-single-result pg-collect-result-alist

   pg:load-alist pg:load-hash

   pg:all-relnames
   pg-truncate

   ->column

   ;; mess:
   ;;
   ;; fixme:  referencer ref modifier

   ;; backwards compatibility:
   ;pg:isodate-parser
   ; pg:timestamptz-parser
   ;pg:text-printer pg:char-printer
   ;pg:name-printer



   ;pg-type-name
   ; pg-convert
   ; pg-converter
   ;pg-printer
   pg-get-value-string

   pg-value->string
   pg-get-value-string-null


   pg-foreach-result

   pg-get-value-by-name
   ;;pg:initialize-parsers
   ;; pg:type-printers

   pg:start-listening-on
   ;;
   <pg-row>
   pg-get-row
   pg-get
   ;;
   ;; emacs-mule->cyrillic
   pg-default-separator
   ;; fixme:   if the result is null (e.g. if the handle is wrong !!),  pg-cmd-tuples  segfaults

   ;; from pg.types
   scheme->pg
   )


  (use gauche.uvector)                  ; ??
  (use mmc.simple)
  (use srfi-1)
  (use mmc.log)
  (use adt.alist)                       ; ->>hash
  (use pg.types)
  (use pg.caching)

  ;; for for-each implementation!
  (use gauche.collection)
  )
(select-module pg-hi)


;; fixme: move in pg ?
(define pg-default-separator "")


;;; Status processing

;; how many tuples were deleted:
(define (pg-deleted result)
  ;; fixme:  throw error if the result is not suitable!
  (rxmatch-let
      (rxmatch #/DELETE ([[:digit:]]+)$/ (pg-cmd-status result))
      (all number)
    (string->number number)))



;;; connection / handle
;;; `<pg>' cclass ???
(define-class <pg> ()
  (
   (types :initform #f)
   (oid->type :initform '())            ;make-hash

   (host :initform #f)                  ;fixme:  via getter !!
   (user :initform #f)                  ;fixme: same
   (port :initform #f)                  ; i want setters/getters ....
   ;;(db :initform #f)
   (schema-path :initform #f)

   (conn       :accessor pg-conn-of :initform #f)

   ;; see pg.database
   (database :initform #f)              ; pg.database depends on this module! we cannot!

   ;; (transaction  :initform #f)
   ;;  state:  as in  psql:  client encoding, ....
   )
  ;;:metaclass <pg-meta>
  )

(define-method write-object ((object <pg>) port)
  (let1 handle (slot-ref object 'conn)
    (display "<pg::" port)
    (display handle port)               ;write-object
              ; (pg-user handle)
;               "@"
;               (pg-host handle)
;               ":"
;               (pg-db handle)
    (display ">" port)))

(define-generic ->db)
;; (->db (pg-open))

(define-method ->db ((pg <pg>))
  (slot-ref pg 'conn))



(define (pg-backend-pid1 pg-handle)
  (pg-backend-pid (if (is-a? pg-handle <pg>)
                      (pg-conn-of pg-handle)
                    pg-handle)))

;; example:  (pg-open :host "/tmp" :user "mmc" )

(define pg-open-accepted-keywords '(:user :port :dbname :host))

(define debug #f)


;; filter known :keywords (+ value) from args.
(define (pg-compose-conninfo args)
  (let add-param
      ;; return a list  ((keyword value) ...)
      ((param '())
       (rest args))
    (cond
     ((null? rest)
      param)

     (else
      (let1 name (car rest)
	(if (member name pg-open-accepted-keywords) ;(keyword? name)
	    (add-param
	     (cons
	      (format #f "~a=~a" (keyword->string name)
		      (list-ref rest 1))
	      param)
	     (cddr rest))
	  ;; skip...  fixme:  only 1?
	  (add-param param (cdr rest))))))))

;;; Connecting

(define (handle->description handle)
  (string-join			; non empty !!
      (remove not
	      (list
	       (format-nonvoid "host=~a" (pg-host handle))
	       (format-nonvoid "dbname=~a" (pg-db handle))
	       (format-nonvoid "user=~a" (pg-user handle))
	       (format-nonvoid "port=~a" (pg-port handle))
	       (format-nonvoid "options=~a" (pg-options handle))
	       (format-nonvoid "pass=~a" (pg-pass handle))))
       " "))



;; the hi level!
(define (pg-open . args)
  ;; todo:  default connect_timeout of 10seconds?
  ;(logformat "~s\n"(vm-get-stack-trace))
  (let* ((conninfo (string-join (pg-compose-conninfo args) " "))
         (conn (pg-connect conninfo))   ;fixme:  pg-connectdb ?
         (handle (make <pg>)))

    (if debug (logformat "pg-open: ~a\n" conninfo))
    (slot-set! handle 'conn conn)

    ;; New:  not a hash, just alist!
    (slot-set! handle 'types (pg-init-types-hash conn)) ;hash oid -> <pg-type>
    ;; old:
    '(let1 h (make-hash-table)
      (alist->>table! h (pg-init-types conn))
      (slot-set! handle 'oid->type h))

    ;; (slot-set! oid->type 'converters pg:*parsers*)
    ;; (pg:initialize-parsers handle)
    handle))








(define (pg-find-type pg oid)
  ;; (assert (is-a? pg <pg>))
  (let1 hash (ref pg 'types)
    (if (hash-table-exists? hash oid)
        (hash-table-get hash oid)
      (errorf "type (oid ~d) unknown" oid))))

'(define (pg-su connection user)
  (pg-exec " SET [ SESSION | LOCAL ] SESSION AUTHORIZATION username"))

;;; `pgresult'
(define-class <pgresult> (<collection>)  ;mmc! <pg-result>
  (
   ;; fixme:
   ;; query
   ;; hash attribute ->index ??
   (result :accessor result-of :init-keyword :result)                            ;<pg-result>
   (handle :init-keyword :handle)
   ;; vectors
   (types)                              ;fixme: used?
   ;(converters)
   ;;
   (tuples)                             ;list of <pg-tuple> objects
   )
  )

(define-method object-apply ((res <pgresult>) row column)
  (pg-get-value (ref res 'result)
		row
		(if (number? column)
		    column
		  (pg-fnumber (ref res 'result) column))))

(define-class <pg-row> ()
  ((result :init-keyword :result)
   (row :init-keyword :row)))

(define-method object-apply ((row <pg-row>) index)
  (pg-get-value (ref row 'result)
		(slot-ref row 'row)
		(if (number? index)
		    index
		  (pg-fnumber (ref row 'result) index))))

(define-generic pg-get-row)
(define-method pg-get-row ((result <pgresult>) i)
  ;;(define (pg-get-row result i)
  (make <pg-row>
    :result result
    :row i))

(define-generic pg-get)
(define-method pg-get ((row <pg-row>) attribute)
  (let1 result (slot-ref row 'result)
    (pg-get-value result
                  (slot-ref row 'row)
                  (pg-fnumber result attribute))))



(define-method write-object ((result <pgresult>) port)
  (format port "<pgresult: ~s/>"
          (ref result 'result)
          ;(length (ref result 'types)
          ))


(define-method call-with-iterator ((result <pgresult>) proc . options)
  ;; todo: options:  START
  (let ((current-row 0)
	(last (pg-ntuples result)))
    ;; return 2 lambdas
    (proc
     ;; predicate for the END:
     (lambda ()
       (= last current-row))
     ;; Get the next:
     (lambda ()
       (let1 row (pg-get-row result current-row)
	 ;; (make <pg-row>
	 ;; 	   ;;
	 ;; 	   :result result
	 ;; 	   :row current-row)
	 (inc! current-row)
	 row)))))


;; find the type converters (C type -> scheme type)
(define (pg-result-prepare! result)
  (let* ((presult (slot-ref result 'result))
         (fields (pg-nfields presult))

         ;(v (make-vector fields #f))
         (types (make-vector fields #f)) ;
         (pg (slot-ref result 'handle)))
    ;; types ->  parsers
    (for-numbers<* i 0 fields
      ;; the type should be an object!  (name, parser, printer)
      ;;
      (let1 type (pg-find-type pg (pg-ftype presult i))
        ;; (pg-type-name conn (pg-ftype presult i)) ;fixme:  pg-ftype is a generic: it returns type-name!

        (vector-set! types i type)
        ;(vector-set! v i (pg-converter pg (pg-ftype presult i)))   ;pg-type-->parsers
        ))
    ;; names ??
    ;; pg-result-prepare
    ;(slot-set! result 'converters v)
    (slot-set! result 'types types)))

(define-generic pg-foreach-result)
;; fixme: it should be inherited!
(define-method pg-foreach-result ((result <pgresult>) attributes function)
  (let1 numbers (if attributes
                    (pg-attribute-indexes (ref result 'result) attributes)
                  (iota (pg-nfields result)))
    (for-numbers<* row 0 (pg-ntuples result)
                                        ;(logformat "pg-foreach-result: ~d ~s\n" row numbers)
      (apply function
             (map (cut pg-get-value result row <>) ; method?
               numbers)))))


(define-method pg-foreach-result ((result <pg-result>) attributes function)
  (let1 numbers (if attributes
                    (pg-attribute-indexes result attributes)
                  (iota (pg-nfields result)))
    (for-numbers<* row 0 (pg-ntuples result)
                                        ;(logformat "pg-foreach-result: ~d ~s\n" row numbers)
      (apply function
             (map (cut pg-get-value result row <>) ; method?
               numbers)))))




;; fixme: i want the better generics ... see  ML!

(define (pg-result->hiresult result handle)
  ;; handle is the hi level!
  (let1 r (make <pgresult> :result result :handle handle)
    (pg-result-prepare! r)
    r))

(define-method pg-finish ((handle <pg>))
  (pg-finish (pg-conn-of handle)))


;; generics:
(define-method pg-exec ((handle <pg>) query)
  ;; this returns a higher lever <pgresult>1
  ;; (slot-ref handle 'debug)
  (if debug (logformat "pg-exec: ~a\n" query))
  (let* ((real-handle (slot-ref handle 'conn))
         ;; pg-exec--internal
         (result (pg-exec real-handle query)))

    ;; mmc: This is obsolete! use pg-transaction-status
    (when #f
      (cond

       ;; fixme:  i think there's a C api to detect it!
       ((string=? (pg-cmd-status result) "BEGIN") ; fixme: i should see ALL results...
	(slot-set! handle 'transaction #t))
       ((string=? (pg-cmd-status result) "COMMIT") ; fixme: i should see ALL results...
	(slot-set! handle 'transaction #f))))

    (pg-result->hiresult result handle)))



(define (pg:start-listening-on handle on)
  (when (= (pg-transaction-status handle) PQTRANS_IDLE)
    (logformat-color 'yellow "BEGIN be pid=~d\n" (pg-backend-pid1 handle))
    (pg-exec handle "BEGIN;"))
  (pg-exec handle (s+ "listen " on ";")))
;;(pg:unlock-fotos handle)




(define-method pg-get-isnull ((result <pgresult>) tuple index)
  ;; result ->  handle ->
  (pg-get-isnull (slot-ref result 'result) tuple index))


(define-method pg-status ((pg <pg>))
  ;; result ->  handle ->
  (pg-status (pg-conn-of pg)))







(define-method pg-get-value ((result <pgresult>) tuple index)
  ;; result ->  handle ->
  ;; (pg-convert
  (if (pg-get-isnull result tuple index)
      eof-object
    (let1 type (vector-ref (slot-ref result 'types) index)
      ((slot-ref type 'parser)
       (pg-get-value (slot-ref result 'result) tuple index)
       ;; (pg-ftype result index)
       ))))

(define (pg-get-value-by-name result row colname)
  (pg-get-value result row (pg-fnumber result colname)))



(define (pg-get-value-string result tuple index) ; <pgresult>
  ;; result ->  handle ->
  ;; (pg-convert
  (if (pg-get-isnull result tuple index)
      eof-object
    (pg-get-value (slot-ref result 'result) tuple index)))



;; Not very efficient, but works!
;; obsolete:  not quite!
(define (pg-value->string value)
  (if (eof-object? value)
      "NULL"
    ;; pg-get-value
    (pg:text-printer value)))


;; IS this a reasonalbe function?  I push numbers as '6' instead of 6!
;; Useful?  in C?
(define (pg-get-value-string-null r i j)
  (if (pg-get-isnull r i j)
      "NULL"
    (pg:text-printer (pg-get-value-string r i j))))






;;; methods:  on <pgresult>

;; the query should produce 2 columns
(define (pg:load-alist alist handle query)
  (for-each
      (lambda (row)
      ;; word -> abbrev
      (set! alist
	    (aput alist
		  (row 1)
		  (row 0))))
    (pg-exec handle query))
  alist)


(define (pg:load-hash hash handle query)
  (for-each
      (lambda (row)
      ;; word -> abbrev
      (hash-table-put! hash
		       (row 0)
		       (row 1)))

    (pg-exec handle query))
  hash)



(define-method pg-fname ((result <pgresult>) index)
  (pg-fname (slot-ref result 'result) index))

(define-method pg-ftype ((result <pgresult>) index)
  ;; old:
  '(hash-table-ref
    (ref (slot-ref result 'handle) 'types)
    (pg-ftype (slot-ref result 'result) index))

  (vector-ref (ref result 'types)
              index
              ;; (pg-ftype (slot-ref result 'result) index)
              ))


(define-method pg-fnumber ((result <pgresult>) name)
  (pg-fnumber (slot-ref result 'result) name))


(define-method pg-fsource ((result <pgresult>) index)
  (pg-fsource (slot-ref result 'result) index))

(define-method pg-ftable ((result <pgresult>) index)
  (pg-ftable (slot-ref result 'result) index))

(define-method pg-ftablecol ((result <pgresult>) index)
  (pg-ftablecol (slot-ref result 'result) index))


(define-method pg-ntuples ((result <pgresult>))
  (pg-ntuples (slot-ref result 'result)))
(define-method pg-nfields ((result <pgresult>))
  (pg-nfields (slot-ref result 'result)))



;;; `mess'


'(define (pg-exec handle query)
  (if (is-a? handle <pg>)
    (pg-exec-internal handle query)))


;;; now we have the primitives available,

;(eq? pg-notice-processor #f)
;; the default one:


'(pg-set-notice-processor
 (lambda (pg-handle message)
   (format (current-error-port) "pg-notice-processor: ~d@~a: ~a\n"
           (pg-host pg-handle)
           (pg-backend-pid pg-handle)
           message)))


(define-syntax with-pg-handle*
  (syntax-rules ()
    ((_ variable getter body ...)
     (let ((variable getter))         ;fixme!
       body ...
       (pg-finish variable)))))


;(define-class <pg-meta> (<class>) ())


;;;  `bound-checks': yes, it's not done in C!    <start-end)
;; (define (out-of-range? start end test)
;;   (or (< test start)
;;       (>= test end)))

;; (define-syntax pg-check-tuple-limit
;;   (syntax-rules ()
;;     ((_ function)
;;      (let ((old-definition function))
;;        (define function
;;          (lambda (result index)
;;            (if (out-of-range? 0 (pg-nfields result) index)
;;                (error "out-of-field-range" #f ,(symbol->string function))
;;              (old-definition result index))))))))

;; (pg-check-tuple-limit pg-fmod)
;; (pg-check-tuple-limit pg-ftype)
;; (pg-check-tuple-limit pg-fsize)
;; (pg-check-tuple-limit pg-fname)


;;  useless .... C checks it?  no. not yet.
; (define-syntax pg-check-tuples-limit
;   (syntax-rules ()
;     ((_ function)
;      (begin
;        (let ((old-definition function))
;          (define function
;            (lambda (result tuple index)
;              (cond
;               ((out-of-range? 0 (pg-nfields result) index)
;                (error "out-of-field-range" #f))
;               ((out-of-range? 0 (pg-ntuples result) tuple)
;                (error "out-of-tuple-range" #f))
;               (else
;                (old-definition result tuple index))))))))))

; (pg-check-tuples-limit pg-get-length)
; (pg-check-tuples-limit pg-get-isnull)
; (pg-check-tuples-limit pg-get-value)


;; mmc: moved to C !!
;; raise exception on error !!!

;; (define (pg-exec-hi handle query)
;;   ;; fixme: other (not fatal) errors ??
;;   (let* ((result (pg-exec handle query))
;;          (status (pg-result-status result)))
;;     (if (eq? status PGRES_FATAL_ERROR)  ;pg-cmd-status
;;         (error (cons (pg-status-status status) query))
;;       result)))



;; get the column (values) as a list  ... should be a <collection> !!
(define (pg-collect-result result attribute)
  (let ((index (pg-fnumber result attribute)))
    (map-numbers 0 (pg-ntuples result)
      (cute pg-get-value result <> index))))

(define (pg-collect-result-alist result key value)
  (let ((index-k (pg-fnumber result key))
        (index-v (pg-fnumber result value)))
    (map (lambda (row)
	   (cons (row index-k)
		 (row index-v)))
      result)))


(define (pg-collect-single-result result attribute)
  (car (pg-collect-result result attribute)))


;; extract the values in a column.  Projection
(define-generic ->column)

(define-method ->column ((result <pg-result>) (index <integer>))
  (map-numbers 0 (pg-ntuples result)
    (cute pg-get-value result <> index)))

(define-method ->column ((result <pg-result>) (attribute <string>))
  (pg-collect-result result attribute))


;; result has *column*s, not attributes!
(define (pg-result-has-attribute? pgresult attname)
  (>= (pg-fnumber pgresult attname) 0))


(define (pg-truncate handle relname)
  (pg-exec handle (format #f "truncate \"~a\";" relname)))


;; fixme:  namespace!
(define (pg:all-relnames handle)
  (let ((result (pg-exec handle
			 "select relname
            from   pg_class
            where  relkind ~ '[r]'
            and    relname !~ '^pg_'
            and    relname !~ '^xin[vx][0-9]+';")))
    (pg-collect-result result "relname")))



;; ??
;;(define (description->handle desc)
;;  (pg-connect desc))


(define (format-nonvoid fmt . args)
  (if (member "" args) #f
    (apply format fmt args)))

;(format-nonvoid  "host=~a" (pg-options handle))
;(string-join  '( "a" #f "b") " ")
;(remove not '( "a" #f "b"))
;(identity 26)




;;; `sequence' interface:
;; function  index -> value


;; (define (pg-get-row result i)
;;   (cut pg-get-value result i <>))

;; ;; fixme: do i have to export it?
;; (define-method referencer ((result <pg-result>)) ;fallback
;;   (lambda (result i)
;;     (pg-get-row result i)))

;; ;; fixme:
;; (define-method ref ((obj <pg-result>) index)
;;   ((referencer obj) obj index))
;;    )

;; (define-method modifier ((result <pg-result>))
;;   (lambda rest
;;     (error "cannot set a pg result")))

;; '(define-method map (function (result <pg-result>))
;;   (map-numbers* row 0 (pg-ntuples result)
;;     (function result row)))

;(define (pg-get-value pg query)
;  (pg-get-value (pg-exec pg query) 0 0))


(provide "pg-hi")
