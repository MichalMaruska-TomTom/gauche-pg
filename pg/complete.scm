;;; (c) 2001 Michal Maruska
;;; will be hopefully:
;;;      Licensed under GPL
;;; ...... I have to contact the original authors of tab-complete.c from
;;; the PostgreSQL source-tree.    it was BSD licensed!
;;;

;;; TODO:  order _desc_
;; Start of the query?
;; ";"
;; beginning of line starting with keyword, not preceded by "("
;;

;; from elisp -> scheme:
;; mapcar ... map
;; defun  ... define

;;; pg-complete:
;;; base queries

(define-module pg.complete
  (export
   psql-complete
   pgc-set-db
   )
  (use mmc.log)
  (use macros.elisp)
                                        ;(use macros.documentation)

  (use srfi-13)
  ;; gauche needs  all-completions:
  (use pg.database)
  (use adt.complete)
  )
(select-module pg.complete)


;;; prepare:
(define upcase
  (lambda (s)
    (if (string? s)
        (string-upcase s)
      s)))
(define (string= a b)
  (and (string? a)
       (string? b)
       (string=? a b )))


;;; constants ... DATA
(defconst pgc-words-after-create
  '(
    ("AGGREGATE" "SELECT distinct aggname FROM pg_aggregate WHERE substr(aggname,1,%d)='%s'")
    ("DATABASE" "SELECT datname FROM pg_database WHERE substr(datname,1,%d)='%s'")
    ("FUNCTION" "SELECT distinct proname FROM pg_proc WHERE substr(proname,1,%d)='%s'")
    ("GROUP" "SELECT groname FROM pg_group WHERE substr(groname,1,%d)='%s'")
    ("INDEX" "SELECT relname FROM pg_class WHERE relkind='i' and substr(relname,1,%d)='%s'")
    ("OPERATOR")			; Querying for this is probably not such a good idea.
    ("RULE" "SELECT rulename FROM pg_rules WHERE substr(rulename,1,%d)='%s'")
    ("SEQUENCE" "SELECT relname FROM pg_class WHERE relkind='S' and substr(relname,1,%d)='%s'")
    ("TABLE" "SELECT relname FROM pg_class WHERE relkind~'[rv]' and substr(relname,1,%d)='%s'")
    ("TEMP" )				; for CREATE TEMP TABLE ...
    ("TRIGGER" "SELECT tgname FROM pg_trigger WHERE substr(tgname,1,%d)='%s'")
    ("TYPE" "SELECT typname FROM pg_type WHERE substr(typname,1,%d)='%s'")
    ("UNIQUE")				; for CREATE UNIQUE INDEX ...
    ("USER" "SELECT usename FROM pg_user WHERE substr(usename,1,%d)='%s'")
    ("VIEW" "SELECT viewname FROM pg_views WHERE substr(viewname,1,%d)='%s'")
    )
  "Alist:  OBJECT --> QUERY to get the list of possible names (completions).
In other words, OBJECTS can folow the *CREATE*/*DROP* keyword")


(defconst pgc-CREATE-objects
  (map car pgc-words-after-create)
  "list of strings which (may) follow the *CREATE* command string,
Abstractly: all known types of objects in the postgreSQL system.")


;; I want to push it all to an ADT approach, by now it is not (plain text queries are passed).
(define (pgc-query-of info)
  "get the query (rather a skeleton thereof) for this object"
  (list-ref info 1))


;;; parametrized queries:
(defconst pgc-q-tables
  (pgc-query-of
   (assoc "TABLE" pgc-words-after-create))
  "SQL query to get the list of existing tables with a given prefix.
Parametrized (by using the `format' format)
expects the the lenght of the prefix and the prefix")
;; system tables are not expanded. Views are !

(defconst pgc-q-indexes
  (pgc-query-of
   (assoc "INDEX" pgc-words-after-create))
  "SQL query")
(defconst pgc-q-databases
  (pgc-query-of
   (assoc "DATABASE" pgc-words-after-create))
  "")
(defconst pgc-q-attributes
  "SELECT a.attname FROM pg_attribute a, pg_class c WHERE c.oid = a.attrelid and a.attnum>0 and substr(a.attname,1,%d)='%s' and c.relname='%s'"
  "")
(defconst pgc-q-users
  (pgc-query-of
   (assoc "USER" pgc-words-after-create))
  "")

(defconst pgc-q-functions
  (pgc-query-of
   (assoc "FUNCTION" pgc-words-after-create))
  "SQL query to get the list of functions with a given prefix.
Parametrized (by using the `format' format)
expects the the lenght of the prefix and the prefix")


;;; How to get the data from DB?
;; Heavily system dependent:  I exploit the Xemacs's postgreSQL awareness !! Sorry.
;; (define (pgc:get-alist query))


;;; different completions
(define (pgc-complete-with-query query text)
  "get the alist of completions from the QUERY and the TEXT (=prefix) which is
 substituted in the QUERY along with its lenght (in reverse order) via the `format'."
  (let ((pgc-complete-query
	 ;; fixme completion_info_charp
	 (format #f query (length text) text)))
    (pgc:get-alist pgc-complete-query)))


;; FIXME: For each database !!!
(define nil '())
;; defvar
(defvar pgc-list-of-tables nil "")



(define (pgc-complete-with-list list text)
  (alist-from-list list)
  ;; (setq matches (completion-matches text complete-from-list))
  )

(define (pgc-complete-with-const string text)
  (cons string string)
					; (setq matches (completion-matches text complete-from-const))
  )


;; We try hard to cache all possible data.
(define (pgc-complete-attribute text)
  ;; is the FROM part present
  (let ((from-part (pgc-part-between 'from "from")) ; "\\(.*\\)"
	(from-info '())
	(from-string '())
	(tables '()))

    (if (null from-part)
	;; brute force:
	(pgc-complete-with-query "SELECT distinct attname FROM attributes WHERE substr(attname,1,%d)='%s'" text)
      ;; From is present.
      (begin
        (unless (and from-part
                     (extentp from-part)
                     (extent-live-p from-part)
                     (setq from-info (extent-property from-part 'info))
                     )
          ;; We have to re-calculate:
          (setq from-string (replace-in-string (extent-string from-part) "[\n\t	]+" " " 't) ;non hungry ?
                from-info (pgc-analyze-from from-string))
          (set-extent-property from-part 'info from-info))

        ;; Now get the attributes of tables:

        ;; the tables are in:
        (setq tables (car from-info))
        (alist-from-list
         (apply
          'append
          (mapcar
           (lambda (table)
             (pgc-complete-with-attr-cache table text nil) ;don't refresh
             )
           tables
           )))
        ;; attach it to the word ??
        ))))

					;(dotimes (j  100)
					;  (pgc-complete-attribute  "nume"))

					;(dotimes (j  100)
					;  (psql-complete))

;;(pgc-complete-attribute  "nume")

(defvar pgc-attribute-alist nil
  "alist mapping tables to the list of attributes")

(defvar pgc-prefixed-attribute-regexp "\\([A-Za-z]+\\)\.\\(.*\\)"
  "regexp for the qualified (with the table alias/name) attribute")

(define (pgc-analyze-from from)
  "analyze the FROM part of a query. return a cons (#, cons (list of strings) (list of  or cons))
String if the class has no nickname/alias, cons otherwise."
  (let* ((classes (split-string from " *, *"))
	 (nicknames '())
         (plain-names '())
         (nickname '())
         (classname '())) ;class-s
    (while classes
      (setq current (car classes)
	    classes (cdr classes))
      ;; FIXME: I SHOULD CHANGE THE SYNTAX TABLE, see psql-mode.el !!
      ;; I use the hungriness !!
      ;; We must survive if the input is wrong !!
      (when (string-match "\\([a-zA-Z_]+\\) *\\([a-zA-Z_]*\\)" current)
	(setq nickname (match-string 2 current)
	      classname (match-string 1 current)
	      ;;class-s (mdb-class-get-create classname)  ;; register with the global store of tuples !!
	      )
	(if (eq (length nickname) 0)
	    ;; plain
	    (setq plain-names (cons classname plain-names)) ;  class-s
	  (setq nicknames (cons (cons nickname classname) nicknames)))) ; class-s
      )
    ;; Return
    (cons (reverse plain-names)
	  nicknames)))


(define (pgc-table-of-the-alias alias info)
  (let ((table-info (assoc alias (cdr info))))
    (if table-info
	(cdr table-info)
      alias)))


(define (pgc-complete-attribute-with-prefix prefix text)
  ""
  ;; Get the list of tables
  (let* ((from-part
	  (pgc-part-between 'from "from" ))
	 (from-part (replace-in-string from-part "[\n\t	]+" " " 't)) ;non hungry ?
	 (from-info (pgc-analyze-from from-part))
					;attribute
	 (table (pgc-table-of-the-alias prefix from-info))
	 )
    ;; get the one of the prefix...
    (if table
	(pgc-complete-with-attr table text)
      nil)
    ))

					;(pgc-complete-attribute-with-prefix  "a.sfdaj")



(defvar pgc-attribute-alist () "")
					;(dotimes (j  100)
					;  (pgc-complete-with-attr-cache "person"  "whe" nil))
;; FIXME: with obarrays !!
(define (pgc-complete-attribute table text)
					;(message table)
  (let1 t (pg:find-relation completion-db table)
    (all-completions text (attributes-of t))))

(define completion-db #f)
(define (pgc-set-db db)
  (set! completion-db db))

(define (pgc-complete-with-table-name prefix)
  (logformat "pgc-complete-with-table-name ~a\n" prefix)
  (alist-from-list (all-completions-list prefix (pg:namespace-relnames completion-db "public"))))

;(pgc-previous-word 0)
(define (pgc-complete-attribute-or-table prefix)
  (logformat "FIXME: pgc-complete-attribute-or-table ~a\n" prefix)
  ;;(list (cons prefix prefix))
  (pgc-complete-with-table-name prefix))



;;; Some keyword lists
(defconst pgc-commands
  '(
    "ABORT" "ALTER" "BEGIN" "CLOSE" "CLUSTER" "COMMENT" "COMMIT" "COPY"
    "CREATE" "DECLARE" "DELETE" "DROP" "EXPLAIN" "FETCH" "GRANT"
    "INSERT" "LISTEN" "LOAD" "LOCK" "MOVE" "NOTIFY" "RESET"
    "REVOKE" "ROLLBACK" "SELECT" "SET" "SHOW" "TRUNCATE" "UNLISTEN" "UPDATE"
    "VACUUM"
    )
  "sql-verbs in first position of a query"
  )

(defconst  pgc-pgsql-variables
  (list
					; these SET arguments are known in gram.y
   "CONSTRAINTS"
   "NAMES"
   "SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL"
   "TRANSACTION ISOLATION LEVEL"
					; these are treated in backend/commands/variable.c
   "DateStyle"
   "TimeZone"
   "client-encoding"
   "server-encoding"
   "random-seed"
					; the rest should match USERSET entries in backend/utils/misc/guc.c
   "enable-seqscan"
   "enable-indexscan"
   "enable-tidscan"
   "enable-sort"
   "enable-nestloop"
   "enable-mergejoin"
   "enable-hashjoin"
   "geqo"
   "ksqo"
   "fsync"
   "debug-assertions"
   "debug-print-query"
   "debug-print-parse"
   "debug-print-rewritten"
   "debug-print-plan"
   "debug-pretty-print"
   "show-parser-stats"
   "show-planner-stats"
   "show-executor-stats"
   "show-query-stats"
   "trace-notify"
   "sql-inheritance"

   "geqo-threshold"
   "geqo-pool-size"
   "geqo-effort"
   "geqo-generations"
   "geqo-random-seed"
   "sort-mem"
   "debug-level"
   "max-expr-depth"
   "commit-delay"

   "effective-cache-size"
   "random-page-cost"
   "cpu-tuple-cost"
   "cpu-index-tuple-cost"
   "cpu-operator-cost"
   "geqo-selection-bias"
   )
  "list of variables (which can be completed in certain position)")

(defconst pgc-backslash-commands
  (list
   "\\connect" "\\copy" "\\d" "\\di" "\\di" "\\ds" "\\dS" "\\dv"
   "\\da" "\\df" "\\do" "\\dt" "\\e" "\\echo" "\\encoding"
   "\\g" "\\h" "\\i" "\\l"
   "\\lo-import" "\\lo-export" "\\lo-list" "\\lo-unlink"
   "\\o" "\\p" "\\pset" "\\q" "\\qecho" "\\r" "\\set" "\\t" "\\unset"
   "\\x" "\\w" "\\z" "\\!"
   )
  "psql accepts these meta-commands, we complete on them too")


;;; Some small lists:
;;(multiset!
(define pgc-list-ALTER 	(list "GROUP" "TABLE" "USER"))
(define pgc-list-ALTER-cmd	(list "ADD" "ALTER" "RENAME"))
(define pgc-list-ALTERGROUP 	(list "ADD" "DROP"))
(define pgc-list-privileg 	(list "SELECT" "INSERT" "UPDATE" "DELETE" "RULE" "ALL"))
(define pgc-list-REINDEX 	(list "TABLE" "DATABASE" "INDEX"))
(define pgc-list-COMMENT 	(list "DATABASE" "INDEX" "RULE" "SEQUENCE" "TABLE" "TYPE" "VIEW"
                                      "COLUMN" "AGGREGATE" "FUNCTION" "OPERATOR" "TRIGGER"))

(define pgc-index-mth 		(list "BTREE" "RTREE" "HASH"))
(define pgc-list-FETCH1 	(list "FORWARD" "BACKWARD" "RELATIVE"))
(define pgc-list-FETCH2	(list "ALL" "NEXT" "PRIOR"))
(define pgc-list-FROMTO 	(list "FROM" "TO"))
(define pgc-rule-events	(list "SELECT" "UPDATE" "INSERT" "DELETE"))
(define pgc-list-INSERT 	(list "SELECT" "VALUES"))
(define pgc-my-list		(list "READ" "SERIALIZABLE"))
(define pgc-constraint-list	(list "DEFERRED" "IMMEDIATE"))
(define pgc-datestyle-list	(list "'ISO'" "'SQL'" "'Postgres'" "'European'" "'NonEuropean'" "'German'" "DEFAULT"))
(define pgc-geqo-list 		(list "ON" "OFF" "DEFAULT"))
(define pgc-default-list	(list "DEFAULT"))
(define pgc-pset-commands	(list "format" "border" "expanded" "null" "fieldsep"
			      "tuples-only" "title" "tableattr" "pager"
			      "PROMPT1" "PROMPT2" "PROMPT3"
			      "recordsep"))


;;; testing
(define (pgc-previous-word n)
  (list-ref (reverse (list #f #f "select" "*" "fro")) n))


;;; The function
;; we have a mapping  WORD4 WORD3 WORD2 WORD1 WORD ---> completion of  WORD.
(define (psql-complete text prev-wd prev2-wd prev3-wd prev4-wd)
  (let* (
;        (text (pgc-previous-word 0))	; thing-at-point  'word
; 	 (prev-wd  (pgc-previous-word 1))
; 	 (prev2-wd (pgc-previous-word 2))
; 	 (prev3-wd (pgc-previous-word 3))
; 	 (prev4-wd (pgc-previous-word 4))

	 ;; MARUSKA:  i dont care about case!  keywords do neither, but user-names (relnames, attnames) ?
	 (prev-wd-uc (upcase prev-wd))
	 (prev2-wd-uc (upcase prev2-wd))
	 (prev3-wd-uc (upcase prev3-wd))
	 (prev4-wd-uc (upcase prev4-wd)))

    ;; We could start with a more simple completion, eg.  X.ag --->
    ;; The is the mapping:
    (cond
					;     (
					;      (string-match "^\\.$" prev-wd)
					;      ;; What is the table ?
					;      (pgc-complete-attribute-with-prefix text)
					;      )
					;     (
					;      ;; If a backslash command was started, continue
					;      (string-match "^\\\\" prev-wd)
					;      (pgc-complete-with-list pgc-backslash-commands text))

     ;; If no previous word suggest one of the basic sql commands
     ((string= "" prev-wd)
      (pgc-complete-with-list pgc-commands text))

     ;; CREATE or DROP
     ;; complete with something you can create or drop
     ;; BUG:  create trigger on DELETE ...
     ((or (string= prev-wd-uc "CREATE")
	  (string= prev-wd-uc "DROP"))
      (pgc-complete-with-list pgc-CREATE-objects text))

     ;; ALTER
     ;; complete with what you can alter (TABLE GROUP USER)
     ((string= prev-wd-uc "ALTER")
      (pgc-complete-with-list pgc-list-ALTER text))

     ;; If we detect ALTER TABLE <name> suggest either ADD ALTER or RENAME
     ((and (string= prev3-wd-uc "ALTER")
	   (string= prev2-wd-uc "TABLE"))
      (pgc-complete-with-list pgc-list-ALTER-cmd text))

     ;; If we have TABLE <sth> ALTER|RENAME provide list of columns
     ((and (string= prev3-wd-uc "TABLE")
	   (or (string= prev-wd-uc "ALTER") (string= prev-wd-uc "RENAME")))
      (pgc-complete-with-attr prev2-wd text))

     ;; complete ALTER GROUP <foo> with ADD or DROP
     ((and (string= prev3-wd-uc "ALTER")
	   (string= prev2-wd-uc "GROUP"))
      (pgc-complete-with-list list-ALTERGROUP text))

     ;; complete ALTER GROUP <foo> ADD|DROP with USER
     ((and (string= prev4-wd-uc "ALTER")
	   (string= prev3-wd-uc "GROUP")
	   (or (string= prev-wd-uc "ADD")
	       (string= prev-wd-uc "DROP")))
      (pgc-complete-with-const "USER" text))

     ;; complete (ALTER) GROUP <foo> ADD|DROP USER with a user name
     ((and (string= prev4-wd-uc "GROUP")
	   (or (string= prev2-wd-uc "ADD")
	       (string= prev2-wd-uc "DROP"))
	   (string= prev-wd-uc "USER"))
      (pgc-complete-with-query pgc-q-users text))

     ;;; CLUSTER

     ;; If the previous word is CLUSTER produce list of indexes.
     ((string= prev-wd-uc "CLUSTER")
      (pgc-complete-with-query pgc-q-indexes text))

     ;; If we have CLUSTER <sth> then add "ON"
     ((string= prev2-wd-uc "CLUSTER")
      (pgc-complete-with-const "ON" text))

     ;; If we have CLUSTER <sth> ON then add the correct tablename as well.
     ((and (string= prev3-wd-uc "CLUSTER")
           (string= prev-wd-uc "ON"))
      (pgc-complete-with-query
       (format "SELECT c1.relname FROM pg-class c1 pg-class c2 pg-index i
WHERE c1.(setq oidi.indrelid and i.(setq indexrelidc2.oid and c2.(setq relname'%s'" prev2-wd)))

     ;;; COMMENT
     ((string= prev-wd-uc "COMMENT")
      (pgc-complete-with-const "ON") text)

     ((and (string= prev2-wd-uc "COMMENT")
	   (string= prev-wd-uc "ON"))
      (pgc-complete-with-list list-COMMENT text))

     ((and (string= prev4-wd-uc "COMMENT")
           (string= prev3-wd-uc "ON"))
      (pgc-complete-with-const "IS" text))

     ;;; COPY
     ;; If we have COPY [BINARY] (which you'd have to type yourself) offer
     ;; list of tables (Also cover the analogous backslash command)
     ((or (string= prev-wd-uc "COPY")
          (string= prev-wd-uc "\\copy")
          (and (string= prev2-wd-uc "COPY")
               (string= prev-wd-uc "BINARY")))
      (pgc-complete-with-table-name text))

     ;; If we have COPY|BINARY <sth> complete it with "TO" or "FROM" n
     ((or (string= prev2-wd-uc "COPY")
          (string= prev2-wd-uc "\\copy")
          (string= prev2-wd-uc "BINARY"))
      (pgc-complete-with-list list-FROMTO text))


     ;;; CREATE INDEX
     ;; First off we complete CREATE UNIQUE with "INDEX"
     ((and (string= prev2-wd-uc "CREATE")
           (string= prev-wd-uc "UNIQUE"))
      (pgc-complete-with-const "INDEX" text))

     ;; If we have CREATE|UNIQUE INDEX <sth> then add "ON"
     ((and (string= prev2-wd-uc "INDEX")
           (or (string= prev3-wd-uc "CREATE")
               (string= prev3-wd-uc "UNIQUE")))
      (pgc-complete-with-const "ON" text))

     ;; Complete ... INDEX <name> ON with a list of tables
;;;; ????
     ((and (string= prev3-wd-uc "INDEX")
           (string= prev-wd-uc "ON"))
      (pgc-complete-with-table-name text))

     ;; Complete INDEX <name> ON <table> with a list of table columns
     ;; (which should really be in parens)
     ((and (string= prev4-wd-uc "INDEX")
           (string= prev2-wd-uc "ON"))
      (pgc-complete-with-attr prev-wd text))

     ;; same if you put in USING
     ((and (string= prev4-wd-uc "ON")
           (string= prev2-wd-uc "USING"))
      (pgc-complete-with-attr prev3-wd text))

     ;; Complete USING with an index method
     ((string= prev-wd-uc "USING")
      (pgc-complete-with-list pgc-index-mth text))


     ;;; CREATE RULE
     ;; Complete "CREATE RULE <sth>" with "AS"
     ((and (string= prev3-wd-uc "CREATE")
           (string= prev2-wd-uc "RULE"))
      (pgc-complete-with-const "AS" text))

     ;; Complete "CREATE RULE <sth> AS with "ON"
     ((and (string= prev4-wd-uc "CREATE")
           (string= prev3-wd-uc "RULE")
           (string= prev-wd-uc "AS"))
      (pgc-complete-with-const "ON" text))

     ;; Complete "RULE * AS ON" with SELECT|UPDATE|DELETE|INSERT
     ((and (string= prev4-wd-uc "RULE")
           (string= prev2-wd-uc "AS")
           (string= prev-wd-uc "ON"))
      (pgc-complete-with-list pgc-rule-events text))

     ;; Complete "AS ON <sth with a 'T' :)>" with a "TO"
     ((and (string= prev3-wd-uc "AS")
           (string= prev2-wd-uc "ON")
;;; (mmc) FIXME
           (or (eq (position prev-wd-uc 4) ?T)
               (eq (position prev-wd-uc 5) ?T)))
      (pgc-complete-with-const "TO" text))

     ;; Complete "AS ON <sth> TO" with a table name
     ((and (string= prev4-wd-uc "AS")
           (string= prev3-wd-uc "ON")
           (string= prev-wd-uc "TO"))
      (pgc-complete-with-table-name text))


     ;;; CREATE TABLE
     ;; Complete CREATE TEMP with "TABLE"
     ((and (string= prev2-wd-uc "CREATE")
           (string= prev-wd-uc "TEMP"))
      (pgc-complete-with-const "TABLE" text))


     ;;; CREATE TRIGGER
     ;; is on the agenda . . .


     ;;; CREATE VIEW
     ;; Complete "CREATE VIEW <name>" with "AS"
     ((and (string= prev3-wd-uc "CREATE")
           (string= prev2-wd-uc "VIEW"))
      (pgc-complete-with-const "AS" text))

     ;; Complete "CREATE VIEW <sth> AS with "SELECT"
     ((and (string= prev4-wd-uc "CREATE")
           (string= prev3-wd-uc "VIEW")
           (string= prev-wd-uc "AS"))
      (pgc-complete-with-const "SELECT" text))


     ;;; DELETE
     ;; Complete DELETE with FROM (only if the word before that is not "ON"
     ;; (cf. rules) or "BEFORE" or "AFTER" (cf. triggers) )
     ((and (string= prev-wd-uc "DELETE")
           (or
            (not (string= prev2-wd-uc "ON"))
            (string= prev2-wd-uc "BEFORE"))
           (string= prev2-wd-uc "AFTER"))
      (pgc-complete-with-const "FROM" text))

     ;; Complete DELETE FROM with a list of tables
     ((and (string= prev2-wd-uc "DELETE")
           (string= prev-wd-uc "FROM"))
      (pgc-complete-with-table-name text))

     ;; Complete DELETE FROM <table> with "WHERE" (perhaps a safe idea?)
     ((and (string= prev3-wd-uc "DELETE")
           (string= prev2-wd-uc "FROM"))
      (pgc-complete-with-const "WHERE" text))


     ;;; EXPLAIN
     ;; Complete EXPLAIN [VERBOSE] (which you'd have to type yourself) with
     ;; the list of SQL commands
     ((or (string= prev-wd-uc "EXPLAIN")
          (and (string= prev2-wd-uc "EXPLAIN")
               (string= prev-wd-uc "VERBOSE")))
      (pgc-complete-with-list sql-commands text))



     ;;; FETCH && MOVE
     ;; Complete FETCH with one of FORWARD BACKWARD RELATIVE
     ((or (string= prev-wd-uc "FETCH")
          (string= prev-wd-uc "MOVE"))
      (pgc-complete-with-list pgc-list-FETCH1 text))


     ;; Complete FETCH <sth> with one of ALL NEXT PRIOR
     ((or (string= prev2-wd-uc "FETCH")
          (string= prev2-wd-uc "MOVE"))
      (pgc-complete-with-list pgc-list-FETCH2 text))

     ;; Complete FETCH <sth1> <sth2> with "FROM" or "TO". (Is there a
     ;; difference? If not remove one.)
     ((or (string= prev3-wd-uc "FETCH")
          (string= prev3-wd-uc "MOVE"))
      (pgc-complete-with-list list-FROMTO text))

     ;;; GRANT && REVOKE
     ;; Complete GRANT/REVOKE with a list of privileges
     ((or (string= prev-wd-uc "GRANT")
          (string= prev-wd-uc "REVOKE"))
      (pgc-complete-with-list pgc-list-privileg text))

     ;; Complete GRANT/REVOKE <sth> with "ON"
     ((or (string= prev2-wd-uc "GRANT")
          (string= prev2-wd-uc "REVOKE"))
      (pgc-complete-with-const "ON" text))

     ;; Complete GRANT/REVOKE <sth> ON with a list of tables views
     ;; sequences and indexes
     ((or (string= prev3-wd-uc "GRANT")
          (and (string= prev3-wd-uc "REVOKE")
               (string= prev-wd-uc "ON")))
      (pgc-complete-with-query
       "SELECT relname FROM pg-class WHERE relkind in ('r''i''S''v') and (substr relname,1,%d)='%s'"))

     ;; Complete "GRANT * ON * " with "TO"
     ((and (string= prev4-wd-uc "GRANT")
           (string= prev2-wd-uc "ON"))
      (pgc-complete-with-const "TO" text))

     ;; Complete "REVOKE * ON * " with "FROM"
     ((and (string= prev4-wd-uc "REVOKE")
           (string= prev2-wd-uc "ON"))
      (pgc-complete-with-const "FROM" text))

     ;; TODO: to complete with user name we need prev5-wd -- wait for a
     ;; more general solution there


     ;;; INSERT
     ;; Complete INSERT with "INTO"
     ((string= prev-wd-uc "INSERT")
      (pgc-complete-with-const "INTO" text))

     ;; Complete INSERT INTO with table names
     ((and (string= prev2-wd-uc "INSERT")
           (string= prev-wd-uc "INTO"))
      (pgc-complete-with-table-name text))


     ;; Complete INSERT INTO <table> with "VALUES" or "SELECT"
     ((and (string= prev3-wd-uc "INSERT")
           (string= prev2-wd-uc "INTO"))
      (pgc-complete-with-list pgc-list-INSERT text))

     ;; Insert an open parenthesis after "VALUES"
     ((string= prev-wd-uc "VALUES")
      (pgc-complete-with-const "(" text))

     ;;; LOCK
     ;; Complete with list of tables
     ((string= prev-wd-uc "LOCK")
      (pgc-complete-with-table-name text))

     ;; (If you want more with LOCK you better think about it yourself.)

     ;;; NOTIFY
     ((string= prev-wd-uc "NOTIFY")
      (pgc-complete-with-query "SELECT relname FROM pg-listener WHERE (substr relname,1,%d)='%s'" text))



     ;; REINDEX
     ((string= prev-wd-uc "REINDEX")
      (pgc-complete-with-list pgc-list-REINDEX text))

     ((string= prev2-wd-uc "REINDEX")
      (cond
       ((string= prev-wd-uc "TABLE")
        (pgc-complete-with-query pgc-q-tables) text)
       ((string= prev-wd-uc "DATABASE")
        (pgc-complete-with-query pgc-q-databases) text)
       ((string= prev-wd-uc "INDEX")
        (pgc-complete-with-query pgc-q-indexes text)
        )))


     ;; SELECT
     ;; naah . . .

     ;; SET RESET SHOW
     ;; Complete with a variable name
     ((or (and (string= prev-wd-uc "SET")
               (not (string= prev3-wd "UPDATE") ))
          (string= prev-wd-uc "RESET")
          (string= prev-wd-uc "SHOW"))
      (pgc-complete-with-list pgsql-variables text))


     ;; (mmc) FIXME   should be a skeleton/tempo ??
     ;; Complete "SET TRANSACTION ISOLOLATION LEVEL"
     ((and (string= prev2-wd-uc "SET")
           (string= prev-wd-uc "TRANSACTION"))
      (pgc-complete-with-const "ISOLATION" text))

     ((and (string= prev3-wd-uc "SET")
           (string= prev2-wd-uc "TRANSACTION")
           (string= prev-wd-uc "ISOLATION"))
      (pgc-complete-with-const "LEVEL" text))

     ((and (or (string= prev4-wd-uc "SET")
               (string= prev4-wd-uc "AS"))
           (string= prev3-wd-uc "TRANSACTION")
           (string= prev2-wd-uc "ISOLATION")
           (string= prev-wd-uc "LEVEL"))
      (pgc-complete-with-list pgc-my-list text))



     ((and (string= prev4-wd-uc "TRANSACTION")
           (string= prev3-wd-uc "ISOLATION")
           (string= prev2-wd-uc "LEVEL")
           (string= prev-wd-uc "READ"))
      (pgc-complete-with-const "COMMITTED" text))

     ;; Complete SET CONSTRAINTS <foo> with DEFERRED|IMMEDIATE
     ((and (string= prev3-wd-uc "SET")
           (string= prev2-wd-uc "CONSTRAINTS"))

      (pgc-complete-with-list pgc-constraint-list text))


     ;; Complete SET <var> with "TO"
     ((and (string= prev2-wd-uc "SET")
           (not (string= prev4-wd "UPDATE")))
      (pgc-complete-with-const "TO" text))

     ;; Suggest possible variable values
     ((and (string= prev3-wd-uc "SET")
           (or (string= prev-wd-uc "TO")
               (string= prev-wd "=")))
      ;; further divide:
      (cond
       ((string= prev2-wd-uc "DateStyle")
        (pgc-complete-with-list pgc-datestyle-list text))
       ((or (string= prev2-wd-uc "GEQO")
            (string= prev2-wd-uc "KSQO"))
        (pgc-complete-with-list pgc-geqo-list text))

       ( 't
         (pgc-complete-with-list pgc-default-list text))))


     ;; TRUNCATE
     ((string= prev-wd-uc "TRUNCATE")
      (pgc-complete-with-table-name text))


     ;; UNLISTEN
     ((string= prev-wd-uc "UNLISTEN")
      (pgc-complete-with-query
       "SELECT relname FROM pg-listener WHERE (substr relname,1,%d)='%s' UNION SELECT '*'::text"))


     ;;; UPDATE
     ;; If prev. word is UPDATE suggest a list of tables
     ((string= prev-wd-uc "UPDATE")
      (pgc-complete-with-table-name text))

     ;; Complete UPDATE <table> with "SET"
     ((string= prev2-wd-uc "UPDATE")
      (pgc-complete-with-const "SET" text))

     ;; If the previous word is SET (and it wasn't caught above as the
     ;; -first- word) the word before it was (hopefully) a table name and
     ;; we'll now make a list of attributes.
     ((string= prev-wd-uc "SET")
      (pgc-complete-with-attr prev2-wd text))

     ;;; VACUUM
     ((string= prev-wd-uc "VACUUM")
      (pgc-complete-with-query
       "SELECT relname FROM pg-class WHERE (setq relkind'r' and (substr relname,1,%d)='%s' UNION SELECT 'ANALYZE'::text"))

     ((and (string= prev2-wd-uc "VACUUM")
           (string= prev-wd-uc "ANALYZE"))
      (pgc-complete-with-table-name text))


     ;; ... FROM ...
     ((string= prev-wd-uc "FROM")
      (pgc-complete-with-table-name text))


     ;;; Backslash commands
     ((or (string= prev-wd "\\connect")
          (string= prev-wd "\\c"))
      (pgc-complete-with-query pgc-q-databases) text)

     ((string= prev-wd "\\d")
      (pgc-complete-with-table-name text))

     ((or (string= prev-wd "\\h")
          (string= prev-wd "\\help"))
      (pgc-complete-with-list sql-commands) text)

     ((string= prev-wd "\\pset")
      (pgc-complete-with-list pgc-pset-commands text))

     ((or (string= prev-wd "\\e")
          (string= prev-wd "\\edit")
          (string= prev-wd "\\g")
          (string= prev-wd "\\i")
          (string= prev-wd "\\include")
          (string= prev-wd "\\o")
          (string= prev-wd "\\out")
          (string= prev-wd "\\s")
          (string= prev-wd "\\w")
          (string= prev-wd "\\write"))
      ;; (mmc) FIXME: I want to  (read-file-name (format "File for (%s): "prev-wd)
      (set! matches (completion-matches text filename-completion-function)))



     ;; Finally we look through the list of "things" such as TABLE INDEX
     ;; and check if that was the previous word. If so execute the query
     ;; to get a list of them.
     ((assoc prev-wd-uc pgc-words-after-create) =>
      (lambda (info)
        (pgc-complete-with-query  (pgc-query-of info) text)))

     ;; Between select from  --- attributes from user's tables ??
     ;; between from where
     ;; between ...


     ;; this was my 1st trial

					;      ((pgc-between "from" "where")
					;       (pgc-complete-with-table-name text)
					;       )
					;       ('t
					;       (pgc-add-keywords
					;        (pgc-complete-attribute text))
					;       )
					;      )
     (
      ;; Complete "AS ON <sth> TO" with a table name
      (string= prev-wd-uc "JOIN")
      (pgc-complete-with-table-name text)
      )
     (
      ;; Complete "AS ON <sth> TO" with a table name
      (string= prev2-wd-uc "JOIN")
      (pgc-complete-with-list (list "on" "using") text))

     ('t
      (pgc-add-keywords
       (pgc-complete-attribute-or-table text))))))


;; the list of keywords is already in sql.el !
;; todo: Should be context sensitive!
(define (pgc-add-keywords alist-or-obarray)
  "given the ALIST-OR-OBARRAY, add the suitable keywords"
  (let ((kw-list '("from" "where" "group" "order" "having" "or" "and" "not")))
    (if (vectorp alist-or-obarray)
	(copy-alist-to-obarray (alist-from-list kw-list) alist-or-obarray)
      (set! alist-or-obarray
            (append!
             alist-or-obarray
             (alist-from-list kw-list))))
    alist-or-obarray))
;(pgc-add-keywords '())



(provide "pg/complete")

