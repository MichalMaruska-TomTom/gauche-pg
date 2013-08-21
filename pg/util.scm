
;; `obsolete' !

;; not inherent to include into pg.
;; But standard accross my code.

(define-module pg.util
  ;; common functions, macros to interact w/ PG dB.
  (export
   handle-or-host->handle
   ;truncate
   disable-trigggers-on
   ;; fixme:  pg:with-triggers-*
   with-triggers-disabled with-triggers-disabled*

   ;pg-dump-explain
   pg-prepare-handle

   pg:with-transaction
   ;; fixme: make obsolete ... warn?
   with-transaction with-transaction*
   make-error-collector-monitor

   ;;
   pg-describe-error-on-connection
   pg-describe-error-of-result
   )
  (use pg-hi)
  (use pg)
  (use pg.types)
  (use mmc.log)
  (use mmc.simple)
  (use srfi-19)
  (use srfi-2)
  (use mmc.exit)
  )
(select-module pg.util)


(define (handle-or-host->handle source)
  (if (string? source)
      (pg-connect (format "host=~a" source))
                                        ;(eq? (class-of source) <pg-handle>)
    source))


;(use srfi-2)

;; must not be inside a transaction !!
(define (disable-trigggers-on handle relname . rest)
  (let-optionals* rest
      ((state "false"))
    (pg-exec handle
      ;; (format #f "update pg_class set reltriggers = 0 where relname = '~a';" relname)) ;; fixme:
      (string-append
       ;; sql::update
       "UPDATE pg_trigger SET tgenabled= "
       (pg:text-printer state)
       " FROM pg_class x WHERE(tgrelid=x.oid) AND x.relname="
       (pg:text-printer relname) ";"))))


;; todo: more relnames ?
(define (with-triggers-disabled handle relname body) ; :verbose   TAG (for messages)
  ;;fixme: check that we are in a transaction!!
  (unwind-protect* (lambda (exception)
                     (if (= (pg-status handle) CONNECTION_OK)
                         (begin
                           (logformat "triggers: setting back\n")
                           (disable-trigggers-on handle relname "true"))
                       (logformat "triggers: connection down, we don't either re-activate\n")))
    (disable-trigggers-on handle relname)
    (logformat "triggers disabled!\n")
    (body)))


(define-syntax with-triggers-disabled*
  (syntax-rules ()
    ((_ handle relname body ...)
     (with-triggers-disabled handle relname
       (lambda ()
         body ...)))))

(define (pg-prepare-handle handle)
  (logformat "pg-prepare-handle\n")
  (pg-exec handle
    "set enable_seqscan to 0;")
  (pg-exec handle
    "set DateStyle TO 'European';")
  (pg-exec handle
    "set debug_print_plan to on"))

;(push! pg-handle-hook pg-prepare-handle)
;(push! pg-handle-hook 'pg-prepare-handle)

;(pg-connect "")
;(set! pg-handle-hook '())



(define (with-transaction handle thunk)
  (pg-exec handle "BEGIN;")
  (logformat "opening transaction: BEGIN\n")
  (unwind-protect (lambda (exception?)
                    ;; (logformat "closing transaction\n")
                    ;;(pg-status handle)
                    (cond
                     ((not (= (pg-status handle) CONNECTION_OK))
                      (logformat "with-transaction: connection is not OK. ignoring\n")) ;(pg-status handle)
                     (exception?
                      (logformat "aborting transaction: ABORT;\n")
                      (pg-exec handle "ABORT;")
                      (error exception?)) ;fixme!
                     (else
                      (logformat "committing transaction: COMMIT;\n")
                      ;; i want to be able to skip here, But on normal exit not?
                      (pg-exec handle "COMMIT;"))))
    ;(thunk handle)
    thunk))
(define pg:with-transaction with-transaction)


(define-syntax with-transaction*
  (syntax-rules ()
    ((_ handle body ...)
     (with-transaction handle
       (lambda ()
         body ...)))))

;; fixme:  use `<pg-exporter-data>' !!
;; collector is an object with slot 'errors.
(define (make-error-collector-monitor collector)
  (lambda (handle message)
    (when (rxmatch #/ERROR:  copy: line (\d*),/  message)
      (logformat-color debug-color "local-notice-processor:")
      (logformat "~a\n" message)
      (push! (ref collector 'errors) message)
      ;; run previous handler...  no: this is asynchronous. !!!
      )))

'(define (with-chained-monitor handle monitor)
  (let previous ;; ... get the current!!!
      (unwind-protect
          )
    ))

(define (pg-describe-error-of-result result)
  (logformat "|-severity: ~a\ndetail: ~a\nhint: ~a\nline: ~a\nprimary:~a\nposition: ~a|_\n"
    (pg-result-error-field result PG_DIAG_SEVERITY)
    (pg-result-error-field result PG_DIAG_MESSAGE_DETAIL)
    (pg-result-error-field result PG_DIAG_MESSAGE_HINT)
    (pg-result-error-field result PG_DIAG_SOURCE_LINE)
    (pg-result-error-field result PG_DIAG_MESSAGE_PRIMARY)
    (pg-result-error-field result PG_DIAG_STATEMENT_POSITION)))

(define (pg-describe-error-on-connection handle)
  (logformat "|-status: ~d\npg-error-message: ~a|_\n"
    (pg-status handle)
    (pg-error-message handle)))

(provide "pg/util")

