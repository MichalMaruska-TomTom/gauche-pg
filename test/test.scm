


(use pg)

(define p (pg-open ""))

(define r (pg-exec p "select * fro m person;"))

(use mmc.log)


(define (pg-dump-error-slots result)
  (format
      "dump: ~a\n ~a\n~a\n  ~a\n ~a\n~a\n   ~a\n ~a\n~a\n ~a\n"
    (pg-result-error-field result PG_DIAG_MESSAGE_PRIMARY)
    (pg-result-error-field result PG_DIAG_MESSAGE_DETAIL)
    (pg-result-error-field result PG_DIAG_STATEMENT_POSITION)

    (pg-result-error-field result PG_DIAG_SEVERITY)
                   
    (pg-result-error-field result PG_DIAG_SQLSTATE)
    (pg-result-error-field result PG_DIAG_MESSAGE_HINT)
    (pg-result-error-field result PG_DIAG_CONTEXT)
    (pg-result-error-field result PG_DIAG_SOURCE_FILE)
    (pg-result-error-field result PG_DIAG_SOURCE_LINE)
    (pg-result-error-field result PG_DIAG_SOURCE_FUNCTION)
    ))

(define r
  (with-error-handler
      (lambda (result)
        (pg-dump-error-slots result)
        result)
    (lambda ()
      (pg-exec (pg-open "") "select * fro m person;"))))




;; sequence:

(use gauche.collection)
(use pg-hi)
(define pg (pg-open "linux2"))
(define res (pg-exec pg "select * from trad where tipo = 'caratter';"))
(use mmc.log)

(for-each (lambda (row) (logformat "~a\n" (row 0))) res)
