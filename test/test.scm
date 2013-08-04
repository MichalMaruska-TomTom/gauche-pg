#!/usr/bin/gosh



(use pg)
(use pg-hi)
(use mmc.log)

(define p (pg-open ""))
(define r (pg-exec p "select * from person;"))

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
      (pg-exec (pg-open "")
	"select * from person;"))))




;; sequence:

(use gauche.collection)
(use pg-hi)
(use mmc.log)
(use macros.reverse)
(define pg (pg-open ))
(define res (pg-exec pg "select * from trad where tipo = 'caratter';"))

;; fixme:
;; for-each-reverse complains about: not list, but pg-result.
(for-each
    (lambda (row) (logformat "~a\n" (row 0)))
  res)
