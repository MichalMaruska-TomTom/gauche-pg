
;; This is the wrapper of C code.
;; There is also a callback to Scheme: `pg-handle-hook' see ../pg-lib.c

;; todo: the result of BEGIN indicates, that the handle enters a transaction
;; ..... try to detect it!!

(define-module pg
  (export-all)

  ;; fixme: the C code uses it, but I'm unable to do this from C
  (use gauche.uvector)
  )

(select-module pg)
(dynamic-load "pg")

(define debug #f)

;; This is used in the C code
(define pg-handle-hook '())

(define (pg-set-coding handle coding)
  ;; utf8
  (pg-exec handle
    (format "SET client_encoding TO ~a;" coding)))

;;; hook:
(define (pg-prepare-handle handle)
  ;; similar to ~/.psqlrc
  (if debug (logformat "pg-prepare-handle\n"))
  ;; tune the planner:
  (pg-exec handle "set enable_seqscan to 0;")
  (pg-exec handle "set DateStyle TO 'European';")
  ;(pg-exec handle "set debug_print_plan to on")
  )


(unless (member pg-prepare-handle pg-handle-hook)
  (push! pg-handle-hook pg-prepare-handle))

(provide "pg")
