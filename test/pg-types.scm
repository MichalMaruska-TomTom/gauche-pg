
;; API:
;; (test* "name" expected-value   body....)

(use gauche.test)

(test-start "pg.types API features")

(use pg.types)
;(use adt.string)

(test-module 'pg.types)
;(select-module pg-low)

(test-section "pg: name-printer")
(test* "printer"
       "\"a b\""
       (pg:name-printer "a b"))


(test-section "pg: parsers")
(test* "parse bool"
       #t ;; scheme     pg value
       ((cdr (assoc "bool" pg:type-parsers)) "t"))


;; send from scheme to pg & back:
(let ((type "float4")
      (flo 1.0001)
      (flo-as-string "1.0001")
      )
  (test* "float  scheme->pg->scheme"
         flo ;; scheme     pg value
         ((pg:parser-for type)
          ((pg:printer-for type) flo)))

  ;; and vice versa
  (test* "float pg->scheme->pg"
         flo-as-string ;; scheme     pg value
         ((pg:printer-for type)
          ((pg:parser-for type) flo-as-string)))
  )


;; pg-array->list-number
;; pg-array->list
;; "timestamptz"

'(
  pg:isodate-printer
  "time"
  pg:date-parser
  )
;;
;;; Configuration
;(sys-setenv "PG_MMC" "y")


(test-end :exit-on-failure #t)
