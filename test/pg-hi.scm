(use gauche.test)

(test-start "pg-hi API features")

(use pg)
(use adt.string)

(use pg-hi)
;(load "pg-low")

(test-module 'pg-hi)
;(select-module pg-low)

;; cycle:
;; py-hi pg.caching pg.db pg-hi
;;       pg-type-name .... this is not searching the hash table!
;;                  pg:with-admin-handle
;;
;; how is pg-type-name used?

;;; Configuration
;(sys-setenv "PG_MMC" "y")

(test-section "Open connection")

(define pg-hi-handle
  (pg-open :dbname "test"
                 :user "michal"))

(test* "pg-open with keywords"
       #t
       (is-a? pg-hi-handle
              <pg>))

;(test* "Extract low handle" (->db pg-hi-handle))

;; since types are initialized automatically:
(test-section "Types")
;; todo: pg-find-type


(test-section "Integrate Types with parsers")
;; pg-printer

;;;
(test-section "Result")



(test-section "Other commands")

(test-end :exit-on-failure #t)
