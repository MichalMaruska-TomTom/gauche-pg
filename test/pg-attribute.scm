(use gauche.test)

(test-start "pg.attribute API features")

(use pg.attribute)
(test-module 'pg.attribute)


; (test-section "Test private APIs")
; (select-module pg.attribute)
; (use gauche.test)


(test-end :exit-on-failure #t)
