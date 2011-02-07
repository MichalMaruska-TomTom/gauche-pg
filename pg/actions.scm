;; I have an `action' bound ... read:
;; http://maruska.dyndns.org/wiki/db-browser.html

;; Here we track the less-equal attribute-groups:
(define-module pg.actions
  (export
   pg:find-fkey-closure
   pg:register-action
   pg:find-actions-for
   )
  (use srfi-1)
  ;; (use pg.database)
  (use pg.base)
  (use pg.relation)
  (use pg.keys)
  (use pg.attribute)

  (use macros.reverse)
  (use mmc.log)
  (use adt.alist)
  (use srfi-1)
  )
(select-module pg.actions)

(define debug #f)



;;; Given a set of ATTRIBUTES (of some pg relation), and a foreign-key, which references that relation
;;; return a list of attributes (in the right order) which reference those ATTRIBUTES.

;; is it just a ??????
(define (project-under-rkey atts fkey)
  (let ((relation (ref fkey 'slave))
        (attnums (fold
                  (lambda (m s seed)
                    (if (find
                         (lambda (att)
                           (= (ref att 'attnum) m))
                         atts)
                        (cons s seed)
                      seed))
                  ()
                  (ref fkey 'm-fields)
                  (ref fkey 's-fields))))
    (map
        (cute pg:nth-attribute relation <>)
      attnums)))



;;; given a `<db-f-key>'  `<pg-foreign-key>'
;;; decide if atts include it (by set inclusion).
(define (fkey-involves fkey atts)
  (lset<=
   (lambda (a b)
     (= (ref a 'attnum) b))
   atts
   (ref fkey 'm-fields)))



;;; attribs comes from 1 relation!
(define (pg:register-action namespace attribs action)
  (let1 closure (pg:find-fkey-closure attribs namespace)
    ;; divide the groups by relation:
    (let* ((ht (make-hash-table 'eq?)))
      (for-each-reverse closure
        (lambda (group)
          (let1 relation (relation-of (car group))
            ;; add-in-alist
            (hash-table-push! ht relation group))))

      (hash-table-for-each ht
        ;; (for-each-reverse info
        (lambda (relation groups)
          (if debug (logformat "registering in ~a: ~a\n" relation groups))
          (let1 l (or (pg:relation-get relation 'actions)
                      ())
            (pg:relation-put!
             relation
             'actions
             (aput l action groups))
            (if debug (logformat "registered: ~a\n~a" (pg:relation-get relation 'actions)
                                 (aput l action groups))))
            )))))


;; simple look-up in the table?
;; Just needs to find by set-inclusion. action bound to a subset-> bound to the set!
;; Returns list: ((action . list-of-atts) .....)
(define (pg:find-actions-for relation atts)
  (let* ((infos (pg:relation-get relation 'actions)))
    ;; we iterate over actions and over the groups
    ;; and collect such pairs! (not only the action, but also the binding)
      (if infos
          (let1 available ()
            (for-each-reverse infos
              (lambda (info)
                (let ((action (car info))
                      (groups (cdr info)))

                  (for-each-reverse groups
                    (lambda (group)
                      (if (lset<= eq? group atts)
                          (push! available (cons action group))))))))
            available)
        (begin
          (logformat-color 'red
              "pg:find-actions-for:  the relation ~a does not have actions associated!\n"
            (ref relation 'name))
          ()))))



;; Given a set of attributes of one relation, find
;; all possible attribute-sets (of single relations),
;; which are lesser in the partial ordering by foreign-key relation.
;; i.e. returns a list.
;; depth first search ?
(define (pg:find-fkey-closure attribs namespace)
  (let step ( ;; queue of nodes to expand
             (reached (list attribs))
             ;; so far found (part of closure):
             (group ())) ;;  (list attribs)

    
    (if (null? reached)
        group
      (begin
        (logformat "step: ~a\n" (car reached))
        (let* ((investigating (car reached))
               (relation (relation-of (car investigating)))
               (fkeys (pg:fkey-under relation))
               (relevant-fkeys (filter
                                   (cute fkey-involves <> investigating)
                                 fkeys)))
          ;; 
          (if (null? relevant-fkeys)
              ;; no way futher!
              (step
               (cdr reached)
               ;; fixme: Add here?
               (cons investigating
                     group))
            (let* ((newly-reached (map
                                      (cute project-under-rkey investigating <>)
                                    relevant-fkeys))
                   (new-atts (filter
                                 (lambda (a)
                                   ;; not memeber of the reached!
                                   (not
                                    (or (member a
                                                ;; (new-atts)
                                                group)
                                        (member a
                                                ;; (new-atts)
                                                reached))))
                               newly-reached)))
              (logformat "adding to queue: ~a\n" new-atts)
              (step
               (append
                new-atts
                (cdr reached))

               ;; fixme: Add here?
               (cons investigating
                     group)))))))))


(provide "pg/actions")