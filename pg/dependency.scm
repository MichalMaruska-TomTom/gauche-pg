
;; fk-constraints 

;; we use a `admin' pg namespace, to store ....  `constraints' `indexes'
(define-module pg.dependency
  (export
   pg:relations-order
   pg:depends-on?
   )
  (use adt.alist)
  (use adt.string)
  (use util.list)

  (use pg.oo)
  (use pg-hi)
  (use pg.types)
  (use pg.sql)
  (use pg.database)
  (use pg.recordset)
  (use pg.cursor)
  ;; mine!!!
  (use util.toposort)
  (use mmc.log)
  (use srfi-1)
  )


(select-module pg.dependency)


;; in order of dependency (by f-keys relation!)
;; should return the edges as well?
;;                                         ;(hash-table-keys (ref db 'relations))



;; I can have relation between relations of 2 namespaces?
(define (pg:relations-order db)         ; only some?
  (let* ((relations (pg:database-relations db 1))
         (f-keys (ref db 'foreign-keys))
         ;; Fixme: 2 f-keys (edges) between the same 2 vertices are unexpected by `topological-sort'
         (unique-f-keys
          (delete-duplicates!
           f-keys
           (lambda (a b)
             (let ((am (ref (ref a 'master) 'oid))
                   (as (ref (ref a 'slave) 'oid))
                   (bm (ref (ref b 'master) 'oid))
                   (bs (ref (ref b 'slave) 'oid)))
               (and
                (= am bm)
                (= as bs)))))))


    (reverse (topological-sort
              (make-dependency-graph relations unique-f-keys ;f-keys
                           (lambda (f-key)
                             (values (ref f-key 'master)
                                     (ref f-key 'slave))))))))


;; namespace 
;; fixme: should accept 2 relations, not relnames!
(define (pg:depends-on? db slave master)
  (let ((master-r (pg:get-relation db master))
        (slave-r (pg:get-relation db slave))
        (f-keys (ref db 'foreign-keys)))
    (find
     (lambda (f-key)
       (and (eq? master-r (ref f-key 'master))
            (eq? slave-r (ref f-key 'slave))))
     f-keys)))


;; consult the   `pg_depend' relation?  No!
(define (pg:depends-on? slave master)
  (find
   (lambda (f-key)
     (and (eq? master-r (ref f-key 'master))
          (eq? slave-r (ref f-key 'slave))))
   f-keys))


;;; fixme: abandoned!   ....  the dependencies are not among relations, but relation->index etc.
;; return relations which depend on `relation'
(define (pg:dependent-relations relation . lesser?)
  (pg:with-handle-of relation
    (lambda (h)
      (let* ( ;; we don't want `functions' or other objects (other than relations)
             ;; as the result.   so limit it to objects from pg_clas  ...but even more! todo!
             (pg:pg-class (pg:get-relation (pg:get-namespace pg "pg_catalog") "pg_class"))
             (class-id (number->string (ref pg:pg-class 'oid)))
             (query (sql:select
                     ;; projection!
                     '("objid" "objsubid"
                       ;; "refobjid"
                       "refobjsubid"
                       "deptype")

                     "pg_depend"
                     (s+ (sql:alist->where
                          `(
                            ;; Dependencies between TABLES:
                            ("classid" . ,class-id)
                            ("refclassid" . ,class-id)
                            ;;
                            (,(if (null? lesser?)
                                  "objid"
                                "refobjid")
                             . ,(number->string (ref relation 'oid))) ; rel-oid
                            ))
                                        ;" AND  deptype !~'[ai]'"
                         )))
             (r (pg-exec h query))
             ;; -> pg_constraint 
             (ids (pg-collect-result r "objid")))
        ids))))



;; we want some relations
;; add `masters', i.e. search in oriented graph.

;; returns a list of relations .... todo:  (types?) ... functions & rules ?
(define (pg:prerequisites relation)
  (let* ((f-keys (pg:fkey-to relation))
         (rels (map (lambda (i)
                      (ref i 'master))
                 f-keys)))
    (delete-duplicates! rels eq?)))


;; Find the set of reachable nodes in a graph given by neighbour function `generate-edges'
;; from the `init-set'
(define (graph-closure init-set generate-edges)
  (let step ((reached init-set)
             (new init-set))

    (let* ((neighbours (append-map generate-edges new))
           (now-new (lset-difference eq? neighbours reached)))
      (if (null? now-new)
          reached
        (step (lset-union eq? reached now-new)
              now-new)))))

(provide "pg/dependency")
