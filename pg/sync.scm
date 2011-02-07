;; Synchronize a table in 2 DBs.
;; Requirement: order on the table.
;; Result: relation in DB2 is equal to relation in DB1
;;         + a diff-relation in DB2 is updated (by inserting new records-differences)


(define-module pg.sync
  (export

   ;; Main top entry:
   pg:sync-cursors
   ;; To call this function you need to provide functions, which
   ;; you can created assisted by:

   pg:process-changes-in-rows
   pg:compare-rows-by-key
   
   pg-sync:make-simple-updater
   pg-sync:make-delta-updater
   
   )
  (use macros.assert)
  (use pg-hi)
  (use pg.database)
  (use pg.cursor)
  (use pg.types)
  (use pg.sql)

  (use pg.compare)

  (use srfi-1)
  (use mmc.log)
  (use mmc.simple)
  (use adt.string)
  )
(select-module pg.sync)

(define debug #f)


;;; Low-level utils

;;; Top `merging' algorithm 

;; This needs foreign key in ...

;; Walk the cursors & possibly  DELETE / Insert / Update  `H2'
;; Walk the 2 cursors c1 c2 (ordered rows)
;; by using COMPARE
;; and run `update' for modified rows (compare returns 0)
;; or `delete'/`insert'
;;    delete from relname
;;    insert into relname
;; Yes, relname1 would be useless! 
;; note: it is similar to comparing 2 alists!?
(define (pg:sync-cursors c1 c2 relname compare-function update delete insert . rest)
  (let-optionals* rest
      ((cursor-step 100))
  (logformat "pg:sync-cursors: ~d\n"  (pg-backend-pid (ref (ref c2 'handle) 'conn)))
    
  (receive (r1 i1) (pg:fetch-from-cursor c1 cursor-step)
    (receive (r2 i2) (pg:fetch-from-cursor c2 cursor-step)
      (let step ((r1 r1)
                 (i1 i1)
                 (r2 r2)
                 (i2 i2))
        (cond
         ((not i1) ;; no more rows in c1
          (if i2
              (begin
                (logformat "delete the rest of r2 !\n")
                (let final-step-delete ((r r2)
                                        (i i2))
                  (if i
                      (begin
                        ;; fixme!
                        (delete r i)
                        (receive (r i) (pg:fetch-from-cursor c2 cursor-step)
                          (final-step-delete r i)))))))
          1)
         ((not i2)
          (logformat "Insert the rest of r1 !\n")
          (let final-step-insert ((r r1)
                                  (i i1))
            (if i
                (begin
                  (insert r i relname)
                  (receive (r i) (pg:fetch-from-cursor c1 cursor-step)
                    (final-step-insert r i)))))
          2)
         (else
          (let1 relation (compare-function r1 i1 r2 i2)
            ;; (compare 0 1)
            ;; The order is given by a list of (attname . CMP) functions?

            (cond
             ;; Different keys  < >
             ((zero? relation)
              ;; Check & find what differs
              ;; update & step ahead
              (update r1 i1 r2 i2)
              ;; ok, step ahead
              (receive (r1 i1) (pg:fetch-from-cursor c1 cursor-step)
                (receive (r2 i2) (pg:fetch-from-cursor c2 cursor-step)
                  (step r1 i1 r2 i2))))

             ((< relation 0)
              ;; Insert & step ahead
              (insert r1 i1 relname)

              (receive (r1 i1) (pg:fetch-from-cursor c1 cursor-step)
                (step r1 i1 r2 i2)))

             (else
              (delete r2 i2)
              ;; Delete & step ahead
              (receive (r2 i2) (pg:fetch-from-cursor c2 cursor-step)
                (step r1 i1 r2 i2))))))))))))


;; pg-ftable
;; Given 2 results, and rows in them (by index) compare them:
;; the result must be "isomorphic" ... we consider only r1 (indices on r1)
;; see if attributes/columns differ. IGNORE (list of indices) some of the columns.
;; If they differ, call UPDATER.
;; Return list of the modified columns ... indices.

;; fixme `-and-update'
(define (pg:process-changes-in-rows r1 i1
                        r2 i2
                        ;relname1 relname2 ; key ;port1 port2
                        ignore updater) ;-attributes
  ;;(logformat "comparing\n")
  (let1 changed-atts ()
    ;; Find what differs:
    (for-numbers< 0 (pg-nfields r1)
      (lambda (i)
        ;; ignore changes in `mtime'!
        (unless (memv i ignore)         ;fixme! make it faster?  lookup in a bit vector!

          (let ((v1 (pg-get-value-string r1 i1 i))
                (v2 (pg-get-value-string r2 i2 i)))
                                        ;(equal?
            (if (eof-object? v1)
                (if (eof-object? v2)
                    1
                  (push! changed-atts i)
                  )
              (if (eof-object? v2)
                  (push! changed-atts i)

                (if (string=? v1 v2)
                    1
                  (push! changed-atts i)
                  )))))))
    (unless (null? changed-atts)
      (updater r1 i1 r2 i2 changed-atts)) ;port1 port2
    changed-atts))


;; ignore numbers: 10383 131282

;;; compare-fs  is a list of `compare' functions (for different types).
;;; ie. they return 1 0 -1 to indicate the order!
;;;
(define (pg:compare-rows-by-key r1 i1 r2 i2 key-1 key-2 compare-fs)
  ;; Assert:
  (assert (= (length key-1)
             (length key-2)
             (length compare-fs)))
  ;; every !
  (let step ((rest1 key-1)
             (rest2 key-2)
             (functions compare-fs))
    (cond
     ((null? rest1)
      ;; Indistinguishable:
      0)
     ;((null? functions)
     ; (error "dunno how to compare " (car rest)))
     (else
      (let* ((v1 (pg-get-value r1 i1 (car rest1))) ; (pg-fnumber r1 (car rest))
             (v2 (pg-get-value r2 i2 (car rest2))) ; pg-fnumber r2 (car rest))))
             ;; fixme:  NULLs come AFTER ?
             (c (if (eof-object? v1)
                    (if (eof-object? v2)
                        0
                      1)
                  (if (eof-object? v2)
                      -1
                    ((car functions) v1 v2)))))
        (if (zero? c)
            (step (cdr rest1)
                  (cdr rest2)
                  (cdr functions))
          ;; decided:
          (begin
            (if debug (logformat "compare: decision on ~a vs ~a: ~d\n" v1 v2 c))
            c)))))))



;; Updater:
;; is a a 2-tuple (init updater) of 2 functions which:
;; INIT:  called w/ 2 args: Result & Relname
;;         prepares by side-effect !
;; UPDATER: update the relation  as needed by the difference between 2 rows/results
;;            & according to the primary key.


;; Simple: Just overwrite i.e. print into
;; port1 `updates' to return to result2 values
;; port2 updates to `update' to result1 values ?
(define (pg-sync:make-simple-updater pg)
  (let ((key-indices ())
        (relation #f))

    (define (init-simple-updater relname r2)
      (set! relation (pg:find-relation pg relname))
      (unless (ref relation 'p-key)
        (errorf "pg relation ~a does not have PRIMARY KEY" relname))
      (set! key-indices
            (map
                (lambda (att)
                  ;; (logformat "~s\n"
                  (pg-fnumber r2 
                              (ref att 'attname)))
              (ref relation 'p-key))))

    (define (simple-updater r1 i1 r2 i2 changed-atts port1 port2)
      (let* ((key-values
              (map
                  (lambda (key-att)
                    ;; (pg-fnumber r1 key-att)
                    (pg-get-value-string-null r1 i1 key-att)) ;this is not NULL, it's in pkey, but ...!
                key-indices))

             (where
              (sql:alist->where
               (map cons
                 (map
                     (cute pg-fname r2 <>)
                   key-indices)
                 key-values))))
        (port1 #\u
               (s+ 
                (sql:update
                 (ref relation 'name)
                 (map cons
                   (map (cute pg-fname r1 <>)
                     changed-atts)
                   (map
                       (cute pg-get-value-string-null r1 i1 <>)
                     changed-atts)
                   )
                 where)
                ";\n"))
        (port2 #\u
               (s+ 
                (sql:update
                 (ref relation 'name)
                 (map cons
                   (map (cute pg-fname r2 <>)
                     changed-atts)
                   (map
                       (cute pg-get-value-string-null r2 i2 <>)
                     changed-atts)
                   )
                 where)
                ";\n"))))
    (values init-simple-updater  simple-updater)))


;;; This (instead overwriting value to emulate either of the tables)
;;; emulates 1 (the first) relation, and records to a `Delta' relation
;;; what changed!
(define (pg-sync:make-delta-updater pg)

  (let ((irrelevant ())
        (delta-relation ())
        (delta-atts ())                 ; ((eta . index) ...
        (relation #f)
                                        ; where index is in the R2 !
                                        ; why not R1?
        ;; indices in the result, which make up a key in r2
        (key-indices ()))

    ;; fixme: i could get pg2 from r2 ?
    (define (delta-updater-prepare relname r2) ;relation
      ;; Find the delta relation
      ;; optional: find IP (& other archive-specific attributes, time?) 
      ;;
      ;; INSERT INTO  ....  VALUES  ....
      (set! relation (pg:find-relation pg relname))

      (let* (                           ;(relname (ref relation 'name))
             (d-relname (s+ "delta_" relname))
             (delta-rel (pg:find-relation pg d-relname)))

        ;; fixme:
        (set! key-indices
              (map
                  (lambda (att)
                    ;; (logformat "~s\n"
                    (pg-fnumber r2 
                                (ref att 'attname)))
                (ref relation 'p-key)))
    
        (set! delta-relation delta-rel)
        ;; Th
        (let* ((delta-attnames (lset-difference
                                string=?
                                (pg:attributes-of delta-rel)
                                '("dip" "dtime")))
               ;; Now, where are they?
               ;;(delta-columns )
               )

          ;;(set! irrelevant (map (cute pg-fnumber r <>) irrelevant-attributes))
          ;; Find `delta-fields'
          (set! delta-atts (map
                               (lambda (a)
                                 (cons
                                  ;; fixme: This should keep the names!
                                  (pg-fnumber r2 a) ;; note from `R2'!
                                  a))
                             delta-attnames))

          (logformat
              "out of the result, these indices are to be archived: ~a\n"
            delta-atts)
          ;;(set! irrelevant (map (cute pg-fnumber r <>) irrelevant-attributes))
          )))


    ;; mapping
    ;; Different is (0 5 ...)  indices of what differs (IGNORED are not reported!)
    ;; delta is     (0 1 ....) + pkey! 
    (define (delta-updater r1 i1 r2 i2 changed-atts port1 port2) 
                                        ;(unless (null? changed-atts)
      (let ((changed-atts-names
             (map
                 (cute pg-fname r1 <>)
               changed-atts)))
        (logformat "delta-updater: ~a\n" changed-atts-names) ;~a\t changed-atts
        (let* ((=-test (lambda (a b)
                         (= (car a) b)))

               ;; Indices to save (not including key) fixme: the key must be disjoint!
               (to-be-archived (lset-intersection =-test
                                                  delta-atts changed-atts))

               (interesting-changes
                (filter
                    (lambda (i)
                      (not (or (pg-get-isnull r2 i2 (car i)) ;;fixme: car i
                               (string=? "" (pg-get-value-string r2 i2 (car i))))))
                  to-be-archived))

               (update-indices
                (if #t
                    changed-atts
                  (lset-difference =-test
                                   changed-atts to-be-archived)))
         
               ;; 
               (key-values
                (map
                    (lambda (key-att)
                      ;; (pg-fnumber r1 key-att)
                      (pg-get-value-string r1 i1 key-att))
                  key-indices))

               (where
                (sql:alist->where
                 (map cons
                   (map
                       (cute pg-fname r1 <>)
                     key-indices)
                   key-values)))
               )
          (if debug
              (unless (null? to-be-archived)
                (logformat-color 'green "INTERSECTION: ~a\n" to-be-archived)
                (logformat-color 'yellow "INTERESTING: ~a\n" interesting-changes)))

          (if debug
              (logformat "changed!: ~a: ~a ~a\n"
                key-values
                changed-atts
                changed-atts-names))


          ;; push in archive/delta-table the to-be overwritten values:

          ;; Relevant:
          ;;   status  via nota  cognome email
          ;;

          (unless (null? interesting-changes)

            ;; fixme: Only  non-null values!
            #;(port2 #\u
            (s+ 
            (sql:update-old
            relname1

            (map cons
            (map (cute pg-fname r1 <>)
            interesting-changes)
                       
            (map cons
            (map
            (cute pg-get-value-string-null r2 i2 <>)
            interesting-changes)
            (map
            (lambda (i)
            (string-append "-- " (pg-get-value-string-null r1 i1 i)))
            interesting-changes)))
            where)
            ";\n\n"))

        
            ;; intersection  `different' with `delta-fields'
        
            ;; insert into delta
            (port1 #\i                  ;fixme! #\i  is just nonsense!
                   (s+
                    (sql:insert-alist
                     (ref delta-relation 'name)
                     ;; fixme!  insert the KEY too!

                     ;; todo: Intersect w/ ...
                     (append
                      ;; `(("dip" . ,(pg:text-printer (kahua-meta-ref "REMOTE_ADDR"))))
                      (filter-map
                          (lambda (i)
                            (cons (cdr i)
                                  (pg-get-value-string-null r2 i2 (car i))))
                        interesting-changes)

                      ;; If double? ... cannot be?
                      (map
                          cons
                        (map
                            (cute pg-fname r2 <>)
                          key-indices)
                        (map
                            (cute pg:text-printer <>)
                          key-values)))
                     ) ";\n")))
    
          ;; `update' 
          ;; update r2 with data from r1:
          (port1 #\u
                 (s+  
                  (sql:update
                   (ref relation 'name) ;fixme!
                   (map cons
                     changed-atts-names
                     (map
                         (cute pg-get-value-string-null r1 i1 <>)
                       changed-atts))
                   where)
                  ";\n")))))
    (values delta-updater-prepare delta-updater)))




(provide "pg/sync")
