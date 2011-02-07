
;; obsolete?


;; Given a hypergraph edges from set to a point, find a path, and produce a list
;; with numbered arguements....

;; production rules:
;;  (a b c) ------F--->  d
;;  (e d f) -----G---->  i
;;  (a b c e f) ----???-> i


(define-module pg.deduce
  (export
   <production-rule>
   find-deduction-path
   compile-deductions
   )
  (use mmc.log)
  (use srfi-1)
  (use pg.database)
  (use pg.links)                        ;Fixme: function-of might be moved to hypergraph or general deduce
  )
(select-module pg.deduce)


(define-class <production-rule> ()
  ((function :init-keyword :function)
   (arguments :init-keyword :arguments)
   (result :init-keyword :result)
   ))

(define-method write-object ((object <production-rule>) port)
  (format port
    "<prod-rule: ~a:-~a>"
    (ref object 'arguments)
    (ref object 'result)))


;; (define number-of (cute hash-table-get numbering-hash <>))



;;; Return a lambda, which given an element
;;; will return its `number' in a virtual set, where we distinguish by EQUALITY.
;;; numbers are counted 0 1 ....
;; fixme: A function to give the element under number??
(define (make-numbering-set equality)
  (let ((counter 0)
        (numbering-hash (make-hash-table equality))) ; points -> number
    (lambda (a)
      (or (hash-table-get numbering-hash a #f)
          (begin
            ;; (assert (not (hash-table-exists? numbering-hash a)))
            (hash-table-put! numbering-hash a counter)
            (begin0
             counter
             (set! counter (+ 1 counter))))))))


;; <hypergraph-rule> (set points info)
(define (find-deduction-path point available rules)
  (logformat "find-deduction-path:  buggy!\n")
  ;; available should be ordered?
  (let* (;; We need to quickly find rules which provide given target:
         (dep-hash (make-hash-table 'eq?))
         ;;
         (rule-for (lambda (a)
                     ;; Get the rule to get A, select the `first' one. -- for now they are in inverse order though!
                     (hash-table-get dep-hash a)))
         ;;
         (assign-number (make-numbering-set 'eq?)))

    (for-each assign-number (sort available pg-attribute<))

    ;; hash the rules, by the target!i.e.     x: =>  (a b c) -> x      
    (for-each
        (lambda (rule)
          ;; I could change the rules to numbers?
          (hash-table-put! dep-hash (car (ref rule 'provided)) rule)) ;fixme!
      rules)


    ;; todo: After I select the necessary rules, I could topo-sort, right?
    (let step ((need (list point))
               (have available)
               (steps '()))
      (cond
       ((null? need) ;;(subset? need available)
        steps)
       ;;
       (else
        (let* ((el (car need))
               ;; Get the rule to get the element
               (rule (rule-for el))     ;fixme: this might return several rules!
               (depends (ref rule 'set))
               ;; (info (ref rule 'info))
               ;;    This should be .... object? which gives (get-function info)
               ;;  the function might be `identity'
               )
          (assign-number el)
          ;; Bug:
          ;; What if the rule needs what we don't have, yet!

                                        ;(receive (diff intersection)
                                        ;    (lset-diff+intersection need available)
          (step
           (lset-union
            eq?                         ;fixme: ???
            (cdr need)
            ;; I should get intersetction+difference in 1 f. call!
            (lset-difference eq? depends have))
           (cons el have)
           (cons
            (make <production-rule>
              :function (function-of (ref rule 'info)) ;fixme! renumber the
              :arguments (map assign-number depends) ;fixme!
              :result (assign-number el))
            steps))))))))


;; rules are from leaves to top level!
;; so, when i renumber the result (to its argument), i shall have to renumber in the rest, and not previous!
(define (compile-deductions rules initial)
  (let* ((renumber-vec (make-vector (+ (length rules) initial) #f)) ;+ the initial set!
         (number-of (lambda (i)
                      (or (vector-ref renumber-vec i)
                          i))))
    (let1 program
        (fold                           ;in order!
         (lambda (rule program)
           (if (eq? identity (ref rule 'function))
               ;; 2 <pg-attribute>s are the same:
               (begin
                 (vector-set! renumber-vec
                              (ref rule 'result) ; note: any result is not used in previous rules, hence not renumbered!
                              (number-of (car (ref rule 'arguments))))
                 program)

             ;; otherwise just renumber!
             (begin
               (slot-set! rule 'arguments
                 (map number-of (slot-ref rule 'arguments)))
               (cons rule program))))
         ()
         rules)

      (if (null? program)
          (begin
            (let1 final (last rules)
              (unless
                  (= initial
                     (ref final 'result))
                (logformat "compile-deductions was given wrong number of initials (~d), or the final rule is fake (~d)?\n"
                  initial
                  (ref final 'result)))
              (vector-ref renumber-vec initial)))
        program)
      ;; the result should be:
      ;     (vector-ref renumber-vec (+ initial 1))
      ;; mmc: Is the renumber-vec useful _only_ when program is () ?
      ;(values program renumber-vec)
      )))


(provide "pg/deduce")
