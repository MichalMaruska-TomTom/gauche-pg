#! /usr/bin/gosh

;; Do the COPY pg/sql command
;; push/pull data to/from PG db.

(define-module pg.mt-copy
  (export
   ;; copy protocol:
   ;; useful?
   ;;   start-copy last-line?  message-continuation?
   copy-import                          ; import from the DB
   copy-export
   ;;  			       pg-copy-export-internal
   ;; make-copy-to-coroutine make-copy-from-coroutine
   copy-from-to
   ;; todo???   '(define (copy-from-function function to relname))

   copy-tuples-to
   ;copy-from-port
   )

  (use pg)
  (use pg-hi)
  (use pg.util)

  (use gauche.uvector)
  (use util.queue)

  (use alg.binary-search)
  (use mmc.exit)
  (use adt.my-vector)

  (use gauche.threads)
  ;(use adt.mt-queue)
  (use mtqueue)

  (use adt.fixed-queue)
  (use mmc.log)
  (use mmc.xterm)

  (use srfi-2)
  (use srfi-13)                         ;substring/shared
  (use srfi-19)                         ; time
  (use adt.list)
  (use mmc.simple)
  (use mmc.threads)
  (use mmc.throw)
  (use macros.reverse)
  ;(use pg.copy-gui)
  )

(select-module pg.mt-copy)

;;; textual trace:
(define mtc-debug-modulo (expt 10 4))        ; how ofted debug (on STDERR)

;;;
(define mtc-unit-buffer 1000000)        ; the unit of allocation of space for Lines.


(define debug-list '())                 ;from to  from

(define debug #t)
(define (debug? what)
  #t ;(member what debug-list)
  )

(define-syntax for-debug                ; `comment' a sexp if not debugging
  (syntax-rules ()
    ((_ body ...)
     1
     ;(begin
     ;   body ...)
    )))


;;; I/F:
;; exceptions:     cannot-connect,   error,
;;
;; Entry points:    abort, get-next

;;; extended `protocol' on the mt-queue
;; (part "....")
;; last-line ....
;; 'end
;; 'error
;; 'abort


;;;  `COPY-protocol'
;;; All is around a big `uvector' (and a pointer)
(define (last-line? buffer start)       ;C version
  (pg-copy-last-line buffer start))
  ;; IS it the last line from COPY ?

(define (message-continuation? message)
  (and (pair? message)
       (eq? (car message) 'part)))


;;; `initialize' the COPY (sql) command. Throw an exception ...
(define (start-copy handle relname in? delimiter) ;limit ??
  (let* ((query (format "COPY \"~a\" ~a ~a ~a '~a';" relname
                        (if in? "TO" "FROM")
			(if in? "stdout" "stdin")
                        "WITH delimiter"
                        (or delimiter "\t") ;binary
                        ))
         (copy-result (pg-exec handle query)))
    ;; go away:
    (unless (eqv? (if in? PGRES_COPY_OUT PGRES_COPY_IN)
                  (pg-result-status copy-result))
      (logformat-color 'yellow
          "start-copy: ~a\t->~d (~a)/~a\n" query
          (pg-result-status copy-result)
          (pg-status-status (pg-result-status copy-result))
          (pg-result-error-message copy-result))
      (raise (list 'pg-copy-start-error ; fixme:  custom exception?
                   (pg-result-error-message copy-result)
                   (pg-result-status copy-result))))
    (logformat "~a: nfields: ~d, binary: ~a\n"
      (if in? "TO" "FROM")
      (pg-nfields copy-result)
      (pg-binary-tuples copy-result)
                                        ; (pg-fformat copy-result)
      )
    #t))


;;; `How' they work:
;; they don't address the mt-queue directly: instead, they call a function `producer'/`consumer'
;; which brings another piece (of the extended protocol)


;;; the `IMPORT'
;; consumer:   what to call when we get a new line, or   'end  'error ...
;; bump-tuples ... run when we finish a line.
(define (copy-import handle relname consumer bump-tuple delimiter) ; standard: (resumer & initial arg)
  (let ((debug-color 'red))
    ;; we need a function (& closur & fixme: accessor
    ;; to collect errors.  at some points (in the row cycle) we look for it, and react
    ;; so, "have any error rows occured?" becomes synchronous
    (let ((error-rows #f))

      ;; fixme: revert !!
      (pg-set-local-notice-processor
          (lambda (pg-handle message)
            ;; this is the global one:
            (format (current-error-port) "pg-notice-processor: ~d@~a: ~a\n"
                    (pg-host pg-handle)
                    (pg-backend-pid pg-handle)
                    message)
            (set error-rows #t))
        handle)

      ;; i need also an abort-handler
      ;;(logformat "make-copy-from-coroutine. backend-pid:~d\n" (pg-backend-pid handle))

      (with-exit-exception-handler* producer-exit
          ;; this is handler which is called from both the init handler, and proper data-copying handler.

          ;; if there is INTERAL error in the DB, we exit
          ;; but, before exiting, we abort the COPY, and signal the consumer?
          (lambda (exit-cc c)
            (logformat "exit-handler\n")
            (cond
             ((and (pair? c)
                   (eq? (car c) 'last))
              (logformat "\tlast line....passing upwards\n"))) ; fixme:  (consumer 'abort ...??)
            (exit-cc 1))

        ;; if the start fails,  ... ?
        (with-error-handler*
            (lambda (c)
              ;; fixme: pg errro message ?
              (if (eqv? (pg-backend-pid handle) 0)
                  (logformat-color 'yellow "the handle is NOT associated w/ any BE\n"))
              (logformat-color 'yellow "start failed")
              (exit #f))                ;producer-exit
          ;; error --->     TO STDOUT  ---> close other consumers ....
          (start-copy handle relname #t delimiter))


        ;;  error occurs in the middle .... tell the consumer.  We cannot continue, b/c PG doesn't provide any means.
        (with-chained-exception-handler*
            (lambda (c next)
              (logformat-color debug-color "with-chained-exception-handler: ~s\n"
                (if (exception? c)
                    (slot-ref c 'message)
                  c))
              (if (and (pair? c)
                       (eq? (car c) 'consumer)) ; message(error) from consumer!
                  (begin
                    ;(pg-put-copy-end handle #f) ; fixme: obsolete!!!  use `pg-put-copy-end'
                    (exit-exit producer-exit 1))
                (next c)))
          (pg-copy-import-internal handle consumer bump-tuple))))))


;;; entire rows
(define (pg-copy-import-internal handle consumer bump-tuple)
  (let ((debug-color 'red))
    ;; handle queue buffer len)
    ;; stuff needed for the data  transfer:
    (let* ((tuple 0)
           ;; `event-loop' evaluator:
           (do-process-result
            (lambda (result)
              (if (and (pair? result)
                       (eq? (car result) 'error))
                                        ;(exception? result)
                  (raise (list 'consumer result)))))
           ;;
           (found-last-line ;close COPY on the handle & tell the consumer:
            (lambda (error?)
              (logformat-color debug-color "sending 'end ~d\n" tuple)
              (consumer (if error?
                            'error
                          'end))
              ;(pg-end-copy handle)
              )))   ; fixme: sure??

      ;; fixme:
      (let* ((last-lines-kept 3) ; fixme: not lines, but parts: some lines are divided !!
             ;; we trace the last lines, so we can see `where' an error occured:
             (error-queue (make <error-queue>
                            :max-size last-lines-kept
                            :queue (list->queue (make-list last-lines-kept))))
             ;; fixme: should be `push-tuple-part'
             (push-tuple (lambda (tuple tuple-number completed)
                           ;; if we exceed the length, we start dropping the old tuples, therefore raise the
                           ;; tag
                           (push-item error-queue (list tuple tuple-number completed)))))

        ;; The `cycle'
        (let one-line ((message-number 0))
          ;(logformat "one-line starts!\n")
          ;; get the data:
          (let1 buffer (pg-get-copy-data-uvector handle #f)
           ; (logformat "~a: one-line: got line from pg\n" (thread-name (current-thread)))
            ;; `debugging'
            ;;(format #t "FROM:\tstatus: ~d ~d -> ~d\n" start end status)
            (if (zero? (modulo tuple mtc-debug-modulo))
                (logformat-color debug-color "producer: tuple #: ~a (~a)\n"
                  (number->currency tuple)
                  (number->currency message-number)))
            (cond
             ((eof-object? buffer)
              (logformat "producer: eof!\n")
              ;; fixme: we have to END the copy, to get possible errors (in the monitor) !! sort-of flush.
              '(if error-rows
                  (begin
                    (logformat-color debug-color "found error. Pushing end\n")
                    (logformat "last rows:\n")
                    ;; dump the error queue:
                    (for-each
                        (lambda (atom)
                          (logformat "~s: ~d ~a"
                            (list-ref 2) ;??
                            (list-ref 1) ; line no. ?
                            (list-ref 0))) ;
                      (get-items error-queue)))
                (logformat-color debug-color "found EOF\n"))
              (logformat "producer: eof 2!\n")
              ;; see, if a notice-monitor caught an exception from the Server
              (found-last-line #f)      ;fixme: error-rows
              ;; commit the queue if neccessary
              ;(exit-exit producer-exit 'end)
              ) ; exit !  really ?  why not simply exit the Let loop ?
             (else
              ;(logformat "indeed data buffer returned!\n")
              ;; Is it over ?
                                        ;(lower-bound-of-constants buffer end-marker start end u8vector-ref eqv?)
              (push-tuple buffer message-number #t)
              (consumer buffer)
              ;; put zeros into the buffer !!! or start after
              (inc! tuple)
              ;(logformat "PRODUCER: ~d!\n" tuple)
              (bump-tuple tuple)
              (one-line (+ 1 message-number))))))))))


;; producer -> handle  (producer x)  x = 'end 'error #f
;; run w/ 1 argument ... the vector.
;; signal-error

;; i have kept all the data in a closure, w/ all the exception handlers together.
;; Now i want to clean the code / make it separate
;; So, to be able to set! the variables, i must pass an object. (record/structure)
(define-class <pg-exporter-data> ()
  (
   ;(handle)
   (tuple)                              ;# of the current
   (errors                              ;list of errors
    :init-keyword :errors
    :init-value '()
    )
   ))


;; flush the copy buffer, so we get all the error messages (if any).
;; call SIGNAL-ERROR w/ the row numbers of unparable lines.
(define (copy-stop-and-report-error-lines c handle exporter-state error-queue signal-error) ;fixme:
  (let ((answer #f))
    (logformat "*** trying to terminate the COPY communication to get all pending error message from the server,
because we receive this exception: ~s\n" c)
    (pg-describe-error-on-connection handle) ;;(flush (current-error-port))

    ;; this seems needed to actually get the MONITOR run. And that is necessary to read the line number.
    (pg-put-copy-end handle #f)
    (logformat "... after pg-end-copy:")
    (pg-describe-error-on-connection handle)

    (let1 result (pg-get-result handle)
      (unless (eof-object? result)
        ;;(pg-describe-error-of-result result)
        (let1 error-message (pg-result-error-field result PG_DIAG_MESSAGE_PRIMARY)
          (rxmatch-if (rxmatch #/copy: line (\d*),/ error-message) (#f line-number)
            (signal-error (list (string->number line-number)))
            #f))))
    ;; send consumer  (error what)
    ;; (setq answer (producer 'error))
    ))


;; return:
;; Start the COPY_IN operation.  If this fails ->
;; Pump the data from PRODUCER to the handle
;; on error:  tell the producer, with SIGNAL-ERROR  and return, closing the COPY_IN operation.
;; we also keep the last 100 lines/pieces.
(define (copy-export handle relname producer signal-error delimiter) ; free-buffer
  (logformat "copy-export\n")
  ;; this one is to handle _ALL_ errors:
  ;(with-error-handler* error-handler:print-message ;; fixme!
  (with-transaction* handle
    ;; dangerous: we need to be inside transaction !
    ;(with-triggers-disabled* handle relname
      (logformat "copy-export: BEGIN;\n")
      (let ((exporter-state (make <pg-exporter-data>)))
        (pg-set-local-notice-processor (make-error-collector-monitor exporter-state) handle)
        ;; fixme: i should remove it, after!

        ;; fixme: throw -> tell producer !
        (start-copy handle relname #f delimiter) ;we have to end it.... so  unwind-protect?

        ;; keep the last rows ...
        (let* ((last-lines-kept 150)
               (error-queue (make <error-queue>
                              :max-size last-lines-kept
                              :queue (list->queue (make-list last-lines-kept))))

               ;; sort of a `hook'
               (push-tuple (lambda (tuple tuple-number completed)
                             ;; if we exceed the length, we start dropping the old tuples, therefore raise the
                             ;; tag
                             (push-item error-queue (list tuple tuple-number completed)))))

          ;; start pushing: But if an _error_ from the Pg side:  abort the copy & scan the error messages.
          ;; any other error -> abort: we _have_ to signal the end of COPY.
          ;; success
          (with-chained-exception-handler-safe*
           (lambda (c next)
             (if (is-a? c <error>) (error-handler:print-message c))
             (cond
              ((eq? c 'eof)
               (copy-stop-and-report-error-lines
                c handle exporter-state error-queue signal-error)
               (next c))
              (else
               (pg-put-copy-end handle #f)
                                        ;(logformat "it is NOT 'eof\n")
                                        ;(flush (current-error-port))
               (next c))))
           (pg-copy-export-internal handle producer push-tuple))))))


;; returns:  #t  ended ok
;; bug:          #f  ->  eof on the connection!
;; throws error:  'abort
;; pg error  -> 'errro
;; `pg-put-copy-end' .... who calls it?  just once !!
;; PUSH-TUPLE:  function to be called on every before pushing to HANDLE
(define (pg-copy-export-internal handle producer push-tuple)
  (let ((message-number 0)              ; in new versions this is also the tuple number.
        (debug-color 'blue)
        (tuple 0))
    (let process-message ((message (producer #f)))
      ;(logformat "consumer: message-number: ~d\n" message-number)
      (inc! message-number)             ;fixme!
      ;; tracing:
      (if (zero? (modulo tuple mtc-debug-modulo))
          (logformat-color debug-color "pg-copy-export-internal: tuple #: ~a (~a)\n"
            (number->currency tuple)
            (number->currency message-number)))

      ;; process the message from the producer:
      ;; this `cond' is followed by actual sending!!!! So if you don't want/have to send, raise an exception
      ;; fixme:  `case' better here?
      (cond
       ((memq message (list 'abort 'error))
        (if (eq? message 'error)
            ;; fixme: if we are in the middle of a line??
            ;; we could terminate the copy
            1)

        (logformat-color debug-color "got ~a ... instead of ~d\n" message tuple)
        (error message))

       ((eof-object? message) ;; fixme: EOF more appropriate
        (logformat-color debug-color "got EOF ... (at line ~d)\n" tuple)
        (pg-put-copy-end handle #f)
        #t)                             ;official successful return!

       (else
        ;; stuff to `send'
        ;; fixme: we could run out-of-memory !!!
        (unless (u8vector? message)     ;fixme:   pg-chunk?
          ;; `complete' <u8vector>
          (error 'unexpected))
        (let1 buffer message
          (push-tuple buffer tuple #t)
          (let1 status (pg-put-copy-data handle buffer 0 (u8vector-length buffer))
            (pg-copy-data-discard buffer)
                                        ;(logformat "status: ~d\n" status)
            (cond
             ((eqv? status 1)
              ;; we have processed an entire tuple
              (inc! tuple) ;;(logformat "tuple: ~d\n" tuple)
              (process-message (producer #f))) ;mmc: must be the tail of process-message!
             ((eof-object? status)
              (logformat "pg-put-copy-data failed!\n")
                                        ;(sys-abort)
              ;;  pg-error-message
              ;; fixme: should the code be here ??
              (raise 'eof))))))))))


;;; make a new thread...
(define (start-consumer-copy-to handle relname queue)
  (spawn* "pg-copy:consumer"
    (copy-export
     handle relname
     (lambda (ignore)
       (mtq-queue-pop! queue))

     ;; feedback:
     (lambda (object)
       (logformat-color 'green "mtq-queue-feedback!: ~a\n" object)
       (mtq-queue-feedback! queue object))

     pg-default-separator)
    (logformat "thread ~a exits!\n" (thread-name (current-thread)))))


;;; Very hi level:
;; given 2 handles and a relname, COPY the relname from/to
;; using threads
(define (copy-from-to from to relname start-hook end-hook)
  (if debug (logformat "copy-from-to: ~a\n" relname))
  (let* ((from-handle (handle-or-host->handle from))
         (to-handle (handle-or-host->handle to)))
    ;; But now, i change it.  i connect different coroutines, which  put/get to queue
    ;; and fire the 2 threads.
    (let1 mt-queue (make-mtqueue 20000)
      ;; I want to be able to exit here:
      ;; That means stopping the other thread ??
;;; [05 gen 05]  mmc: i cannot  trust this now:

;       (with-exit-exception-handler* exit-variable
;           (lambda (exit-cc c)
;             (logformat-color 'green
;                 "external exception handler: ~s ~s\n" (class-of c)
;                 (if (eq? (class-of c) <error>) ;<exception>
;                     (slot-ref c 'message)
;                   c))
;             (when (pair? c)
;               ;; fixme: i should post on the queue an 'abort command for both threads ...
;               ;; mmc: what is it?
;               (producer-abort 1)
;               (consumer-abort 1)
;               (unless (eq? from from-handle)
;                 (pg-finish from-handle))
;               (unless (eq? to to-handle)
;                 (pg-finish to-handle))
;               (exit-cc #t)))


        ;; 3rd thread shows a GUI ?
      (logformat "spawning the threads\n")

      (let ((producer-thread
             (spawn* "pg-copy:producer"
               (copy-import from-handle relname
                 ;; consumer:
                 (lambda (message)
                   ;; mmc:
                   ;(logformat "pushing to mtqueue!\n")
                   (mtq-queue-push! mt-queue message)
                   ;(logformat "pushed!\n")
                   )
                 ;; hook ?
                 (lambda (tuple)
                   ;(slot-set! mt-queue 'writer-count tuple)
                   1
                   )
                 pg-default-separator)))
            (consumer-thread (start-consumer-copy-to to-handle relname mt-queue)))

        (if start-hook (start-hook mt-queue))
                                        ;(logformat "joining threads\n")
        (thread-join! producer-thread)
        (logformat "joined the producer threads!\n")
                                        ;(logformat-color 'yellow "thread producer-thread finished\n")
        (thread-join! consumer-thread)
        ;; kill the helper-thread
        (if end-hook (end-hook))
        ;;(pg-finish helper-handle)
        ))))


;; returns the number of the first Error line. or #t
;; tuple-function returns the string/line !!!
(define (copy-tuples-to relname handle start-function tuple-function max-lines) ; separator
  ;; similar to awk!
  (let1 mt-queue (make-mtqueue 20000)
    (start-function mt-queue)
    (logformat "copy-tuples-to: ~a\n" relname)
    ;(logformat "spawning the 'exporting' COPY-ing thread\n")
    (let1 consumer-thread (start-consumer-copy-to handle relname mt-queue)
      ;; now pump:
      (catch 'stop                      ; (with-exit-exception-handler*
        (let pump-a-tuple ((line (tuple-function))
                           (line-number 0)
                           (wrong-lines 0))
          ;; i should keep a list of wrong lines. So, given the PG wrong line, i can see exactly how many before that one, if the process
          ;; is asynchro.
          (cond
           ((or (eof-object? line)
                (= line-number max-lines)) ;even this is not ok!
            ;; ... close
            (begin
              (mtq-queue-push! mt-queue eof-object) ;???
              (logformat "copy-tuples-to: producer (tuple-function) returned EOF. calling thread-join! on the consumer\n")
              (thread-join! consumer-thread) ;wait for the consumer
              #t))

          ;; fixme:  these calls increase line-number, but don't contribute. bug!
           ((eq? line #f) ;(not line) ;; #f
            ;; skip this one:
            ;;(logformat "copy-tuples-to: got #f\n")
            (pump-a-tuple (tuple-function) line-number (+ wrong-lines 1)))
           ((equal? line 'error)
            (logformat "copy-tuples-to: got error!\n")
            (pump-a-tuple (tuple-function) line-number (+ wrong-lines 1)))

           (else
            ;;(logformat "copy-tuples-to: pushing\n")
            (let1 result (mtq-queue-push! mt-queue line)
              (if result
                  (begin
                    '(and (pair? result)
                          (eq? (car result) 'error)
                          result)
                    (logformat "copy-tuples-from-port: error lines: ~s\n" result) ;(cdr result)
                    (logformat "killing the consumer thread (thread-join!)\n")
                    (with-f-handler
                     (thread-join! consumer-thread))
                    (pg-reset handle)
                    ;; this is a problem!!
                    (throw 'stop (+ wrong-lines (car result))))
                (pump-a-tuple (tuple-function) (+ line-number 1) wrong-lines))))))
        #t))))


(provide "pg/mt-copy")
