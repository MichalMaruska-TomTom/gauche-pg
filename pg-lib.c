/* #define GAUCHE_API_0_8_10 1 */
#include "pg-lib.h"

#define DEBUG_finalizer 0
#define GLOBAL_NOTICE_PROCESSOR 0
#define hook_name "pg-handle-hook"
#define use_hook 1


/* print about the <pg-handle>. Could be done in scheme, if i had PQ-XX funcstions?
 * mmc: Maybe not. I want info on NULL connection too. But working w/ such complicates all? */
void
pg_print(ScmObj obj, ScmPort *out, ScmWriteContext *ctx)
{
   PGconn* P=SCM_PG_HANDLE(obj);
   if (P)
      {
         Scm_Printf(out, "#<pg-handle %s@%s as %s>",
                    PQdb(P),
                    PQhost(P),
                    PQuser(P)
            )			; /* fixme: no name */
      } else Scm_Printf(out, "#<pg-handle null>") ;
}



static
void pg_finalize(ScmObj obj, void* data)
{
/*  no need to test. the finalizer has just been deduced from the structure itself ? */
   PGconn *g = SCM_PG_HANDLE(obj);
#if 0
   printf("pg_finalize\\n");
#endif
   if (g) {
           PQfinish(g);
           /* fixme:*/
           g = NULL;
   } else Scm_Panic("%s unexpected\n", __FUNCTION__);
}


/***** notice processor.  Asynchronous. Unfortunately. */
#if 0
static
#endif
void
NoticeProcessor(void *arg, const char *message)
{
   ScmPg* g;
   PGconn *P;
   Scm_Warn ("**** %s: %s", __FUNCTION__, message);
   //P= (PGconn*) arg->handle;
   g = arg;
   P = SCM_PG_HANDLE(g);
   if (g->notice_monitor!=SCM_FALSE)
      Scm_ApplyRec(g->notice_monitor,
                   SCM_LIST2((ScmObj) g ,SCM_MAKE_STR_COPYING(message)));
   else
      {
#if GLOBAL_NOTICE_PROCESSOR
         Scm_ApplyRec(pg_notice_processor,
                      SCM_LIST2((ScmObj*)g ,SCM_MAKE_STR_COPYING(message)));
#endif
      }
};


static
void pg_finalize_notify(ScmObj obj, void* data)
{
/*  no need to test. the finalizer has just been deduced from the structure itself */
   PGnotify *g = SCM_PG_NOTIFY(obj);

   if (g) {
      PQfreemem(g);
      SCM_PG_NOTIFY(obj) = NULL;
   } else Scm_Panic("%s unexpected\n", __FUNCTION__);
}

/* should be void*  ?? */
ScmObj
Scm_Make_PGnotify(PGnotify* notify) /* handle */
{
   ScmPGnotify* g = SCM_NEW(ScmPGnotify);
   SCM_SET_CLASS(g, SCM_CLASS_PGNOTIFY);
   g->data=notify;

   /* PQfreemem */
   Scm_RegisterFinalizer(SCM_OBJ(g), pg_finalize_notify, NULL);
   return SCM_OBJ(g);
}



/* should be void*  ?? */
ScmObj
new_pg_handle(PGconn* handle)
{
   ScmPg* g = SCM_NEW(ScmPg);
   SCM_SET_CLASS(g, SCM_CLASS_PG);
   g->conn=handle;
   g->converters=SCM_FALSE;
   g->notice_monitor=SCM_FALSE;
   PQsetNoticeProcessor(handle, NoticeProcessor ,(void*) g);	/* fixme: some data ?conninfo */


#if use_hook
   /* i don't need it anymore */
   /* i would like to run a hook */

   ScmModule *module = SCM_MODULE(SCM_FIND_MODULE("pg", TRUE)); /* SCM_OBJ(SCM_CURRENT_MODULE()) */
   ScmSymbol *symbol = SCM_SYMBOL(SCM_INTERN(hook_name)); /* Scm_Intern((ScmString*) SCM_MAKE_STR("pg-handle-hook")) */

   ScmObj hook = Scm_SymbolValue(module, symbol);

   if ( hook && (SCM_LISTP(hook)) && !(SCM_NULLP(hook)))
      {
         /* Scm_Warn("%s: running the hook\n", __FUNCTION__); */
         ScmObj p;
         SCM_FOR_EACH(p, hook)
            {
               ScmObj f = SCM_CAR(p);
               if (SCM_PROCEDUREP(f)){
                       /* mmc: fixme! for new API, to catch errors! */
                  Scm_ApplyRec(f, SCM_LIST1(SCM_OBJ(g)));
               } else {
                  Scm_Error("%s: %s: not a procedure!! %S\n", __FUNCTION__, hook_name, f);
               };
            };

      } else {
         /* Scm_Warn("%s: not found any hook\n", __FUNCTION__); */
      };
#endif
   /* Scm_Eval(ScmObj form, ScmObj env); */
   Scm_RegisterFinalizer(SCM_OBJ(g), pg_finalize, NULL);
   return SCM_OBJ(g);
}



/**** result  */

void pg_result_print(ScmObj obj, ScmPort *out, ScmWriteContext *ctx)
{
   PGresult* R=SCM_PG_RESULT(obj);
   if (R) {
      Scm_Printf(out, "#<pg-result %s/%s>",
                 PQresultErrorMessage(R),
                 PQcmdStatus(R)
                 // PQresStatus(PQresultStatus(R))
         );
   } else Scm_Printf(out, "#<pg-result null>");
}

static void
pg_result_finalize(ScmObj obj, void* data)
{
/*  no need to test. the finalizer has just been deduced from the structure itself ? */
        PGresult* g = SCM_PG_RESULT(obj);
#if DEBUG_finalizer
        printf("pg_result finalize\n");
#endif
        if (g) {
                /* fixme:  if we already called pg-clear, this is segfault! */
                PQclear(g);
                g = NULL;
        } else Scm_Panic("%s unexpected\n", __FUNCTION__);
}


/* This is called in 2*/
ScmObj
new_pg_result (PGresult* r, ScmPg* handle)
{
   ScmPgR* g = SCM_NEW(ScmPgR);
   SCM_SET_CLASS(g, SCM_CLASS_PG_RESULT);
   g->result=r;
   g->handle = handle;          /* fixme: i would have to reference ...   w/ boehm it's ok! */
   Scm_RegisterFinalizer(SCM_OBJ(g), pg_result_finalize, NULL);
   return SCM_OBJ(g);
}



/* move elsewhere! */
int
binary_search(char* vector, int start, int end, char element)
{
   int middle;
/*   printf ("start: %d %d\\n", start, end); */
/* start should be below, end over the boundary */
   while ((end-start) > 1){
/*   printf ("middle: %d\\n", middle); */
      middle = (start+end)/2;
      if (vector[middle]!= element)
         start=middle;
      else
         end=middle;
   };
/*   printf ("found %d\\n", (vector[start]==element)?start:end); */
   return (vector[start]==element)?start:end;
}


/* Hack for initialization stub */
void internal_init_pg(ScmModule*);

void Scm_Init_pg(void)
{
   ScmModule *mod;
   SCM_INIT_EXTENSION(pg);
   mod = SCM_MODULE(SCM_FIND_MODULE("pg", TRUE));
   internal_init_pg(mod);
}

