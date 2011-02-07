#ifndef PG_LIB_H
#define PG_LIB_H

#include <libpq-fe.h>
#include <gauche/extend.h>	/*  why ?*/
#include <gauche/class.h>
#include <gauche/uvector.h>
#include <gauche.h>


#define CONST_CHAR_PTR(string)     ((const char*)Scm_GetStringConst(SCM_STRING(string)))
/* extern ScmObj Scm_pg_charPtr_Box(const char *string); */

#define CHECK_COLUMN(result, index, subr) \
{\
   if ((index >= PQnfields(result)) || (index < 0))       \
      Scm_Error("%s: %d,max = %d", subr, index,PQnfields(result)); \
}



/***  in-advance `classes' (as objects) */

/**** handle   */
SCM_CLASS_DECL(ScmPg_class);

typedef struct ScmPgRec {
    SCM_HEADER;
    ScmObj notice_monitor;
    PGconn*  conn;              /* NULL if closed */
    ScmObj converters;          /* alist type oid -> conversion   scheme procedure  or C ??*/
 } ScmPg;

#define SCM_CLASS_PG       (&ScmPg_class)
#define SCM_PG_HANDLE(obj)        (((ScmPg*) obj)->conn)
#define SCM_PG_HANDLE_P(obj)       Scm_TypeP(obj, SCM_CLASS_PG)


extern void pg_print(ScmObj obj, ScmPort *out, ScmWriteContext *ctx);

extern ScmObj new_pg_handle(PGconn* handle);




/**** result  */
SCM_CLASS_DECL(ScmPgR_class);

extern void pg_result_print(ScmObj obj, ScmPort *out, ScmWriteContext *ctx);

extern ScmObj new_pg_result (PGresult* r, ScmPg* handle);


/* extern ScmObj new_pg_result (PGresult* r); */



typedef struct ScmPgRRec
{
   SCM_HEADER;
   PGresult* result;
   ScmPg* handle;               /* the handle!! */
} ScmPgR;

#define SCM_CLASS_PG_RESULT       (&ScmPgR_class)
#define SCM_PG_RESULT(obj)        (((ScmPgR*) obj)->result)
#define SCM_PG_RESULT_P(obj)      Scm_TypeP(obj, SCM_CLASS_PG_RESULT)
/*unused*/
#define SCM_MAKE_PG_RESULT(result)


/* Notify: */

typedef struct ScmPGnotifyRec {
    SCM_HEADER;
    PGnotify *data;
} ScmPGnotify;

SCM_CLASS_DECL(Scm_PGnotify_Class);
#define SCM_CLASS_PGNOTIFY     (&Scm_PGnotify_Class)
#define SCM_PG_NOTIFY(obj)      (((ScmPGnotify*) obj)->data)
#define SCM_PG_NOTIFY_P(obj)    (Scm_TypeP(obj, SCM_CLASS_PGNOTIFY))
extern ScmObj Scm_Make_PGnotify(PGnotify *data);







extern char* mule_to_utf(const char* string);





#endif
