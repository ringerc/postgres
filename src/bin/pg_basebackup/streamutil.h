#include "libpq-fe.h"

extern const char *progname;
extern char *connection_string;
extern char *dbhost;
extern char *dbuser;
extern char *dbport;
extern char *dbname;
extern int	dbgetpassword;

/* Connection kept global so we can disconnect easily */
extern PGconn *conn;

#define disconnect_and_exit(code)				\
	{											\
	if (conn != NULL) PQfinish(conn);			\
	exit(code);									\
	}

extern PGconn *GetConnection(void);

extern int64 feGetCurrentTimestamp(void);
extern void feTimestampDifference(int64 start_time, int64 stop_time,
									 long *secs, int *microsecs);

extern bool feTimestampDifferenceExceeds(int64 start_time, int64 stop_time,
											int msec);
extern void fe_sendint64(int64 i, char *buf);
extern int64 fe_recvint64(char *buf);
