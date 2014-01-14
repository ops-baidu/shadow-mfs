/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _MAIN_H_
#define _MAIN_H_

#include <sys/epoll.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <sys/types.h>
#include <unistd.h>

#include "version.h"
#include "cfg.h"

#define TIMEMODE_SKIP 0
#define TIMEMODE_RUNONCE 1
#define TIMEMODE_RUNALL 2

#define NOT_USED(x) ( (void)(x) )
void main_destructregister (void (*fun)(void));
void main_canexitregister (int (*fun)(void));
void main_wantexitregister (void (*fun)(void));
void main_reloadregister (void (*fun)(void));
void main_epollregister (void (*desc)(int),void (*serve)(int ,int ,struct epoll_event *));
void main_eachloopregister (void (*fun)(void));
void main_timeregister (int mode,uint32_t seconds,uint32_t offset,void (*fun)(void));
int main_time(void);
uint64_t main_utime(void);

enum {FREE,CONNECTING,HEADER,DATA,KILL};

//changelog transfer buffer
#define MaxLogCount 100
//changelog transfer info 
typedef struct file_info {
        uint64_t idx; //file offset
        FILE *fd; //FILE fd
} file_info;

//changelog transfer status
typedef struct trans_status {
        int resend;
        int should_rotate;
        uint64_t last_idx;
} trans_status;

typedef struct packetstruct {
        struct packetstruct *next;
        uint8_t *startptr;
        uint32_t bytesleft;
        uint8_t *packet;
} packetstruct;

typedef struct filelist {
        uint32_t inode;
        struct filelist *next;
} filelist;

typedef struct session {
        uint32_t sessionid;
        char *info;
        uint32_t peerip;
        uint8_t newsession;
        uint8_t sesflags;
        uint32_t rootuid;
        uint32_t rootgid;
        uint32_t mapalluid;
        uint32_t mapallgid;
        uint32_t rootinode;
        uint32_t disconnected;  // 0 = connected ; other = disconnection timestamp
        uint32_t nsocks;        // >0 - connected (number of active connections) ; 0 - not connected
        uint32_t currentopstats[16];
        uint32_t lasthouropstats[16];
        filelist *openedfiles;
        struct session *next;
} session;

typedef struct chunklist {
        uint64_t chunkid;
        uint64_t fleng;         // file length
        uint32_t qid;           // queryid for answer
        uint32_t inode;         // inode
        uint32_t uid;
        uint32_t gid;
        uint32_t auid;
        uint32_t agid;
        uint8_t type;
        struct chunklist *next;
} chunklist;

enum sync_step{
	MFS_SYNC_META 		     = 1,
	MFS_SYNC_CHANGELOG_START,
	MFS_SYNC_CHANGELOG_END,
};

typedef struct chunk_helem {
	void *chunk;
	struct chunk_helem *next;
} chunk_helem_t;

typedef struct chunk_hlist {
	uint32_t size;
	uint32_t mask;
	uint32_t num;
	chunk_helem_t *elem;
} chunk_hlist_t;

typedef struct serventry {
        uint8_t registered;
        uint8_t mode;                           //0 - not active, 1 - read header, 2 - read packet
        int sock;                               //socket number
        uint8_t listen_sock;                    //0 - not listen socket, 1 - listen socket
        uint8_t connection;                     //0 - to monut, 1 - to chunkserver, 2 - to metalogger, 3 - shadowmaster
        uint32_t version;
        uint32_t peerip;
        time_t lastread,lastwrite;              //time of last activity
        uint8_t hdrbuff[8];
        packetstruct inputpacket;
        packetstruct *outputhead,**outputtail;

        uint8_t passwordrnd[32];
        session *sesdata;
        chunklist *chunkdelayedops;

	file_info *cur_file;			// changelog information
	trans_status *trans;			// changelog transfer infomation 
	uint64_t changelog_offset;		// changelog.0 transfer offset
	char pack_buff[MaxLogCount][1000];	

        int metafd;

        char *servstrip;                // human readable version of servip
        uint32_t servip;                // ip to coonnect to
        uint16_t servport;              // port to connect to
        uint16_t timeout;               // communication timeout
        uint64_t usedspace;             // used hdd space in bytes
        uint64_t totalspace;            // total hdd space in bytes
        uint32_t chunkscount;
        uint64_t todelusedspace;
        uint64_t todeltotalspace;
        uint32_t todelchunkscount;
        uint32_t errorcounter;
        uint16_t rrepcounter;
        uint16_t wrepcounter;

        double carry;
	
        uint32_t bindip;
        uint32_t masterip;
        uint16_t masterport;
        uint8_t masteraddrvalid;
        uint8_t retrycnt;
        uint8_t downloading;
        FILE *logfd;    // using stdio because this is text file
        uint64_t filesize;
        uint64_t dloffset;
        uint64_t dlstartuts;
        struct serventry *next;
        uint32_t syncstep;  /* master eptr with shadow and slave */
        chunk_hlist_t chunkhlist; /* master eptr with chunk server */
} serventry;

typedef struct sync_entry {
        uint8_t mode;                           //0 - not active, 1 - read header, 2 - read packet
        int sock;                               //socket number
        uint8_t listen_sock;                    //0 - not listen socket, 1 - listen socket
        uint32_t version;
        uint32_t peerip;
        time_t lastread,lastwrite;              //time of last activity
        uint8_t hdrbuff[8];
        packetstruct inputpacket;
        packetstruct *outputhead,**outputtail;
        
        file_info *cur_file;           		// changelog information
        trans_status *trans;			// trans changelog status
        uint64_t changelog_offset;		// changelog.0 offset

        int metafd;

        char *servstrip;                // human readable version of servip
        uint32_t servip;                // ip to coonnect to
        uint16_t servport;              // port to connect to
        uint16_t timeout;               // communication timeout

        double carry;

        uint32_t bindip;
        uint32_t masterip;
        uint16_t masterport;

} sync_entry;

static inline int32_t get_current_time()
{
    struct timeval now;

    gettimeofday(&now,NULL);
    return now.tv_sec;
}

extern FILE *msgfd;
extern uint64_t logsize;
extern char * logfile;
#define LOG_BUFFER_SIZE 1024
#define LOG_FILENAME_LENGTH 1024
#define LOG_MAX_SIZE (1<<30) 
#define LOG_NUM 10
static inline void mfslog(int32_t level, char *filename, int32_t linenum, char *fmt, ...)
{
	char buf[LOG_BUFFER_SIZE];
	va_list argptr;
    time_t   now;
    struct   tm     *timenow; 
    time(&now);

    NOT_USED(level);

	memset(buf, 0., LOG_BUFFER_SIZE);	
	pthread_t mythid = pthread_self();
    timenow   =   localtime(&now);
	pid_t     pid    = getpid();

	snprintf(buf, LOG_BUFFER_SIZE, "%s", asctime(timenow));
    buf[strlen(buf) - 1] = 0; //clear the last '\n'
	snprintf(buf + strlen(buf), LOG_BUFFER_SIZE - strlen(buf), " mfs[%u][%lu]: %s:%d ", 
		pid, mythid, filename, linenum);
	
	va_start(argptr, fmt);
	vsnprintf(buf + strlen(buf),  LOG_BUFFER_SIZE - strlen(buf), fmt, argptr);
	va_end(argptr);

    if(buf[strlen(buf) - 1] != '\n') {
        snprintf(buf + strlen(buf), LOG_BUFFER_SIZE - strlen(buf), "\n");
    }        

	fprintf(msgfd, "%s", buf);
	fflush(msgfd);

    if(logfile != NULL && strcmp(logfile, "") != 0) {
        logsize += strlen(buf);
        if(logsize > LOG_MAX_SIZE) {
            char logname1[100],logname2[100];
            uint32_t i;

            fclose(msgfd);

            for(i = LOG_NUM; i>1; i--) {
                snprintf(logname1,100,"%s.%"PRIu32"",logfile,i);
                snprintf(logname2,100,"%s.%"PRIu32"",logfile,i-1);
                rename(logname2, logname1);
            }
            
            rename(logfile, logname2);
            msgfd = fopen(logfile,"a"); 
            logsize = 0;
        }
    }
}

#ifndef UNITTEST
#define MFSLOG(level, format, args...) mfslog(level, __FILE__, __LINE__, format, ##args)
#else
#define MFSLOG(level, format, args...) 
#endif

#endif
