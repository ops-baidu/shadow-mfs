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

#include "config.h"

#include <stdio.h>
#include <stdarg.h>
#include <syslog.h>
#include <unistd.h>
#include <pthread.h>

#include "main.h"
#include "changelog.h"
#include "matomlserv.h"
#include "cfg.h"
#include "state.h"

#define MAXLOGLINESIZE 10000
static uint32_t BackLogsNumber;
static FILE *fd;

//changelog func should not send anything
void changelog_rotate() {
	char logname1[100],logname2[100];
	uint32_t i;
	FILE *new_fd;

	/* slave noneed to changelog */
	if(isslave()) {
		return;
	}
	
	if (fd) {
		fclose(fd);
		fd=NULL;
	}
	if (BackLogsNumber>0) {
		for (i=BackLogsNumber ; i>0 ; i--) {
			snprintf(logname1,100,"changelog.%"PRIu32".mfs",i);
			snprintf(logname2,100,"changelog.%"PRIu32".mfs",i-1);
			rename(logname2,logname1);
		}
		new_fd = fopen("changelog.0.mfs","a+");
		if (new_fd == NULL) {
			MFSLOG(LOG_ERR,"changelog rotate,create new changelog.0.mfs failed");
		} else {
			fclose(new_fd);
		}
	} else {
		unlink("changelog.0.mfs");
		new_fd = fopen("changelog.0.mfs","a+");
                if (new_fd == NULL) {
                        MFSLOG(LOG_ERR,"changelog rotate,create new changelog.0.mfs failed");
                } else {
                        fclose(new_fd);
                }
	}
//	matomlserv_broadcast_logrotate();
}

extern uint64_t version;
void changelog(uint64_t in_version,const char *format,...) {
	static char printbuff[MAXLOGLINESIZE];
	va_list ap;
	uint32_t leng;

	/* as the caller will the global version if we need not to increase actually */
	if(isslave()) {
		version--;
		return;
	}

	va_start(ap,format);
	leng = vsnprintf(printbuff,MAXLOGLINESIZE,format,ap);
	va_end(ap);
	if (leng>=MAXLOGLINESIZE) {
		printbuff[MAXLOGLINESIZE-1]='\0';
		leng=MAXLOGLINESIZE;
	} else {
		leng++;
	}

	if (fd==NULL) {
		fd = fopen("changelog.0.mfs","a");
		if (!fd) {
			MFSLOG(LOG_NOTICE,"lost MFS change %"PRIu64": %s",in_version,printbuff);
		}
	}

	if (fd) {
		fprintf(fd,"%"PRIu64": %s\n",in_version,printbuff);
		fflush(fd);
	}
	//matomlserv_broadcast_logstring(version,(uint8_t*)printbuff,leng);
}

int changelog_init() {
	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	fd = NULL;
	//changelog_lock_init();
	return 0;
}

