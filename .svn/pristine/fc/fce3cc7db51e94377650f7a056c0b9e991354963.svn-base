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

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <inttypes.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <syslog.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <assert.h> 
#include <string.h>

#include "state.h"
#include "main.h"
#include "matocuserv.h"
#include "cfg.h"
#include "chunks.h"
#include "filesystem.h"

int g_master_state;
char * g_virtual_ip;

inline int ismaster()
{
	return g_master_state == MFS_STATE_MASTER;
}

inline int isslave()
{
	return g_master_state == MFS_STATE_SLAVE;
}

inline int isshutdown()
{   
    return g_master_state == MFS_STATE_SHUTDOWN;
}

inline void set_state(int state)
{
	g_master_state = state;
}

static int checkvirtualip()
{
	int sock;
	struct ifconf conf;
	struct ifreq *ifr;
	char buff[BUFSIZ];
	int num;
	int i;
	int rc = 0;
	
	sock = socket(PF_INET, SOCK_DGRAM, 0);
	conf.ifc_len = BUFSIZ;
	conf.ifc_buf = buff;	

	ioctl(sock, SIOCGIFCONF, &conf);
	num = conf.ifc_len / sizeof(struct ifreq);
	ifr = conf.ifc_req;

	for (i=0; i<num; i++) {
		struct sockaddr_in *sin = (struct sockaddr_in *)(&ifr->ifr_addr);
		ioctl(sock, SIOCGIFFLAGS, ifr);
	    	if (( (ifr->ifr_flags & IFF_LOOPBACK) == 0 ) && (ifr->ifr_flags & IFF_UP) ) {
	        	if ( strncmp(inet_ntoa(sin->sin_addr), g_virtual_ip, strlen(g_virtual_ip) ) == 0 ) {
	            		//syslog(LOG_NOTICE, "Get Addr: %s (%s)", ifr->ifr_name, inet_ntoa(sin->sin_addr));
		             rc = 1;
				goto l_free_sock;
	             }			
				
	       }
        	 ifr++;
	}

	//syslog(LOG_NOTICE, "not get virtual ip\n");
	
l_free_sock:
	close(sock);
	
	return rc;
}

static void state_refresh()
{
	int havevip  = checkvirtualip();

	if( ismaster() ) {
		if(!havevip) {
			MFSLOG(LOG_ERR, "is master but have not the vip kill myself\n");
			assert(0);
		}

		/* for master, nothing to deal with */
		return;
	}

	/**
	  * for session is not replayed in slave, we should load it
	  * for now, changelog from master will be discarded
	  * 
	  * Dongyang Zhang
	  */
	if(havevip) {
		MFSLOG(LOG_NOTICE,  "slave got the vip, will switch to master");
             set_state(MFS_STATE_MASTER);
        	matocuserv_sessionsinit(NULL);

		main_timeregister(TIMEMODE_RUNONCE,1,0,chunk_jobs_main);
		main_timeregister(TIMEMODE_RUNONCE,1,0,fs_test_files);
		main_timeregister(TIMEMODE_RUNONCE,1,0,fsnodes_check_all_quotas);
		main_timeregister(TIMEMODE_RUNONCE,300,0,fs_emptytrash);
		main_timeregister(TIMEMODE_RUNONCE,60,0,fs_emptyreserved);
		main_timeregister(TIMEMODE_RUNONCE,60,0,fsnodes_freeinodes);	
             	main_timeregister(TIMEMODE_RUNONCE,3600,0,fs_dostoreall);
	}
}

int state_init()
{
	int whethermaster = cfg_getuint32("MASTER_STATE", 1);
	g_virtual_ip = cfg_getstr("VIRTUAL_IP", ""); 
	int havevip  = checkvirtualip();
	
	if( ( whethermaster && !havevip ) || ( !whethermaster && havevip ) )  { 
		MFSLOG(LOG_ERR, "state : %d  and havevip :%d  conflict vip:%s\n", 
			whethermaster, havevip, g_virtual_ip);
		return -1;
	}

    whethermaster ? set_state(MFS_STATE_MASTER) : set_state(MFS_STATE_SLAVE);

	main_timeregister(TIMEMODE_RUNALL, 2, 0,  state_refresh);

	return 0;
}
