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

#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <assert.h>

#include "MFSCommunication.h"

#include "datapack.h"
#include "matomlserv.h"
#include "crc.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "state.h"
#include "changelog.h"

#define MaxLogCount 100
#define MaxConnect 30
#define MaxPacketSize 1500000
#define GETU64(data,clptr) (data)=strtoull(clptr,&clptr,10)
#define GETU32(data,clptr) (data)=strtoul(clptr,&clptr,10)
#define EAT(clptr,vno,c) { \
        if (*(clptr)!=(c)) { \
                printf("%"PRIu64": '%c' expected\n",(vno),(c)); \
                return 1; \
        } \
        (clptr)++; \
}

#define NOT_USED(x) ( (void)(x) )
//#define NOT_USED(x) ( *(volatile typeof(x) *)&(x) = (x); )

static serventry *matomlservhead = NULL;
static int lsock;
//static int32_t lsockpdescpos;
static int first_add_listen_sock;
static int reconnect_count = 0; 
static int MaxReconnect;
static char *BindHost;
// from config
static char *ListenHost;
static char *ListenPort;
//static char *Bind;

//save thread's connection address
typedef struct thread_addr {
	char *host;
	char *port;
} thread_addr;

thread_addr worker_addr[4];

typedef struct recog_addr {
	uint32_t ip;
	uint16_t port;
} recog_addr;

recog_addr config_addr[4];
//worker thread's it
static pthread_t worker_id[4];

//changelog open function
static int open_changelog(file_info *new_file) {
	char *datapath = NULL;
	char *logpath = NULL;
	int dplen;
	int rc = 0;

        datapath = strdup(DATA_PATH);

	 if(NULL == datapath) {
	 	MFSLOG(LOG_WARNING, "alloc mem failed\n");
		return -1;
	 }
		
        dplen = strlen(datapath);
        logpath = malloc(dplen+sizeof("/changelog.0.mfs"));
        memcpy(logpath,datapath,dplen);
        memcpy(logpath+dplen,"/changelog.0.mfs",sizeof("/changelog.0.mfs"));

	if(NULL == logpath)  {
		MFSLOG(LOG_WARNING, "alloc mem failed");
		free(datapath);
		return -1;
	}

        if ((new_file->fd = fopen(logpath,"r")) == NULL) {
		new_file->idx = 0;
		MFSLOG(LOG_NOTICE,"open changelog file failed errno:%d\n", errno);
		rc = -1;
	} else {
		new_file->idx = 0;
	}

	free(datapath);
	free(logpath);
	
	return rc;
}

//changelog read function
static int read_changelog(file_info *cur_file,char *buff) {
	uint64_t cur_idx = 0;

	if (cur_file != NULL) {
        	cur_idx = ftell(cur_file->fd);
        	cur_file->idx = cur_idx;
		if (fseek(cur_file->fd,cur_idx,SEEK_SET) != 0) {
			MFSLOG(LOG_ERR,"3 seek_set failed cur_file:%p fd:%p cur_idx:%lu errno:%d\n",
				cur_file, cur_file->fd, cur_idx, errno);
			return -1;
		}	
		//cur_idx = ftell(cur_file->fd);
		//cur_file->idx = cur_idx;
//		syslog(LOG_NOTICE,"the cur_idx is %d",cur_idx);
	}
	if (buff == NULL) {
		MFSLOG(LOG_NOTICE,"buff is NULL");
		return -1;
	}

	if (fgets(buff,1000,cur_file->fd) != NULL) {
		if (buff[strlen(buff)-1] != '\n') {
			if (fseek(cur_file->fd,cur_idx,SEEK_SET) != 0) {
				MFSLOG(LOG_ERR,"seek_set failed");
				return -1;
			} 
//			syslog(LOG_NOTICE,"no more line 1");
			return 0;
		} else {
			return strlen(buff);
		}
	} else {
		if (fseek(cur_file->fd,cur_idx,SEEK_SET) != 0) {
               		MFSLOG(LOG_ERR,"seek_set failed");
               		return -1;
		} else {
//			syslog(LOG_NOTICE,"no more line 2");
			return 0;
		}
	}
}

uint32_t matomlserv_mloglist_size(void) {
        serventry *eptr;
        uint32_t i;
        i=0;
        for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
                if (eptr->mode!=KILL && eptr->listen_sock==0) {
                        i++;
                }
        }
        return i*(4+4);
}

void matomlserv_mloglist_data(uint8_t *ptr) {
        serventry *eptr;
        for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
                if (eptr->mode!=KILL && eptr->listen_sock==0) {
                        put32bit(&ptr,eptr->version);
                        put32bit(&ptr,eptr->servip);
                }
        }
}

uint8_t* matomlserv_createpacket(serventry *eptr,uint32_t type,uint32_t size) {
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket=(packetstruct*)malloc(sizeof(packetstruct));
	if (outpacket==NULL) {
		return NULL;
	}
	psize = size+8;
	outpacket->packet=malloc(psize);
	outpacket->bytesleft = psize;
	if (outpacket->packet==NULL) {
		free(outpacket);
		return NULL;
	}
	ptr = outpacket->packet;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = (uint8_t*)(outpacket->packet);
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

//indentify worker thread
static int worker_thread_indent(pthread_t tid) {
	int num = 0;
	
	if (tid == worker_id[0]) {
		num = 0;
	} else if (tid == worker_id[1]) {
		num = 1;
	} else if (tid == worker_id[2]) {
 		num = 2;
	} else if (tid == worker_id[3]) {
		num = 3;
	} else {
		MFSLOG(LOG_ERR,"worker thread indent failed num is %d,tid is %ld",num,tid);
        assert(0);
	}
	return num;
}

//sync thread protocol
void matoml_sync_thread(serventry *eptr,uint8_t flag) {
	uint8_t *ptr;
	
//	syslog(LOG_NOTICE,"the matoml_sync_thread flag is %d",flag);	
	ptr = matomlserv_createpacket(eptr,MATOML_SYNC_THREAD,1);
	if (ptr != NULL) {
		if ((flag == 1) || (flag == 2) || (flag == 3)){
			put8bit(&ptr,flag);
		} else {
			MFSLOG(LOG_ERR,"unrecogize flag");
		}
	} else {
		eptr->mode = KILL;
		MFSLOG(LOG_ERR,"can't create packet");
	}
}

//init the thread's socket
static int sync_socket_init(serventry *eptr,pthread_t tid) {
	int status;
	uint32_t shadow_ip = 0,bind_ip = 0,bip = 0;
	uint16_t shadow_port;
	int num;
	int count = 0;

	num = worker_thread_indent(tid);
	if (tcpresolve(worker_addr[num].host,worker_addr[num].port,&shadow_ip,&shadow_port,0)>=0) {
		eptr->masterip = shadow_ip;
        	eptr->masterport = shadow_port;
	} else {
		MFSLOG(LOG_ERR,"resolve shadow's addr failed");
		return -1;
	}
	eptr->sock = tcpsocket();
	if (eptr->sock < 0) {
		MFSLOG(LOG_ERR,"socket init failed");
		return -1;
	}
	if (tcpnonblock(eptr->sock)<0) {
		MFSLOG(LOG_WARNING,"set nonblock, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		return -1;
        }
    	 tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
    	 eptr->servstrip = matomlserv_makestrip(eptr->servip);	
	 if (tcpresolve(BindHost,NULL,&bind_ip,NULL,1)>=0) {
        	eptr->bindip = bip;
        } else {
                eptr->bindip = 0;
        }	
        if (eptr->bindip>0) {
                if (tcpnumbind(eptr->sock,eptr->bindip,0)<0) {
                        MFSLOG(LOG_WARNING,"can't bind socket to given ip: %m");
                        tcpclose(eptr->sock);
                        eptr->sock=-1;
                        return -1;
                }
        }
	eptr->cur_file = (file_info *)malloc(sizeof(file_info));
	if (eptr->cur_file == NULL) {
		MFSLOG(LOG_ERR,"FATAL,malloc file_info failed");
	} else {
		eptr->cur_file->fd = NULL;
		eptr->cur_file->idx = 0;
	}
	while(count < MaxConnect) {
		status = tcpnumconnect(eptr->sock,eptr->masterip,eptr->masterport);
	        if (status<0) {
	                MFSLOG(LOG_WARNING,"matoml sync connect failed, error: %m (1)");
	                tcpclose(eptr->sock);
	                eptr->sock=-1;
	                return -1;
	        }
		if (status==0) {
			MFSLOG(LOG_NOTICE,"connected to shadow master");
		        tcpnodelay(eptr->sock);
		        eptr->mode=HEADER;
		        eptr->inputpacket.next = NULL;
	      		eptr->inputpacket.bytesleft = 8;
	        	eptr->inputpacket.startptr = eptr->hdrbuff;
	        	eptr->inputpacket.packet = NULL;
	        	eptr->outputhead = NULL;
	        	eptr->outputtail = &(eptr->outputhead);
	
			eptr->lastread = eptr->lastwrite = get_current_time();
			count = 100;
			break;
		} else {
			MFSLOG(LOG_ERR,"connect failed, error: %m (2)");
		}
		count++;
		sleep(1);
	}
	
	return 0;
}

//worker thread read fuc
void matomlserv_worker_read(serventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;
//	syslog(LOG_NOTICE,"read func");
	for (;;) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
//		syslog(LOG_NOTICE,"the i is %d",i);
		if (i==0) {
			MFSLOG(LOG_INFO,"connection with ML(%u) lost",eptr->masterip);
			eptr->mode = KILL;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				MFSLOG(LOG_INFO,"read from ML(%u) error: %m",eptr->masterip);
				eptr->mode = KILL;
			}
			return;
		}
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;

		if (eptr->inputpacket.bytesleft>0) {
			return;
		}

		if (eptr->mode==HEADER) {
			ptr = eptr->hdrbuff+4;
			size = get32bit(&ptr);
//			syslog(LOG_NOTICE,"the size is %d",size);

			if (size>0) {
				if (size>MaxPacketSize) {
					MFSLOG(LOG_WARNING,"ML(%u) packet too long (%"PRIu32"/%u)",eptr->masterip,size,MaxPacketSize);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.packet = malloc(size);
				if (eptr->inputpacket.packet==NULL) {
					MFSLOG(LOG_WARNING,"ML(%u) packet: out of memory",eptr->masterip);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.bytesleft = size;
				eptr->inputpacket.startptr = eptr->inputpacket.packet;
				eptr->mode = DATA;
				continue;
			}
			eptr->mode = DATA;
		}

		if (eptr->mode==DATA) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode=HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			matomlserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}
	}
}

//worker thread write func
void matomlserv_worker_write(serventry *eptr) {
	packetstruct *pack;
	int32_t i;
	
//	syslog(LOG_NOTICE,"write func");
	for (;;) {
		pack = eptr->outputhead;
		if (pack==NULL) {
			return;
		}
		i=write(eptr->sock,pack->startptr,pack->bytesleft);
		if (i<0) {
			if (errno!=EAGAIN) {
				MFSLOG(LOG_INFO,"write to ML(%u) error: %m",eptr->masterip);
				eptr->mode = KILL;
			}
			return;
		}
		pack->startptr+=i;
		pack->bytesleft-=i;
		if (pack->bytesleft>0) {
			return;
		}
		free(pack->packet);
		eptr->outputhead = pack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(pack);
	}
}

//rebuild logrotate func
void matomlserv_logrotate(serventry *eptr) {
	uint8_t *data;

	data = matomlserv_createpacket(eptr,MATOML_CHANGELOG_ROTATE,1);
	if (data!=NULL) {
		put8bit(&data,0x55);
	} else {
		eptr->mode = KILL;
	}
}

//init the trans status
static void init_trans(trans_status * sync_trans) {
	sync_trans->resend = 0;
	sync_trans->should_rotate = 0;
	sync_trans->last_idx = 0;	
}	

//sync thread
static void *meta_sync_worker(void *argc) {
		//uint32_t now = main_time();
        NOT_USED(argc);

		uint64_t reconnect_offset = 0;
		serventry *sync_serventry;
		struct pollfd sync_pfd;
		pthread_t self_id = pthread_self();
		uint32_t lastsyncstep = MFS_SYNC_META;

		//	syslog(LOG_NOTICE,"thread");
		sync_serventry = (serventry *)malloc(sizeof(serventry));
		sync_serventry->trans = (trans_status *)malloc(sizeof(trans_status));
		sync_serventry->changelog_offset = reconnect_offset;
		init_trans(sync_serventry->trans);
		if(sync_socket_init(sync_serventry,self_id) < 0 ) {
				MFSLOG(LOG_ERR,"socket init failed");
				pthread_exit(0);
		}
		sync_serventry->syncstep = MFS_SYNC_META;
		matoml_sync_thread(sync_serventry,MFS_SYNC_META);
		sync_pfd.fd = sync_serventry->sock;
		while (reconnect_count < MaxReconnect) {
				if (sync_serventry == NULL) {
						sync_serventry = (serventry *)malloc(sizeof(serventry));
						sync_serventry->trans = (trans_status *)malloc(sizeof(trans_status));
						sync_serventry->changelog_offset = reconnect_offset;
						sync_serventry->syncstep  = lastsyncstep;
						init_trans(sync_serventry->trans);
						if(sync_socket_init(sync_serventry,self_id) < 0 ) {
								MFSLOG(LOG_ERR,"socket init failed");
								pthread_exit(0);
						}
						matoml_sync_thread(sync_serventry,sync_serventry->syncstep);
						sync_pfd.fd = sync_serventry->sock;
				}
				while(sync_serventry->mode != KILL) {
						sync_pfd.events = POLLIN;
						if (sync_serventry->outputhead != NULL) {
								sync_pfd.events = POLLIN | POLLOUT;
						}
						if (poll(&sync_pfd,1,100)<0) {
								MFSLOG(LOG_ERR,"poll error");
						}
						if (sync_pfd.revents & (POLLERR|POLLHUP)) {
								//				syslog(LOG_NOTICE,"POLLERR and POLLHUP");
								sync_serventry->mode = KILL;
						}
						if ((sync_pfd.revents & POLLIN) && sync_serventry->mode != KILL) {
								//				syslog(LOG_NOTICE,"log count is %d",count++);
								matomlserv_worker_read(sync_serventry);
								sync_serventry->lastread = get_current_time();							
						}
						if ((sync_pfd.revents & POLLOUT) && sync_serventry->mode != KILL) {
								matomlserv_worker_write(sync_serventry);
								sync_serventry->lastwrite = get_current_time();			
						}
						if ((uint32_t)(sync_serventry->lastwrite+(sync_serventry->timeout/2))<(uint32_t)get_current_time()
								&& 	sync_serventry->outputhead==NULL) {
								matomlserv_createpacket(sync_serventry,ANTOAN_NOP,0);
						}
						//			syslog(LOG_NOTICE,"the resend is %d",resend);
						if (sync_serventry->trans->resend == 1) {
								if(matomlserv_pack_log(sync_serventry,0)<0) {
										MFSLOG(LOG_NOTICE,"resend get log failed");
								}
						}	
				}
				if (sync_serventry->mode == KILL) {
						if(sync_pfd.fd > 0) {
							close(sync_pfd.fd);
						}
                       		       if(sync_serventry->cur_file->fd) {
						    fclose(sync_serventry->cur_file->fd);
                        			}
						reconnect_offset = sync_serventry->cur_file->idx;
						free(sync_serventry->cur_file);
						sync_serventry->cur_file = NULL;
						free(sync_serventry->trans);
						sync_serventry->trans = NULL;
						lastsyncstep = sync_serventry->syncstep;
						free(sync_serventry);						
						sync_serventry = NULL;
						reconnect_count++;
				}						
		}
		pthread_exit(0);
}

//address resolve func
static int worker_addr_resolve(void) {
	uint32_t config_ip;
	uint16_t config_port;

	if (tcpresolve(worker_addr[0].host,worker_addr[0].port,&config_ip,&config_port,0) < 0) {
		MFSLOG(LOG_NOTICE,"host is %s,port is %s.resolve thread 1 failed",worker_addr[0].host,worker_addr[0].port);
		return -1;
	} else {
		config_addr[0].ip = config_ip;
		config_addr[0].port = config_port;
	}
        if (tcpresolve(worker_addr[1].host,worker_addr[1].port,&config_ip,&config_port,0) < 0) {
                MFSLOG(LOG_NOTICE,"resolve thread 2 failed");
                return -1;
        } else {
		config_addr[1].ip = config_ip;
		config_addr[1].port = config_port;
	}
        if (tcpresolve(worker_addr[2].host,worker_addr[2].port,&config_ip,&config_port,0) < 0) {
                MFSLOG(LOG_NOTICE,"resolve thread 3 failed");
                return -1;
        } else {
		config_addr[2].ip = config_ip;
		config_addr[2].port = config_port;
	}
        if (tcpresolve(worker_addr[3].host,worker_addr[3].port,&config_ip,&config_port,0) < 0) {
                MFSLOG(LOG_NOTICE,"resolve thread 4 failed");
                return -1;
        } else {
		config_addr[3].ip = config_ip;
		config_addr[3].port = config_port;
	}
	return 0;
}

//worker thread init func
static int worker_thread_init(uint32_t shadow_ip){
	uint32_t status;	

//	syslog(LOG_NOTICE,"shadow_ip is %d,config0_ip is %d,config1_ip is %d,config2_ip is %d,config3_ip is %d",shadow_ip,config_addr[0].ip,config_addr[1].ip,config_addr[2].ip,config_addr[3].ip);
	if (shadow_ip == config_addr[0].ip) {
		pthread_create(&worker_id[0],NULL,meta_sync_worker,NULL);
		status = 0;
	} else if (shadow_ip == config_addr[1].ip) {
		pthread_create(&worker_id[1],NULL,meta_sync_worker,NULL);
		status = 0;
	} else if (shadow_ip == config_addr[2].ip) {	
		pthread_create(&worker_id[2],NULL,meta_sync_worker,NULL);
		status = 0;
	} else if (shadow_ip == config_addr[3].ip) {
		pthread_create(&worker_id[3],NULL,meta_sync_worker,NULL);
		status = 0;
	} else {
		MFSLOG(LOG_ERR,"create thread failed");
		status = -1;
	}			 
	return status;
}


//rewrited download end 
void matomlserv_download_end(serventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t filenum;
	if (length!=1) {
		MFSLOG(LOG_NOTICE,"MLTOMA_DOWNLOAD_END - wrong size (%"PRIu32"/0)",length);
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd>0) {
		close(eptr->metafd);
		eptr->metafd=-1;
	}
	filenum = get8bit(&data);
	MFSLOG(LOG_NOTICE,"the matomlserv_download_end filenum is %d",filenum);
	if (filenum == MFS_SYNC_META) {
		eptr->syncstep = MFS_SYNC_CHANGELOG_START;
		matoml_sync_thread(eptr, MFS_SYNC_CHANGELOG_START);
	}
	if (filenum == MFS_SYNC_CHANGELOG_START) {
		eptr->syncstep = MFS_SYNC_CHANGELOG_END;
		matoml_sync_thread(eptr,MFS_SYNC_CHANGELOG_END);
	}
}

//start one time log transfer
int matomlserv_pack_log(serventry *eptr,int flag) {
	int status;
	int should_return = 0;
	uint8_t log_count = 0;

    NOT_USED(eptr);

	if (flag == 0) {
		eptr->trans->last_idx = eptr->cur_file->idx;
	} else if (flag == 1) {
		eptr->cur_file->idx = eptr->trans->last_idx;
		fseek(eptr->cur_file->fd,eptr->cur_file->idx,SEEK_SET);
	} else {
		MFSLOG(LOG_NOTICE,"unrecognize flag in pack_log");
		return -1;
	}	
	if (eptr->trans->should_rotate == 1) {
		matomlserv_logrotate(eptr);
		eptr->trans->should_rotate = 0;
		eptr->trans->resend = 1;
		return 0;
	}
	while((log_count<MaxLogCount) && (eptr->trans->should_rotate==0) &&(should_return==0)) {
		status = matomlserv_get_log(eptr,eptr->pack_buff[log_count]);
		switch(status) {
			case -1:
				MFSLOG(LOG_ERR,"get log failed,seriours error");
				return -1;
			case 0:
				log_count++;
				break;
			case 1:
				eptr->trans->resend = 1;
				eptr->trans->should_rotate = 1;
				break;
			case 2:
				eptr->trans->resend = 1;
				should_return = 1;
				break;
			default:
				MFSLOG(LOG_ERR,"get_log return unknown message");
				break;	
		}
	}
	if (log_count != 0) {
		matomlserv_send_log(eptr,log_count);
		eptr->trans->resend = 0;
		return 0;
	}

    return 0;
}

//get changelog from file and process
int matomlserv_get_log(serventry *eptr,char *work_buff) {
	int status;
	int ret = 0;
	//uint64_t ver;
	//uint32_t size;
	uint32_t mllogsize;
	char buff[1000];
	//char *work_ptr;
	file_info *new_file;
	
	status = read_changelog(eptr->cur_file,buff);
	if (status < 0) {
		MFSLOG(LOG_ERR,"FATAL fseek failed");
		ret = -1;
		goto l_out;
	}
	if (status == 0) {
		//judge new file process
		new_file = (file_info *)malloc(sizeof(file_info));
		ret = open_changelog(new_file);
		if (ret < 0) {
			MFSLOG(LOG_NOTICE,"judge new file,open file failed");
		} else {
			fseek(new_file->fd,0,SEEK_END);
			new_file->idx = ftell(new_file->fd);
//			syslog(LOG_NOTICE,"cur_file idx is %d,and the new_file idx is %d ",eptr->cur_file->idx,new_file->idx);
			//new changlog.0.mfs generated
			if (eptr->cur_file->idx > new_file->idx) {
				fclose(new_file->fd);
				free(new_file);
				new_file = NULL;
				fclose(eptr->cur_file->fd);
				ret = open_changelog(eptr->cur_file);
				if (ret < 0) {
					MFSLOG(LOG_NOTICE,"rotate changelog ,open failed");
					eptr->mode = KILL;	
				}
				fseek(eptr->cur_file->fd,0,SEEK_SET);
				ret = 1;
				goto l_out;
			} else {
				fclose(new_file->fd);
				free(new_file);
				new_file = NULL;	
//				syslog(LOG_NOTICE,"no more line read and no new changelog.0.mfs generated");
				ret = 2;
				goto l_out;
			}
		}
	}
	if (status > 0 ) {
		mllogsize = strlen(buff) + 1;
		memcpy(work_buff,buff,mllogsize);
		ret = 0;	
	}

l_out:	
    	return ret;
}

//rewrite changelog package send
int matomlserv_send_log(serventry *eptr,uint8_t log_count) {
	uint8_t *data;
	uint8_t count = 0;
	uint64_t ver[MaxLogCount];
	uint32_t size[MaxLogCount];
	char *work_ptr;
	char *pack_ptr[MaxLogCount];
	uint32_t packet_size = 0;

	while(count < log_count) {
		work_ptr = eptr->pack_buff[count];
		GETU64(ver[count],work_ptr);
		EAT(work_ptr,ver[count],':');
		EAT(work_ptr,ver[count],' ');
		size[count] = strlen(work_ptr) + 1;
		pack_ptr[count] = work_ptr;
		packet_size = packet_size + size[count] + 12;
		count++;
	}
	count = 0;
	data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,packet_size+1);
	if (data!=NULL) {
		put8bit(&data,log_count);
		while(count < log_count) {
			put64bit(&data,ver[count]);
			put32bit(&data,size[count]);
			memcpy(data,pack_ptr[count],size[count]);
			data = data + size[count];
			count++;
		}
//		syslog(LOG_NOTICE,"the last version is %d",ver[count-1]);
	} else {
		MFSLOG(LOG_ERR,"create packet error,fatal error");
		eptr->mode = KILL;
	}	

    return 0;
}


//rewrited changelog send
//void matomlserv_send_log(serventry *eptr,uint64_t version,uint8_t *logstr,uint32_t logstrsize) {
//	uint8_t *data;
//
//	if (eptr->version>0 && eptr->listen_sock==0) {
//	data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,9+logstrsize);
//	if (data!=NULL) {
//		put8bit(&data,0xFF);
//		put64bit(&data,version);
//		memcpy(data,logstr,logstrsize);
//	} else {
//		syslog(LOG_ERR,"create packet error,fatal error");
//		eptr->mode = KILL;
//	}
//}

//start changelog transfer
void matomlserv_changelog_start(serventry *eptr,const uint8_t *data,uint32_t length) {
    int status;

    NOT_USED(data);
    NOT_USED(length);
    if (length!=0) {
        MFSLOG(LOG_NOTICE,"MLTOMA_DOWNLOAD_START - wrong size (%"PRIu32"/1)",length);
        eptr->mode=KILL;
        return;
    }	
    status = open_changelog(eptr->cur_file);
    if (status != 0) {
        eptr->mode = KILL;
        MFSLOG(LOG_ERR,"can't open changelog");
    } else {
        eptr->cur_file->idx = eptr->changelog_offset;
        status = fseek(eptr->cur_file->fd,eptr->cur_file->idx,SEEK_SET);
        if (status != 0) {
            eptr->mode = KILL;
            MFSLOG(LOG_ERR,"fseek failed");
        }
        if (matomlserv_pack_log(eptr,0)<0) {
            MFSLOG(LOG_ERR,"get log failed");
        }
    }
}

//continue changelog transfer
int matomlserv_changelog(serventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t flag;
	//char *buff;
	//uint64_t ver;
	//uint32_t size;

	if (length!=1) {
                MFSLOG(LOG_NOTICE,"MLTOMA_DOWNLOAD_DATA - wrong size (%"PRIu32"/12)",length);
                eptr->mode=KILL;
                return -1;
        }
	flag = get8bit(&data);
	if (flag == 0) {
		if (matomlserv_pack_log(eptr,0)<0) {
			MFSLOG(LOG_ERR,"pack log failed,no resend");
		}
//resend should be changed
	} else if (flag == 1) {
		if (matomlserv_pack_log(eptr,1)<0) {
			MFSLOG(LOG_ERR,"pack log failed,resend");
		}
	} else {
		MFSLOG(LOG_ERR,"unrecogize matomlserv_changelog flag");
	}

    return 0;
}			
		
char* matomlserv_makestrip(uint32_t ip) {
	uint8_t *ptr,pt[4];
	uint32_t l,i;
	char *optr;
	ptr = pt;
	put32bit(&ptr,ip);
	l=0;
	for (i=0 ; i<4 ; i++) {
		if (pt[i]>=100) {
			l+=3;
		} else if (pt[i]>=10) {
			l+=2;
		} else {
			l+=1;
		}
	}
	l+=4;
	optr = malloc(l);
	snprintf(optr,l,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,pt[0],pt[1],pt[2],pt[3]);
	optr[l-1]=0;
	return optr;
}

void matomlserv_register(serventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t rversion;
	int status = 0;
	int ret;

	if(isslave()) {
		MFSLOG(LOG_ERR, "slave will not send changelog to shadow\n");
		return;
	}	

	if (eptr->version>0) {
		MFSLOG(LOG_WARNING,"got register message from registered metalogger !!!");
		eptr->mode=KILL;
		return;
	}
	if (length<1) {
		MFSLOG(LOG_NOTICE,"MLTOMA_REGISTER - wrong size (%"PRIu32")",length);
		eptr->mode=KILL;
		return;
	} else {
		rversion = get8bit(&data);
		if (rversion==1) {
			if (length!=7) {
				MFSLOG(LOG_NOTICE,"MLTOMA_REGISTER (ver 1) - wrong size (%"PRIu32"/7)",length);
				eptr->mode=KILL;
				return;
			}
			eptr->version = get32bit(&data);
			eptr->timeout = get16bit(&data);
			ret = worker_thread_init(eptr->servip);
//			syslog(LOG_NOTICE,"eptr->servip is %d,eptr->servstrip is %d",eptr->servip,eptr->servstrip);
			if (status < 0) {
				MFSLOG(LOG_ERR,"worker of init failed");
			}
			eptr->mode = KILL;
			return;
		} else {
			MFSLOG(LOG_NOTICE,"MLTOMA_REGISTER - wrong version (%"PRIu8"/1)",rversion);
			eptr->mode=KILL;
			return;
		}
	}
}


void matomlserv_download_start(serventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t filenum;
	uint64_t size;
	uint8_t *ptr;
	if (length!=1) {
		MFSLOG(LOG_NOTICE,"MLTOMA_DOWNLOAD_START - wrong size (%"PRIu32"/1)",length);
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd>0) {
		close(eptr->metafd);
		eptr->metafd=-1;
	}
	filenum = get8bit(&data);
	if (filenum==1) {
		eptr->metafd = open("metadata.mfs.back",O_RDONLY);
	} else if (filenum==2) {
		eptr->metafd = open("changelog.0.mfs",O_RDONLY);
	} else {
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd<0) {
		ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_START,1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		put8bit(&ptr,0xff);	// error
		return;
	}
	size = lseek(eptr->metafd,0,SEEK_END);
	if (filenum==2) {
		eptr->changelog_offset = size;
//		syslog(LOG_NOTICE,"changelog_offset is %d",changelog_offset);
	}
	ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_START,8);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	put64bit(&ptr,size);	// ok
}

void matomlserv_download_data(serventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint64_t offset;
	uint32_t leng;
	uint32_t crc;
	ssize_t ret;

	if (length!=12) {
		MFSLOG(LOG_NOTICE,"MLTOMA_DOWNLOAD_DATA - wrong size (%"PRIu32"/12)",length);
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd<0) {
		MFSLOG(LOG_NOTICE,"MLTOMA_DOWNLOAD_DATA - file not opened");
		eptr->mode=KILL;
		return;
	}
	offset = get64bit(&data);
	leng = get32bit(&data);
	ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_DATA,16+leng);
	if (ptr==NULL) {
		eptr->mode=KILL;
		return;
	}
	put64bit(&ptr,offset);
	put32bit(&ptr,leng);
#ifdef HAVE_PREAD
	ret = pread(eptr->metafd,ptr+4,leng,offset);
#else /* HAVE_PWRITE */
	lseek(eptr->metafd,offset,SEEK_SET);
	ret = read(eptr->metafd,ptr+4,leng);
#endif /* HAVE_PWRITE */
	if (ret!=(ssize_t)leng) {
		MFSLOG(LOG_NOTICE,"error reading metafile: %m");
		eptr->mode=KILL;
		return;
	}
	crc = mycrc32(0,ptr+4,leng);
	put32bit(&ptr,crc);
}


void matomlserv_beforeclose(serventry *eptr) {
	if (eptr->metafd>0) {
		close(eptr->metafd);
		eptr->metafd=-1;
	}
}

void matomlserv_gotpacket(serventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case MLTOMA_REGISTER:
			matomlserv_register(eptr,data,length);
			break;
		case MLTOMA_DOWNLOAD_START:
			matomlserv_download_start(eptr,data,length);
			break;
		case MLTOMA_DOWNLOAD_DATA:
			matomlserv_download_data(eptr,data,length);
			break;
		case MLTOMA_DOWNLOAD_END:
			matomlserv_download_end(eptr,data,length);
			break;
		case MLTOMA_ACK_CHANGELOG:
			matomlserv_changelog(eptr,data,length);
			break;
		case MLTOMA_CHANGELOG_READY:
			matomlserv_changelog_start(eptr,data,length);
			break;
		default:
			MFSLOG(LOG_NOTICE,"matoml: got unknown message (type:%"PRIu32")",type);
			eptr->mode=KILL;
	}
}

void matomlserv_term(void) {
	serventry *eptr,*eaptr;
	packetstruct *pptr,*paptr;
	MFSLOG(LOG_INFO,"matoml: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matomlservhead;
	while (eptr) {
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		pptr = eptr->outputhead;
		while (pptr) {
			if (pptr->packet) {
				free(pptr->packet);
			}
			paptr = pptr;
			pptr = pptr->next;
			free(paptr);
		}
		eaptr = eptr;
		eptr = eptr->next;
		free(eaptr);
	}
	matomlservhead=NULL;
}

void matomlserv_read(serventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;
	for (;;) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
			MFSLOG(LOG_INFO,"connection with ML(%s) lost",eptr->servstrip);
			eptr->mode = KILL;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				MFSLOG(LOG_INFO,"read from ML(%s) error: %m",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;

		if (eptr->inputpacket.bytesleft>0) {
			return;
		}

		if (eptr->mode==HEADER) {
			ptr = eptr->hdrbuff+4;
			size = get32bit(&ptr);

			if (size>0) {
				if (size>MaxPacketSize) {
					MFSLOG(LOG_WARNING,"ML(%s) packet too long (%"PRIu32"/%u)",eptr->servstrip,size,MaxPacketSize);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.packet = malloc(size);
				if (eptr->inputpacket.packet==NULL) {
					MFSLOG(LOG_WARNING,"ML(%s) packet: out of memory",eptr->servstrip);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.bytesleft = size;
				eptr->inputpacket.startptr = eptr->inputpacket.packet;
				eptr->mode = DATA;
				continue;
			}
			eptr->mode = DATA;
		}

		if (eptr->mode==DATA) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode=HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			matomlserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}
	}
}

void matomlserv_write(serventry *eptr) {
	packetstruct *pack;
	int32_t i;
	for (;;) {
		pack = eptr->outputhead;
		if (pack==NULL) {
			return;
		}
		i=write(eptr->sock,pack->startptr,pack->bytesleft);
		if (i<0) {
			if (errno!=EAGAIN) {
				MFSLOG(LOG_INFO,"write to ML(%s) error: %m",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		pack->startptr+=i;
		pack->bytesleft-=i;
		if (pack->bytesleft>0) {
			return;
		}
		free(pack->packet);
		eptr->outputhead = pack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(pack);
	}
}

void matomlserv_desc(int epoll_fd) {
	//uint32_t now=main_time();
        serventry *eptr,**kptr,**wptr;
        packetstruct *pptr,*paptr;
	struct epoll_event ev;
	int ret;

	if (first_add_listen_sock==0) {
		eptr = (serventry *)malloc(sizeof(serventry));
                eptr->next = matomlservhead;
                matomlservhead = eptr;
                eptr->sock = lsock;
                eptr->mode = HEADER;
                eptr->lastread = eptr->lastwrite = get_current_time();
                eptr->inputpacket.next = NULL;
                eptr->inputpacket.bytesleft = 8;
                eptr->inputpacket.startptr = eptr->hdrbuff;
                eptr->inputpacket.packet = NULL;
                eptr->outputhead = NULL;
                eptr->outputtail = &(eptr->outputhead);
		   /**
		    * as the load meta in slave may cost much time which could not send a heartbeat
		    *
		    * Dongyang Zhang
		    */				
                eptr->timeout = 60;
 
                tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
                eptr->servstrip = matomlserv_makestrip(eptr->servip);
                eptr->version=0;
                eptr->metafd=-1;
	
		eptr->listen_sock = 1;
                eptr->connection = 2;
	
		ev.data.ptr = eptr;
                ev.events = EPOLLIN;
                ret = epoll_ctl(epoll_fd,EPOLL_CTL_ADD,lsock,&ev);
                if(ret!=0) {
                        MFSLOG(LOG_NOTICE,"epoll_ctl error 1");
                }
		first_add_listen_sock = 1;
	}
        kptr = &matomlservhead;
        wptr = &matomlservhead;
	while ((eptr=*kptr)) {
		if (eptr->listen_sock == 0 && eptr->mode != KILL) {
			ev.data.ptr = eptr;
			ev.events = EPOLLIN;		
			if (eptr->outputhead != NULL && eptr->mode != KILL) {
				ev.events = EPOLLIN|EPOLLOUT;
			}
			ret = epoll_ctl(epoll_fd,EPOLL_CTL_MOD,eptr->sock,&ev);
			if(ret!=0) {
	                        MFSLOG(LOG_NOTICE,"epoll_ctl error 2");
	                }
		}
		
	        if (eptr->listen_sock == 1) {
	    		eptr->lastread = eptr->lastwrite = get_current_time();
		}
		if ((uint32_t)(eptr->lastread+eptr->timeout)<(uint32_t)get_current_time()) {
	                eptr->mode = KILL;
	        }
	        if ((uint32_t)(eptr->lastwrite+(eptr->timeout/2))<(uint32_t)get_current_time()&& eptr->outputhead==NULL) {
	                matomlserv_createpacket(eptr,ANTOAN_NOP,0);
	        }
		if (eptr->mode == KILL) {
	                ev.data.ptr = eptr;
			   matomlserv_beforeclose(eptr);
			   epoll_ctl(epoll_fd,EPOLL_CTL_DEL,eptr->sock,&ev);			
                       tcpclose(eptr->sock);
                        if (eptr->inputpacket.packet) {
                                free(eptr->inputpacket.packet);
                        }
                        pptr = eptr->outputhead;
                        while (pptr) {
                                if (pptr->packet) {
                                        free(pptr->packet);
                                }
                                paptr = pptr;
                                pptr = pptr->next;
                                free(paptr);
                        }
			if(eptr == matomlservhead) {
	                	matomlservhead = eptr->next;
	                	wptr = &matomlservhead;
	                }       
	                else {
	                        (*wptr)->next = eptr->next;
	                }       
	                *kptr = eptr->next;
	                free(eptr);
		} else {
			wptr = &eptr;
	                kptr = &(eptr->next);
	        }
	}
}

void matomlserv_serve(int epoll_fd,int count,struct epoll_event *pdesc) {
	//uint32_t now=main_time();
	serventry *eptr,*weptr;
	int ns;
	
	weptr = (serventry *)pdesc[count].data.ptr;
	if ((weptr->listen_sock == 1) && (pdesc[count].events & EPOLLIN)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			MFSLOG(LOG_INFO,"Master<->ML socket: accept error: %m");
		} else {
			struct epoll_event ev;
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = (serventry *)malloc(sizeof(serventry));
			eptr->next = matomlservhead;
			matomlservhead = eptr;
			eptr->sock = ns;
			eptr->mode = HEADER;
			eptr->lastread = eptr->lastwrite = get_current_time();
			eptr->inputpacket.next = NULL;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);
			eptr->timeout = 10;
			
			tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
			eptr->servstrip = matomlserv_makestrip(eptr->servip);
			eptr->version=0;
			eptr->metafd=-1;

			eptr->listen_sock = 0;
                        eptr->connection = 2;

			ev.data.ptr = eptr;
                        ev.events = EPOLLIN | EPOLLOUT;
                        epoll_ctl(epoll_fd,EPOLL_CTL_ADD,ns,&ev);
		}
	}
	if(weptr->listen_sock == 0) {
		if (pdesc[count].events & (EPOLLERR|EPOLLHUP)) {
			weptr->mode = KILL;
		}
		if ((pdesc[count].events & EPOLLIN) && weptr->mode!=KILL) {
			matomlserv_read(weptr);
			weptr->lastread = get_current_time();			
		}
		if ((pdesc[count].events & EPOLLOUT) && weptr->mode!=KILL && weptr->outputhead!=NULL) {
			matomlserv_write(weptr);
			weptr->lastwrite = get_current_time();			
		}
	}
}

int matomlserv_init() {
	ListenHost = cfg_getstr("MATOML_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOML_LISTEN_PORT","9419");
	BindHost = cfg_getstr("BIND_HOST","*");
	MaxReconnect = cfg_getuint32("MAX_RECONNECT",10);
	worker_addr[0].host = cfg_getstr("SYNC_WORKER_HOST_1","*");
	worker_addr[0].port = cfg_getstr("SYNC_WORKER_PORT_1","9422");
        worker_addr[1].host = cfg_getstr("SYNC_WORKER_HOST_2","*");
        worker_addr[1].port = cfg_getstr("SYNC_WORKER_PORT_2","9422");
        worker_addr[2].host = cfg_getstr("SYNC_WORKER_HOST_3","*");
        worker_addr[2].port = cfg_getstr("SYNC_WORKER_PORT_3","9422");
        worker_addr[3].host = cfg_getstr("SYNC_WORKER_HOST_4","*");
        worker_addr[3].port = cfg_getstr("SYNC_WORKER_PORT_4","9422");

	first_add_listen_sock = 0;
	lsock = tcpsocket();
	if (lsock<0) {
		MFSLOG(LOG_ERR,"matoml: socket error: %m");
		fprintf(msgfd,"master <-> metaloggers module: can't create socket\n");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (worker_addr_resolve()<0) {
		MFSLOG(LOG_NOTICE,"worker_addr_resolve failed");
	}
	if (tcpsetacceptfilter(lsock)<0) {
		MFSLOG(LOG_NOTICE,"matoml: can't set accept filter: %m");
	}
	if (tcpstrlisten(lsock,ListenHost,ListenPort,1024)<0) {
		MFSLOG(LOG_ERR,"matoml: listen error: %m");
		fprintf(msgfd,"master <-> metaloggers module: can't listen on socket\n");
		return -1;
	}
	MFSLOG(LOG_NOTICE,"matoml: listen on %s:%s",ListenHost,ListenPort);
	fprintf(msgfd,"master <-> metaloggers module: listen on %s:%s\n",ListenHost,ListenPort);

	matomlservhead = NULL;
	main_destructregister(matomlserv_term);
	main_epollregister(matomlserv_desc,matomlserv_serve);
	return 0;
}
