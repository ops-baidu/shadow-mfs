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

#include "MFSCommunication.h"
#include "datapack.h"
#include "masterconn.h"
#include "crc.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "replay.h"
#include "filesystem.h"
#include "chunks.h"
#include "state.h"
#include "filesystem.h"

#define MaxPacketSize 1500000

#define META_DL_BLOCK 1000000
#define NOT_USED(x) ( (void)(x) )

//typedef struct masterconn {
//	int mode;
//	int sock;
//	int32_t pdescpos;
//	time_t lastread,lastwrite;
//	uint8_t hdrbuff[8];
//	packetstruct inputpacket;
//	packetstruct *outputhead,**outputtail;
//	uint32_t bindip;
//	uint32_t masterip;
//	uint16_t masterport;
//	uint8_t masteraddrvalid;
//
//	uint8_t retrycnt;
//	uint8_t downloading;
//	FILE *logfd;	// using stdio because this is text file
//	int metafd;	// using standard unix I/O because this is binary file
//	uint64_t filesize;
//	uint64_t dloffset;
//	uint64_t dlstartuts;
//} masterconn;

//data connection
static serventry *masterconnsingleton = NULL;
//listen port connection
static serventry *master_listen = NULL;
static serventry *master_serve = NULL;

//metadata sync flag
//static int first_metadata_transfer = 0;

// from config
static uint32_t BackLogsNumber;
static char *MasterHost;
static char *MasterPort;
static char *BindHost;
static uint32_t Timeout;
static int listen_add;

//add lsock for listen sync thread
static char *ListenHost;
static char *ListenPort;
static int first_add;

//add lsock for listen sync thread
static int lsock;

static uint32_t stats_bytesout=0;
static uint32_t stats_bytesin=0;

int meta_ready = 1;
//service ready tag
//further consideration:don't provide service if sync failed ?

void masterconn_stats(uint32_t *bin,uint32_t *bout) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	stats_bytesin = 0;
	stats_bytesout = 0;
}

uint8_t* masterconn_createpacket(serventry *eptr,uint32_t type,uint32_t size) {
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

void masterconn_sendregister(serventry *eptr) {
	uint8_t *buff;

	eptr->downloading=0;
	eptr->metafd=-1;
	eptr->logfd=NULL;

	buff = masterconn_createpacket(eptr,SLATOMA_REGISTER,1+4+2);
	if (buff==NULL) {
		eptr->mode=KILL;
		return;
	}
	put8bit(&buff,1);
	put16bit(&buff,VERSMAJ);
	put8bit(&buff,VERSMID);
	put8bit(&buff,VERSMIN);
	put16bit(&buff,Timeout);
}

//to handle changelog rotate
void masterconn_changelog_rotate(serventry *eptr,const uint8_t *data,uint32_t length) {
        char logname1[100],logname2[100];
	FILE *new_fd;
	uint32_t i;

	if (length==1 && data[0]==0x55) {
                if (eptr->logfd!=NULL) {
                        fclose(eptr->logfd);
                        eptr->logfd=NULL;
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
                
                /* 
                 * as the changelog is transfered by master, if we call fs_dostoreall timely, 
                 * the version stored will be different with changelog which will evetually 
                 * cause the shadow master version dismatch when master swtich. Call 
                 * it when the master finish rotate in order to let all the master have the 
                 * same changelog
                 *
                 * Dongyang Zhang 
                 */
                fs_dostoreall();     
                
                return;
        } else {
		MFSLOG(LOG_NOTICE,"MATOSLA_CHANGELOG_ROTATE wrong format");
	}   
}


//rewrite metachanges_log func
void masterconn_metachanges_log(serventry *eptr,const uint8_t *data,uint32_t length) {
	char log_data[1000];
	char log_str[1000];                                                                                                                                        
	uint64_t version;
	uint32_t size;
	//FILE *new_fd;
	uint8_t log_count = 0;
	uint8_t log_limit;
	int ret = 0;

    NOT_USED(length);
	log_limit = get8bit(&data);
	if (eptr->logfd == NULL) {
		eptr->logfd = fopen("changelog.0.mfs","a");
	}
	while(log_count < log_limit) {
		version = get64bit(&data);
		size = get32bit(&data);
		memcpy(log_str,data,size);
		data = data + size;
		if (log_str[size - 1] != '\0') {
			MFSLOG(LOG_NOTICE,"MATOSLA_CHANGELOG - invalide string the last is %c",log_str[size - 1]);
		}
		snprintf(log_data,sizeof(log_data),"%"PRIu64": %s\n",version,log_str);
	        if (eptr->logfd) {
                	ret = replay(log_data);
         	      	if (ret != 0) {
				MFSLOG(LOG_ERR,"changelog replay failed");
				break;
                  	      //more complicated method to ensure consistency
                	}
                	fprintf(eptr->logfd,"%"PRIu64": %s",version,log_str);
                	fflush(eptr->logfd);
			log_count++;
        	} else {
                	MFSLOG(LOG_NOTICE,"lost MFS change %"PRIu64": %s",version,log_str);
			ret = -1;
			break;
        	}
	}
	if (ret == 0) {
		masterconn_ack_changelog(eptr,0);
	} else {
		masterconn_ack_changelog(eptr,1);
	}
}

//	if (data[length-1]!='\0') {
//		syslog(LOG_NOTICE,"MATOML_METACHANGES_LOG - invalid string");
//		eptr->mode = KILL;
//		return;
//	}
//
//	if (eptr->logfd==NULL) {
//		eptr->logfd = fopen("changelog.0.mfs","a");
//	}
//
//	data++;
//	version = get64bit(&data);
//	snprintf(log_data,sizeof(log_data),"%"PRIu64": %s\n",version,data);
//	if (eptr->logfd) {
//		ret = replay(log_data);
//		if (ret != 0) {
//			syslog(LOG_ERR,"replay changelog failed");
//			masterconn_ack_changelog(eptr,1);
//			//more complicated method to ensure consistency
//		}
//		fprintf(eptr->logfd,"%"PRIu64": %s",version,data);
//		fflush(eptr->logfd);
//		masterconn_ack_changelog(eptr,0);
//	} else {
//		syslog(LOG_NOTICE,"lost MFS change %"PRIu64": %s",version,data);
//		masterconn_ack_changelog(eptr,1);
//	}
//}

int masterconn_download_end(serventry *eptr,uint8_t filenum) {
	uint8_t *buff;

	eptr->downloading=0;
	buff = masterconn_createpacket(eptr,SLATOMA_DOWNLOAD_END,1);
	if (buff==NULL) {
		eptr->mode=KILL;
		return -1;
	}
	put8bit(&buff,filenum);
	if (eptr->metafd>0) {
		if (close(eptr->metafd)<0) {
			MFSLOG(LOG_NOTICE,"error closing metafile: %m");
			eptr->metafd=-1;
			return -1;
		}
		eptr->metafd=-1;
	}
	return 0;
}

void masterconn_download_init(serventry *eptr,uint8_t filenum) {
	uint8_t *ptr;
	MFSLOG(LOG_NOTICE,"download_init %d",filenum);
	if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->downloading==0) {
		MFSLOG(LOG_NOTICE,"sending packet");
		ptr = masterconn_createpacket(eptr,SLATOMA_DOWNLOAD_START,1);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		put8bit(&ptr,filenum);
		eptr->downloading=filenum;
	}
}

void masterconn_metadownloadinit(void) {
	masterconn_download_init(master_serve,1);
}
void masterconn_changelog0downloadinit(void) {
	masterconn_download_init(master_serve,2);
}
void masterconn_download_next(serventry *eptr) {
	uint8_t *ptr;
	uint8_t filenum;
	uint64_t dltime;
	uint64_t starttime;
 
	if (eptr->dloffset>=eptr->filesize) {	// end of file
		filenum = eptr->downloading;
		dltime = main_utime()-eptr->dlstartuts;
		MFSLOG(LOG_NOTICE,"%s downloaded %"PRIu64"B/%"PRIu64".%06"PRIu32"s (%.3lf MB/s)",(filenum==1)?"metadata":(filenum==2)?"changelog.0":"???",eptr->filesize,dltime/1000000,(uint32_t)(dltime%1000000),(double)(eptr->filesize)/(double)(dltime));
		if (filenum==1) {
			if (rename("metadata.tmp","metadata.mfs")<0) {
				MFSLOG(LOG_NOTICE,"can't rename downloaded metadata - do it manually before next download");
			} else {
				MFSLOG(LOG_NOTICE,"loading metadata ...");
			        fs_strinit();
			        chunk_strinit();
        			starttime = get_current_time();
        			if (fs_loadall(NULL)<0) {
                			return;
					MFSLOG(LOG_NOTICE,"fs init failed");
        			}
        			MFSLOG(LOG_NOTICE,"metadata file has been loaded");
			}
		} else if (filenum==2) {
			if (rename("changelog.0.tmp","changelog.0.mfs")<0) {
				MFSLOG(LOG_NOTICE,"can't rename downloaded changelog.0.mfs - do it manually before next download");
			} else {
				if (restore() < 0) {
					MFSLOG(LOG_ERR,"restore failed");
				} else {
					meta_ready = 0;
				}
			}		
		}
		
		//filenum:1,metadata finish ;2,changelog.0 finish
		if (masterconn_download_end(eptr,filenum)<0) {
			return; 
		}
	} else {	// send request for next data packet
		ptr = masterconn_createpacket(eptr,SLATOMA_DOWNLOAD_DATA,12);
		if (ptr==NULL) {
			eptr->mode=KILL;
			return;
		}
		put64bit(&ptr,eptr->dloffset);
		if (eptr->filesize-eptr->dloffset>META_DL_BLOCK) {
			put32bit(&ptr,META_DL_BLOCK);
		} else {
			put32bit(&ptr,eptr->filesize-eptr->dloffset);
		}
	}
}

void masterconn_download_start(serventry *eptr,const uint8_t *data,uint32_t length) {
	if (length!=1 && length!=8) {
		MFSLOG(LOG_NOTICE,"MATOSLA_DOWNLOAD_START - wrong size (%"PRIu32"/1|8)",length);
		eptr->mode = KILL;
		return;
	}
	if (length==1) {
		MFSLOG(LOG_NOTICE,"download start error");
		return;
	}
	eptr->filesize = get64bit(&data);
	eptr->dloffset = 0;
	eptr->retrycnt = 0;
	eptr->dlstartuts = main_utime();
	if (eptr->downloading==1) {
		eptr->metafd = open("metadata.tmp",O_WRONLY | O_TRUNC | O_CREAT,0666);
	} else if (eptr->downloading==2) {
		eptr->metafd = open("changelog.0.tmp",O_WRONLY | O_TRUNC | O_CREAT,0666);
	} else {
		MFSLOG(LOG_NOTICE,"unexpected MATOSLA_DOWNLOAD_START packet");
		eptr->mode = KILL;
		return;
	}
	if (eptr->metafd<0) {
		MFSLOG(LOG_NOTICE,"error opening metafile: %m");
		masterconn_download_end(eptr,4);
		return;
	}
	masterconn_download_next(eptr);
}

void masterconn_download_data(serventry *eptr,const uint8_t *data,uint32_t length) {
	uint64_t offset;
	uint32_t leng;
	uint32_t crc;
	ssize_t ret;
	if (eptr->metafd<0) {
		MFSLOG(LOG_NOTICE,"MATOSLA_DOWNLOAD_DATA - file not opened");
		eptr->mode = KILL;
		return;
	}
	if (length<16) {
		MFSLOG(LOG_NOTICE,"MATOSLA_DOWNLOAD_DATA - wrong size (%"PRIu32"/16+data)",length);
		eptr->mode = KILL;
		return;
	}
	offset = get64bit(&data);
	leng = get32bit(&data);
	crc = get32bit(&data);
	if (leng+16!=length) {
		MFSLOG(LOG_NOTICE,"MATOSLA_DOWNLOAD_DATA - wrong size (%"PRIu32"/16+%"PRIu32")",length,leng);
		eptr->mode = KILL;
		return;
	}
	if (offset!=eptr->dloffset) {
		MFSLOG(LOG_NOTICE,"MATOSLA_DOWNLOAD_DATA - unexpected file offset (%"PRIu64"/%"PRIu64")",offset,eptr->dloffset);
		eptr->mode = KILL;
		return;
	}
	if (offset+leng>eptr->filesize) {
		MFSLOG(LOG_NOTICE,"MATOSLA_DOWNLOAD_DATA - unexpected file size (%"PRIu64"/%"PRIu64")",offset+leng,eptr->filesize);
		eptr->mode = KILL;
		return;
	}
#ifdef HAVE_PWRITE
	ret = pwrite(eptr->metafd,data,leng,offset);
#else /* HAVE_PWRITE */
	lseek(eptr->metafd,offset,SEEK_SET);
	ret = write(eptr->metafd,data,leng);
#endif /* HAVE_PWRITE */
	if (ret!=(ssize_t)leng) {
		MFSLOG(LOG_NOTICE,"error writing metafile: %m");
		if (eptr->retrycnt>=5) {
			masterconn_download_end(eptr,4);
		} else {
			eptr->retrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	if (crc!=mycrc32(0,data,leng)) {
		MFSLOG(LOG_NOTICE,"metafile data crc error");
		if (eptr->retrycnt>=5) {
			masterconn_download_end(eptr,4);
		} else {
			eptr->retrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	if (fsync(eptr->metafd)<0) {
		MFSLOG(LOG_NOTICE,"error syncing metafile: %m");
		if (eptr->retrycnt>=5) {
			masterconn_download_end(eptr,4);
		} else {
			eptr->retrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	eptr->dloffset+=leng;
	eptr->retrycnt=0;
	masterconn_download_next(eptr);
}

//confirm changelog replayed successful
void masterconn_ack_changelog(serventry *eptr,int flag) {
	uint8_t *ptr;

	ptr = masterconn_createpacket(eptr,SLATOMA_ACK_CHANGELOG,1);			
	if (ptr == NULL) {
		eptr->mode = KILL;
		return;
	}
	put8bit(&ptr,flag);
}

//confirm sync thread is ready to transfer metadata
void masterconn_sync_thread(serventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t flag;
	//serventry *weptr;

    NOT_USED(length);
	flag = get8bit(&data);
	MFSLOG(LOG_NOTICE,"the masterconn_sync_thread flag is %d",flag);
	if (flag == 1) {
		masterconn_metadownloadinit();
	} else if (flag == 2) {
		masterconn_changelog0downloadinit();
	} else if (flag == 3) {
		masterconn_changelog_ready(eptr);
	} else {
		MFSLOG(LOG_NOTICE,"MATOSLA_SYNC_THREAD:unrecognize flag");
	}
	
}

//confirm metalog trans ready
void masterconn_changelog_ready(serventry *eptr) {
	uint8_t *ptr;

	ptr = masterconn_createpacket(eptr,SLATOMA_CHANGELOG_READY,0);
	if (ptr == NULL) {
		eptr->mode = KILL;
		MFSLOG(LOG_ERR,"can't createpacket");
	}
}


void masterconn_beforeclose(serventry *eptr) {
	if (eptr->metafd>0) {
		close(eptr->metafd);
		unlink("metadata.tmp");
		unlink("changelog.0.tmp");
	}
	if (eptr->logfd) {
		fclose(eptr->logfd);
	}
}

void masterconn_gotpacket(serventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	switch (type) {
		case ANTOAN_NOP:
			break;
		case MATOSLA_METACHANGES_LOG:
			masterconn_metachanges_log(eptr,data,length);
			break;
		case MATOSLA_CHANGELOG_ROTATE:
			masterconn_changelog_rotate(eptr,data,length);
			break;
		case MATOSLA_DOWNLOAD_START:
			masterconn_download_start(eptr,data,length);
			break;
		case MATOSLA_DOWNLOAD_DATA:
			masterconn_download_data(eptr,data,length);
			break;
		case MATOSLA_SYNC_THREAD:
			masterconn_sync_thread(eptr,data,length);
			break;
		default:
			MFSLOG(LOG_NOTICE,"got unknown message (type:%"PRIu32")",type);
			eptr->mode = KILL;
	}
}

void masterconn_term(void) {
	packetstruct *pptr,*paptr;
	serventry *eptr;

	if (masterconnsingleton != NULL) {
		eptr = masterconnsingleton;
		if (eptr->mode!=FREE) {
			tcpclose(eptr->sock);
		       	if (eptr->mode!=CONNECTING) {
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
			}
		}
		free(eptr);
		masterconnsingleton = NULL;
	}
	
	if (master_listen != NULL) {
		eptr=master_listen;
		if(eptr->mode != FREE) {
	                tcpclose(eptr->sock);
	                if (eptr->mode!=CONNECTING) {
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
	                }
	        }
	        free(eptr);
		master_listen = NULL;
	}

	if (master_serve != NULL) {	
	        eptr=master_serve;
        	if(eptr->mode != FREE) {
                	tcpclose(eptr->sock);
         	       if (eptr->mode!=CONNECTING) {
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
                	}
        	}
        	free(eptr);
        	master_serve = NULL;
	}
}

void masterconn_connected(serventry *eptr) {
	tcpnodelay(eptr->sock);
	eptr->mode=HEADER;
	eptr->inputpacket.next = NULL;
	eptr->inputpacket.bytesleft = 8;
	eptr->inputpacket.startptr = eptr->hdrbuff;
	eptr->inputpacket.packet = NULL;
	eptr->outputtail = &(eptr->outputhead);

	masterconn_sendregister(eptr);
	eptr->lastread = eptr->lastwrite = get_current_time();
}

//initialize listen port
int masterconn_initlisten() {
        lsock = tcpsocket();
        if (lsock<0) {
                MFSLOG(LOG_ERR,"smtoma: socket error: %m");
                fprintf(msgfd,"shadow master <-> master module: can't create socket\n");
                return -1;
        }
        tcpnonblock(lsock);
        tcpnodelay(lsock);
        tcpreuseaddr(lsock);
        if (tcpsetacceptfilter(lsock)<0) {
                MFSLOG(LOG_NOTICE,"smtoma: can't set accept filter: %m");
        }
        if (tcpstrlisten(lsock,ListenHost,ListenPort,100)<0) {
                MFSLOG(LOG_ERR,"smtoma: listen  port:%s error: %m\n", ListenPort);
                fprintf(msgfd,"shadow master to master  module: can't listen on socket\n");
                return -1;
        }
        MFSLOG(LOG_NOTICE,"smtoma: listen on %s:%s",ListenHost,ListenPort);
        fprintf(msgfd,"slave <-> master module: listen on %s:%s\n",ListenHost,ListenPort);
	return 0;
}

int masterconn_initconnect(serventry *eptr) {
	int status;
	if (eptr->masteraddrvalid==0) {
		uint32_t mip,bip;
		uint16_t mport;
		if (tcpresolve(BindHost,NULL,&bip,NULL,1)>=0) {
			eptr->bindip = bip;
		} else {
			eptr->bindip = 0;
		}
		if (tcpresolve(MasterHost,MasterPort,&mip,&mport,0)>=0) {
			eptr->masterip = mip;
			eptr->masterport = mport;
			eptr->masteraddrvalid = 1;
		} else {
            MFSLOG(LOG_WARNING,"can't resolve master host/port (%s:%s)",MasterHost,MasterPort);
			return -1;
		}
	}
	eptr->sock=tcpsocket();
	if (eptr->sock<0) {
			MFSLOG(LOG_WARNING,"create socket, error: %m");
		return -1;
	}
	if (tcpnonblock(eptr->sock)<0) {
        MFSLOG(LOG_WARNING,"set nonblock, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		return -1;
	}
	if (eptr->bindip>0) {
		if (tcpnumbind(eptr->sock,eptr->bindip,0)<0) {
            MFSLOG(LOG_WARNING,"can't bind socket to given ip: %m");
			tcpclose(eptr->sock);
			eptr->sock=-1;
			return -1;
		}
	}
	status = tcpnumconnect(eptr->sock,eptr->masterip,eptr->masterport);
	if (status<0) {
        MFSLOG(LOG_WARNING,"connect failed, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		return -1;
	}
	if (status==0) {
		MFSLOG(LOG_NOTICE,"connected to Master immediately");
		masterconn_connected(eptr);
	} else {
		eptr->mode = CONNECTING;
		MFSLOG(LOG_NOTICE,"connecting ...");
	}
//	syslog(LOG_NOTICE,"the masconn's sock is %d,the connection id is %d,masterconnsingleton's sock is %d,masterconnsingleton's id is %d",eptr->sock,eptr->connection,masterconnsingleton->sock,masterconnsingleton->connection);
	return 0;
}

void masterconn_connecttest(serventry *eptr) {
	int status;

	status = tcpgetstatus(eptr->sock);
	if (status) {
		MFSLOG(LOG_WARNING,"connection failed, error: %m");
		tcpclose(eptr->sock);
		eptr->sock=-1;
		eptr->mode=FREE;
	} else {
		MFSLOG(LOG_NOTICE,"connected to Master");
		masterconn_connected(eptr);
	}
}

void masterconn_read(serventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;
	for (;;) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
			MFSLOG(LOG_INFO,"Master connection lost");
			eptr->mode = KILL;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				MFSLOG(LOG_INFO,"read from Master error: %m");
				eptr->mode = KILL;
			}
			return;
		}
		stats_bytesin+=i;
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
					MFSLOG(LOG_WARNING,"Master packet too long (%"PRIu32"/%u)",size,MaxPacketSize);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.packet = malloc(size);
				if (eptr->inputpacket.packet==NULL) {
					MFSLOG(LOG_WARNING,"Master packet: out of memory");
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

			masterconn_gotpacket(eptr,type,eptr->inputpacket.packet,size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}
	}
}

void masterconn_write(serventry *eptr) {
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
				MFSLOG(LOG_INFO,"write to Master error: %m");
				eptr->mode = KILL;
			}
			return;
		}
		stats_bytesout+=i;
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


void masterconn_desc(int epoll_fd) {
	serventry *eptr;
	packetstruct *pptr,*paptr;
	int ret;	
	struct epoll_event ev_listen,ev_first,ev;

	if(ismaster()) {
		return;
	}

	//set up listen socket for connection with sync thread
	if (listen_add==0) {
		master_listen = (serventry *)malloc(sizeof(serventry));
		eptr = master_listen;
                eptr->sock = lsock;
                eptr->mode = HEADER;
		eptr->logfd = NULL;
		eptr->inputpacket.next = NULL;
      		eptr->inputpacket.bytesleft = 8;
        	eptr->inputpacket.startptr = eptr->hdrbuff;
        	eptr->inputpacket.packet = NULL;
        	eptr->outputhead = NULL;
        	eptr->outputtail = &(eptr->outputhead);
		eptr->lastread = eptr->lastwrite = get_current_time();

		eptr->listen_sock = 1;
		eptr->connection = 3;
		
                ev_listen.data.ptr = eptr;
                ev_listen.events = EPOLLIN;
                ret = epoll_ctl(epoll_fd,EPOLL_CTL_ADD,lsock,&ev_listen);
                if(ret!=0) {
                        MFSLOG(LOG_NOTICE,"epoll_ctl error");
                }	
		listen_add = 1;
	}
	//first connect master
	if (first_add==0) {
		eptr = masterconnsingleton;
		ev_first.data.ptr = eptr;
		ev_first.events = EPOLLIN|EPOLLOUT;
                ret = epoll_ctl(epoll_fd,EPOLL_CTL_ADD,eptr->sock,&ev_first);
		if(ret!=0) {
                        MFSLOG(LOG_NOTICE,"epoll_ctl error");
                }
		first_add = 1;
	}
	//destroy first connection
	if (masterconnsingleton != NULL) {
		eptr = masterconnsingleton;
		if (eptr->mode == KILL) {
			ev.data.ptr = eptr;
	
	               	masterconn_beforeclose(eptr);

				/**
				  * we should epoll del the fd first, then close the fd
				  * otherwise, the  epoll_ctl(EPOLL_CTL_DEL)
				  * will not find it in files_struct
				  *
				  * Dongyang Zhang 
				  */			
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
	               	eptr->mode = FREE;
			free(masterconnsingleton);
			masterconnsingleton = NULL;
	       	}
	}

	if (master_listen != NULL) {
		eptr = master_listen;
		if (eptr->mode == KILL) {
	                ev.data.ptr = eptr;
	
	                masterconn_beforeclose(eptr);
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
                	eptr->mode = FREE;
			free(master_listen);
			master_listen = NULL;
        	}
	}

	if (master_serve != NULL) {
		eptr = master_serve;
                ev.data.ptr = eptr;
                ev.events = EPOLLIN;
                if (eptr->outputhead != NULL && eptr->mode != KILL) {
                        ev.events = EPOLLIN | EPOLLOUT;
                }
                ret = epoll_ctl(epoll_fd,EPOLL_CTL_MOD,eptr->sock,&ev);
                if(ret!=0) {
                        MFSLOG(LOG_NOTICE,"epoll_ctl error 2");
                }   
	        if (eptr->mode == KILL) {
	                ev.data.ptr = eptr;

        	        masterconn_beforeclose(eptr);
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
                	eptr->mode = FREE;
			free(master_serve);
			master_serve = NULL;
        	}
	}
}


void masterconn_serve(int epoll_fd,int count,struct epoll_event *pdesc) {
	//uint32_t now=main_time();
	serventry *weptr = NULL,*eptr = NULL;
	int ns;
	int ret;
       struct epoll_event ev;

	if(ismaster()) {
		return;
	}
	
	weptr = (serventry *)pdesc[count].data.ptr;
	if ((weptr->listen_sock == 1) && (pdesc[count].events & EPOLLIN) && (weptr->mode != KILL)) {
		ns = tcpaccept(lsock);
		if (ns<0) {
                        MFSLOG(LOG_INFO,"master sync thread<->shadow master socket: accept error: %m");
                } else {
                        tcpnonblock(ns);
                        tcpnodelay(ns);
                        master_serve = (serventry *)malloc(sizeof(serventry));
			eptr = master_serve;
                        eptr->sock = ns;
                        eptr->mode = HEADER;
                        eptr->lastread = eptr->lastwrite = get_current_time();                 
        		eptr->inputpacket.next = NULL;
        		eptr->inputpacket.bytesleft = 8;
        		eptr->inputpacket.startptr = eptr->hdrbuff;
        		eptr->inputpacket.packet = NULL;
			eptr->outputhead = NULL;
        		eptr->outputtail = &(eptr->outputhead);

			eptr->downloading = 0;
			eptr->metafd=-1;
                        eptr->logfd=NULL;

			eptr->listen_sock = 0;
                        eptr->connection = 3;
			
			ev.data.ptr = eptr;
                        ev.events = EPOLLIN;
                        ret = epoll_ctl(epoll_fd,EPOLL_CTL_ADD,ns,&ev);
			if (ret < 0) {
				MFSLOG(LOG_NOTICE,"add epoll fail");
			}
		}
	}
	if (weptr->listen_sock == 0) {
		if (weptr->mode == CONNECTING) {
			if (pdesc[count].events & (EPOLLHUP | EPOLLERR)) {
				masterconn_connecttest(weptr);
			}
			if (pdesc[count].events & EPOLLOUT) {
				masterconn_connecttest(weptr);
			}
		} else {	
			if ((pdesc[count].events & (EPOLLHUP | EPOLLERR)) && (weptr->mode != KILL)) {
				MFSLOG(LOG_NOTICE, "set to NULL");
				weptr->mode = KILL;
			}
			if ((weptr->mode==HEADER || weptr->mode==DATA) && (pdesc[count].events & EPOLLIN)) { // FD_ISSET(eptr->sock,rset)) {
				masterconn_read(weptr);
				weptr->lastread = get_current_time();				
			}
			if ((weptr->mode==HEADER || weptr->mode==DATA) && (pdesc[count].events & EPOLLOUT)) { // FD_ISSET(eptr->sock,wset)) {
				masterconn_write(weptr);
				weptr->lastwrite = get_current_time();				
			}
			if ((weptr->mode==HEADER || weptr->mode==DATA) && weptr->lastread+Timeout<get_current_time()) {
				MFSLOG(LOG_NOTICE, "set to NULL");
				weptr->mode = KILL;
			}
			if ((weptr->mode==HEADER || weptr->mode==DATA) && weptr->lastwrite+5<get_current_time()
				&& weptr->outputhead==NULL) {
				masterconn_createpacket(weptr,ANTOAN_NOP,0);
			}
		}
	}
}

//void masterconn_reload(void) {
//	serventry *eptr = masterconnsingleton;
//	eptr->masteraddrvalid=0;
//}


int masterconn_init() {
	uint32_t ReconnectionDelay;
	uint32_t MetaDLFreq;
	serventry *eptr;

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",5);
	MasterHost = cfg_getstr("MASTER_HOST","mfsmaster");
	MasterPort = cfg_getstr("MASTER_PORT","9423");
	BindHost = cfg_getstr("BIND_HOST","*");
	Timeout = cfg_getuint32("MASTER_TIMEOUT",60);
	BackLogsNumber = cfg_getuint32("BACK_LOGS",50);
	MetaDLFreq = cfg_getuint32("META_DOWNLOAD_FREQ",24);
	ListenHost = cfg_getstr("SYNC_THREAD_LISTEN_HOST","*");
	ListenPort = cfg_getstr("SYNC_THREAD_LISTEN_PORT","9422");

	first_add = 0;
	listen_add = 0;

	if (Timeout>65536) {
		Timeout=65535;
	}
	if (Timeout<=1) {
		Timeout=2;
	}
	if (BackLogsNumber<5) {
		BackLogsNumber=5;
	}
	if (BackLogsNumber>10000) {
		BackLogsNumber=10000;
	}
	if (MetaDLFreq>(BackLogsNumber/2)) {
		MetaDLFreq=BackLogsNumber/2;
	}
	

	masterconnsingleton = (serventry *)malloc(sizeof(serventry));
	eptr = masterconnsingleton;

	eptr->masteraddrvalid = 0;
	eptr->mode = FREE;
	eptr->logfd = NULL;
	eptr->metafd = -1;
	eptr->connection = 3;
	eptr->listen_sock = 0;

	if (masterconn_initlisten()<0) {
               	return -1;
        }

	if (masterconn_initconnect(eptr)<0) {
		return -1;
	}
	

	main_destructregister(masterconn_term);
	main_epollregister(masterconn_desc,masterconn_serve);
//	main_reloadregister(masterconn_reload);
//	main_timeregister(TIMEMODE_RUNONCE,MetaDLFreq*3600,630,masterconn_metadownloadinit);
//	main_timeregister(TIMEMODE_RUNONCE,60,0,masterconn_sessionsdownloadinit);
	(void)msgfd;
	return 0;
}
