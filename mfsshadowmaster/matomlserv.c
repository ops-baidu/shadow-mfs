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
#include "matomlserv.h"
#include "crc.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"

#define MaxPacketSize 1500000

static serventry *matomlservhead=NULL;
static int lsock;
//static int32_t lsockpdescpos;
static int first_add_listen_sock;

// from config
static char *ListenHost;
static char *ListenPort;

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

//void matomlserv_status(void) {
//	serventry *eptr;
//	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
//		if (eptr->listen_sock==0) {
//			if (eptr->mode==HEADER || eptr->mode==DATA) {
//				return;
//			}
//		}
//	}
//	syslog(LOG_WARNING,"no meta loggers connected !!!");
//}

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

void matomlserv_register(serventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t rversion;

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
		eptr->metafd = open("sessions.mfs",O_RDONLY);
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

void matomlserv_download_end(serventry *eptr,const uint8_t *data,uint32_t length) {
	(void)data;
	if (length!=0) {
		MFSLOG(LOG_NOTICE,"MLTOMA_DOWNLOAD_END - wrong size (%"PRIu32"/0)",length);
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd>0) {
		close(eptr->metafd);
		eptr->metafd=-1;
	}
}

void matomlserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize) {
	serventry *eptr;
	uint8_t *data;

	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->version>0 && eptr->listen_sock==0) {
			data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,9+logstrsize);
			if (data!=NULL) {
				put8bit(&data,0xFF);
				put64bit(&data,version);
				memcpy(data,logstr,logstrsize);
			} else {
				eptr->mode = KILL;
			}
		}
	}
}

void matomlserv_broadcast_logrotate() {
	serventry *eptr;
	uint8_t *data;

	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->version>0 && eptr->listen_sock==0) {
			data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,1);
			if (data!=NULL) {
				put8bit(&data,0x55);
			} else {
				eptr->mode = KILL;
			}
		}
	}
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
	uint32_t now=get_current_time();
        serventry *eptr,**kptr,**wptr;
        packetstruct *pptr,*paptr;
	struct epoll_event ev;
	int ret;

	if (first_add_listen_sock==0) {
		eptr = malloc(sizeof(serventry));
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
                eptr->timeout = 10;
 
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
		if ((uint32_t)(eptr->lastread+eptr->timeout)<(uint32_t)now) {
                        eptr->mode = KILL;
                }
                if ((uint32_t)(eptr->lastwrite+(eptr->timeout/2))<(uint32_t)now && eptr->outputhead==NULL) {
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
                        if (eptr->servstrip) {
                                free(eptr->servstrip);
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
			eptr = malloc(sizeof(serventry));
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
	if (tcpsetacceptfilter(lsock)<0) {
		MFSLOG(LOG_NOTICE,"matoml: can't set accept filter: %m");
	}
	if (tcpstrlisten(lsock,ListenHost,ListenPort,100)<0) {
		MFSLOG(LOG_ERR,"matoml: listen error: %m");
		fprintf(msgfd,"master <-> metaloggers module: can't listen on socket\n");
		return -1;
	}
	MFSLOG(LOG_NOTICE,"matoml: listen on %s:%s",ListenHost,ListenPort);
	fprintf(msgfd,"master <-> metaloggers module: listen on %s:%s\n",ListenHost,ListenPort);

	matomlservhead = NULL;
	main_destructregister(matomlserv_term);
	main_epollregister(matomlserv_desc,matomlserv_serve);
//	main_timeregister(TIMEMODE_SKIP,60,0,matomlserv_status);
	return 0;
}
