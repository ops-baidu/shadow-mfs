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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/poll.h>
#include <syslog.h>
#include <sys/time.h>
#include <time.h>
#include <limits.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "stats.h"
#include "sockets.h"
#include "md5.h"
#include "datapack.h"

typedef struct _master_info {
	char masterstrip[17];
	uint32_t masterip;
	uint16_t masterport;
	int fd;
	int disconnect;
	time_t lastwrite;
	int sessionlost;
	uint32_t sessionid;
	pthread_mutex_t fdlock;
	pthread_t rpthid,npthid;
	struct _master_info *next;
} master_info;

typedef struct _threc {
	pthread_t thid;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	uint8_t *buff;
	uint32_t buffsize;
	uint8_t sent;
	uint8_t status;
	uint8_t release;	// cond variable
	uint8_t waiting;
	uint32_t size;
	uint32_t cmd;
	uint32_t packetid;
	master_info *master_used;
	struct _threc *next;
} threc;

typedef struct _aquired_file {
	uint32_t inode;
	uint32_t cnt;
	struct _aquired_file *next;
} aquired_file;

#define DEFAULT_BUFFSIZE 10000
#define RECEIVE_TIMEOUT 10

#define MAX_MASTER_MUN 10

#define WRITE_EFFECT_USEC 1000000

static threc *threchead=NULL;

static aquired_file *afhead=NULL;

//static int fd;
//static int disconnect;
//static time_t lastwrite;
//static int sessionlost;

static uint32_t maxretries;

//static pthread_t rpthid,npthid;
//static pthread_mutex_t fdlock,reclock,aflock;
static pthread_mutex_t reclock,aflock,slave_select_lock;

static pthread_t update_flag_pthid;
static pthread_mutex_t write_flag_lock;
static uint64_t usec_last_write;
static uint8_t write_flag=0;

//static uint32_t sessionid;

//static char masterstrip[17];
//static uint32_t masterip=0;
//static uint16_t masterport=0;
static char srcstrip[17];
static uint32_t srcip=0;
static master_info *master_head=NULL,*slave_selected=NULL;
static uint32_t master_count = 0;

void fs_getmasterlocation(uint8_t loc[10]) {
	put32bit(&loc,master_head->masterip);
	put16bit(&loc,master_head->masterport);
	put32bit(&loc,master_head->sessionid);
}

uint32_t fs_getsrcip() {
	return srcip;
}

enum {
	MASTER_CONNECTS = 0,
	MASTER_BYTESSENT,
	MASTER_BYTESRCVD,
	MASTER_PACKETSSENT,
	MASTER_PACKETSRCVD,
	STATNODES
};

static uint64_t *statsptr[STATNODES];
static pthread_mutex_t statsptrlock = PTHREAD_MUTEX_INITIALIZER;

void master_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"master");
	statsptr[MASTER_PACKETSRCVD] = stats_get_counterptr(stats_get_subnode(s,"packets_received"));
	statsptr[MASTER_PACKETSSENT] = stats_get_counterptr(stats_get_subnode(s,"packets_sent"));
	statsptr[MASTER_BYTESRCVD] = stats_get_counterptr(stats_get_subnode(s,"bytes_received"));
	statsptr[MASTER_BYTESSENT] = stats_get_counterptr(stats_get_subnode(s,"bytes_sent"));
	statsptr[MASTER_CONNECTS] = stats_get_counterptr(stats_get_subnode(s,"reconnects"));
}

void master_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		pthread_mutex_lock(&statsptrlock);
		(*statsptr[id])++;
		pthread_mutex_unlock(&statsptrlock);
	}
}

void master_stats_add(uint8_t id,uint64_t s) {
	if (id<STATNODES) {
		pthread_mutex_lock(&statsptrlock);
		(*statsptr[id])+=s;
		pthread_mutex_unlock(&statsptrlock);
	}
}

const char* errtab[]={ERROR_STRINGS};

static inline const char* mfs_strerror(uint8_t status) {
	if (status>ERROR_MAX) {
		status=ERROR_MAX;
	}
	return errtab[status];
}
/*
void fs_lock_acnt(void) {
	pthread_mutex_lock(&aflock);
}

void fs_unlock_acnt(void) {
	pthread_mutex_unlock(&aflock);
}

uint32_t fs_get_acnt(uint32_t inode) {
	aquired_file *afptr;
	for (afptr=afhead ; afptr ; afptr=afptr->next) {
		if (afptr->inode==inode) {
			return (afptr->cnt);
		}
	}
	return 0;
}
*/

void fs_inc_acnt(uint32_t inode) {
	aquired_file *afptr,**afpptr;
	pthread_mutex_lock(&aflock);
	afpptr = &afhead;
	while ((afptr=*afpptr)) {
		if (afptr->inode==inode) {
			afptr->cnt++;
			pthread_mutex_unlock(&aflock);
			return;
		}
		if (afptr->inode>inode) {
			break;
		}
		afpptr = &(afptr->next);
	}
	afptr = (aquired_file*)malloc(sizeof(aquired_file));
	afptr->inode = inode;
	afptr->cnt = 1;
	afptr->next = *afpptr;
	*afpptr = afptr;
	pthread_mutex_unlock(&aflock);
}

void fs_dec_acnt(uint32_t inode) {
	aquired_file *afptr,**afpptr;
	pthread_mutex_lock(&aflock);
	afpptr = &afhead;
	while ((afptr=*afpptr)) {
		if (afptr->inode == inode) {
			if (afptr->cnt<=1) {
				*afpptr = afptr->next;
				free(afptr);
			} else {
				afptr->cnt--;
			}
			pthread_mutex_unlock(&aflock);
			return;
		}
		afpptr = &(afptr->next);
	}
	pthread_mutex_unlock(&aflock);
}

threc* fs_get_my_threc() {
	pthread_t mythid = pthread_self();
	threc *rec;
	pthread_mutex_lock(&reclock);
	for (rec = threchead ; rec ; rec=rec->next) {
		if (pthread_equal(rec->thid,mythid)) {
			pthread_mutex_unlock(&reclock);
			return rec;
		}
	}
	rec = malloc(sizeof(threc));
	rec->thid = mythid;
	if (threchead==NULL) {
		rec->packetid = 1;
	} else {
		rec->packetid = threchead->packetid+1;
	}
	pthread_mutex_init(&(rec->mutex),NULL);
	pthread_cond_init(&(rec->cond),NULL);
	rec->buff = malloc(DEFAULT_BUFFSIZE);
	if (rec->buff==NULL) {
		free(rec);
		pthread_mutex_unlock(&reclock);
		return NULL;
	}
	rec->buffsize = DEFAULT_BUFFSIZE;
	rec->sent = 0;
	rec->status = 0;
	rec->release = 0;
	rec->cmd = 0;
	rec->size = 0;
	rec->next = threchead;
	//syslog(LOG_NOTICE,"mastercomm: create new threc (%"PRIu32")",rec->packetid);
	threchead = rec;
	pthread_mutex_unlock(&reclock);
	return rec;
}

threc* fs_get_threc_by_id(uint32_t packetid) {
	threc *rec;
	pthread_mutex_lock(&reclock);
	for (rec = threchead ; rec ; rec=rec->next) {
		if (rec->packetid==packetid) {
			pthread_mutex_unlock(&reclock);
			return rec;
		}
	}
	pthread_mutex_unlock(&reclock);
	return NULL;
}

void fs_buffer_init(threc *rec,uint32_t size) {
	if (size>DEFAULT_BUFFSIZE) {
		rec->buff = realloc(rec->buff,size);
		rec->buffsize = size;
	} else if (rec->buffsize>DEFAULT_BUFFSIZE) {
		rec->buff = realloc(rec->buff,DEFAULT_BUFFSIZE);
		rec->buffsize = DEFAULT_BUFFSIZE;
	}
}

static uint64_t get_usec_now(){
	struct timeval tv;
	uint64_t usecnow;

	gettimeofday(&tv,NULL);
	usecnow = tv.tv_sec;
	usecnow *= 1000000;
	usecnow += tv.tv_usec;

	return usecnow;
}


master_info* master_select(uint32_t command_info){
	master_info *slave_iterator;
	uint8_t use_master;

	if(slave_selected==NULL){
		return master_head;
	}

	use_master = 1;
	switch(command_info){
		case MATOCU_FUSE_ACCESS:
		case MATOCU_FUSE_LOOKUP:
		case MATOCU_FUSE_GETATTR:
		case MATOCU_FUSE_GETRESERVED:
		case MATOCU_FUSE_GETTRASH:
		case MATOCU_FUSE_GETDETACHEDATTR:
		case MATOCU_FUSE_GETTRASHPATH:
			use_master = 0;
			break;
		case MATOCU_FUSE_SETTRASHPATH:
		case MATOCU_FUSE_UNDEL:
		case MATOCU_FUSE_PURGE:
		case MATOCU_FUSE_TRUNCATE:
		case MATOCU_FUSE_SETATTR:
		case MATOCU_FUSE_READLINK:
		case MATOCU_FUSE_SYMLINK:
		case MATOCU_FUSE_MKNOD:
		case MATOCU_FUSE_MKDIR:
		case MATOCU_FUSE_UNLINK:
		case MATOCU_FUSE_RMDIR:
		case MATOCU_FUSE_RENAME:
		case MATOCU_FUSE_LINK:
		case MATOCU_FUSE_GETDIR:
		case MATOCU_FUSE_OPEN:
		case MATOCU_FUSE_READ_CHUNK:
		case MATOCU_FUSE_WRITE_CHUNK:
		case MATOCU_FUSE_WRITE_CHUNK_END:
		case MATOCU_FUSE_STATFS:
			break;
		default:
			syslog(LOG_WARNING,"unknown mfsmount opt: %"PRIu32, command_info);
			break;
	}

	if(use_master==1){
		pthread_mutex_lock(&write_flag_lock);
		usec_last_write = get_usec_now();
		write_flag = 1;
		pthread_mutex_unlock(&write_flag_lock);
		return master_head;
	}

	if(write_flag==1) {
		return master_head;
	}

	pthread_mutex_lock(&slave_select_lock);
	slave_iterator = slave_selected;
	do{
		slave_iterator = (slave_iterator->next==NULL) ? master_head->next : slave_iterator->next;
		if (slave_iterator->sessionlost) {
			continue;
		}
		if (slave_iterator->fd==-1) {
			continue;
		}
		slave_selected = slave_iterator;
		pthread_mutex_unlock(&slave_select_lock);
		return slave_iterator;
	} while (slave_iterator != slave_selected);
	pthread_mutex_unlock(&slave_select_lock);
	
	return master_head;
}

uint8_t* fs_createpacket(threc *rec,uint32_t cmd,uint32_t size) {
	uint8_t *ptr;
	uint32_t hdrsize = size+4;
	fs_buffer_init(rec,size+12);
	if (rec->buff==NULL) {
		return NULL;
	}
	ptr = rec->buff;
	put32bit(&ptr,cmd);
	put32bit(&ptr,hdrsize);
	put32bit(&ptr,rec->packetid);
	rec->size = size+12;
	return rec->buff+12;
}

const uint8_t* fs_sendandreceive(threc *rec,uint32_t command_info,uint32_t *info_length) {
	uint32_t cnt;
	master_info *master_item;
	uint32_t size = rec->size;

	master_item = master_select(command_info);
//	syslog(LOG_NOTICE,"master_selected: %s, for opt: %"PRIu32,master_item->masterstrip,command_info);
	rec->master_used = master_item;
	for (cnt=0 ; cnt<maxretries ; cnt++) {
		pthread_mutex_lock(&master_item->fdlock);
		if (master_item->sessionlost) {
			pthread_mutex_unlock(&master_item->fdlock);
			return NULL;
		}
		if (master_item->fd==-1) {
			pthread_mutex_unlock(&master_item->fdlock);
			sleep(1+(cnt<30)?(cnt/3):10);
			continue;
		}
		//syslog(LOG_NOTICE,"threc(%"PRIu32") - sending ...",rec->packetid);
		rec->release=0;
		if (tcptowrite(master_item->fd,rec->buff,size,1000)!=(int32_t)(size)) {
			syslog(LOG_WARNING,"tcp send error: %m");
			master_item->disconnect = 1;
			pthread_mutex_unlock(&master_item->fdlock);
			sleep(1+(cnt<30)?(cnt/3):10);
			continue;
		}
		master_stats_add(MASTER_BYTESSENT,size);
		master_stats_inc(MASTER_PACKETSSENT);
		rec->sent = 1;
		master_item->lastwrite = time(NULL);
		pthread_mutex_unlock(&master_item->fdlock);
		// syslog(LOG_NOTICE,"master: lock: %"PRIu32,rec->packetid);
		pthread_mutex_lock(&(rec->mutex));
		while (rec->release==0) {
			rec->waiting=1;
			pthread_cond_wait(&(rec->cond),&(rec->mutex));
		}
		rec->waiting=0;
		pthread_mutex_unlock(&(rec->mutex));
		// syslog(LOG_NOTICE,"master: unlocked: %"PRIu32,rec->packetid);
		// syslog(LOG_NOTICE,"master: command_info: %"PRIu32" ; reccmd: %"PRIu32,command_info,rec->cmd);
		if (rec->status!=0) {
			sleep(1+(cnt<30)?(cnt/3):10);
			continue;
		}
		if (rec->cmd!=command_info) {
			pthread_mutex_lock(&master_item->fdlock);
			master_item->disconnect = 1;
			pthread_mutex_unlock(&master_item->fdlock);
			sleep(1+(cnt<30)?(cnt/3):10);
			continue;
		}
		//syslog(LOG_NOTICE,"threc(%"PRIu32") - received",rec->packetid);
		*info_length = rec->size;
		return rec->buff+size;
	}
	return NULL;
}

/*
int fs_direct_connect() {
	int rfd;
	rfd = tcpsocket();
	if (tcpnumconnect(rfd,masterip,masterport)<0) {
		tcpclose(rfd);
		return -1;
	}
	master_stats_inc(MASTER_TCONNECTS);
	return rfd;
}

void fs_direct_close(int rfd) {
	tcpclose(rfd);
}

int fs_direct_write(int rfd,const uint8_t *buff,uint32_t size) {
	int rsize = tcptowrite(rfd,buff,size,60000);
	if (rsize==(int)size) {
		master_stats_add(MASTER_BYTESSENT,size);
	}
	return rsize;
}

int fs_direct_read(int rfd,uint8_t *buff,uint32_t size) {
	int rsize = tcptoread(rfd,buff,size,60000);
	if (rsize>0) {
		master_stats_add(MASTER_BYTESRCVD,rsize);
	}
	return rsize;
}
*/

void fs_reconnect(master_info *master_item) {
	uint32_t i;
	uint8_t *wptr,regbuff[8+64+9];
	const uint8_t *rptr;
	int newfd;

	if (master_item->sessionid==0) {
		syslog(LOG_WARNING,"can't register: session not created");
		return;
	}
	newfd = tcpsocket();
	if (tcpnodelay(newfd)<0) {
		syslog(LOG_WARNING,"can't set TCP_NODELAY: %m");
	}
	if (srcip>0) {
		if (tcpnumbind(newfd,srcip,0)<0) {
			syslog(LOG_WARNING,"can't bind socket to given ip (\"%s\")",srcstrip);
			tcpclose(newfd);
			master_item->fd=-1;
			return;
		}
	}
	if (tcpnumconnect(newfd,master_item->masterip,master_item->masterport)<0) {
		syslog(LOG_WARNING,"can't connect to master (\"%s\":\"%"PRIu16"\")",master_item->masterstrip,master_item->masterport);
		tcpclose(newfd);
		master_item->fd=-1;
		return;
	}
	master_stats_inc(MASTER_CONNECTS);
	wptr = regbuff;
	put32bit(&wptr,CUTOMA_FUSE_REGISTER);
	put32bit(&wptr,73);
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,REGISTER_RECONNECT);
	put32bit(&wptr,master_item->sessionid);
	put16bit(&wptr,VERSMAJ);
	put8bit(&wptr,VERSMID);
	put8bit(&wptr,VERSMIN);
	if (tcptowrite(newfd,regbuff,8+64+9,1000)!=8+64+9) {
		syslog(LOG_WARNING,"master(%s): register error (write: %m)", master_item->masterstrip);
		tcpclose(newfd);
		master_item->fd=-1;
		return;
	}
	master_stats_add(MASTER_BYTESSENT,16+64);
	master_stats_inc(MASTER_PACKETSSENT);
	if (tcptoread(newfd,regbuff,8,1000)!=8) {
		syslog(LOG_WARNING,"master(%s): register error (read header: %m)", master_item->masterstrip);
		tcpclose(newfd);
		master_item->fd=-1;
		return;
	}
	master_stats_add(MASTER_BYTESRCVD,8);
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCU_FUSE_REGISTER) {
		syslog(LOG_WARNING,"master(%s): register error (bad answer: %"PRIu32")", master_item->masterstrip, i);
		tcpclose(newfd);
		master_item->fd=-1;
		return;
	}
	i = get32bit(&rptr);
	if (i!=1) {
		syslog(LOG_WARNING,"master(%s): register error (bad length: %"PRIu32")", master_item->masterstrip, i);
		tcpclose(newfd);
		master_item->fd=-1;
		return;
	}
	if (tcptoread(newfd,regbuff,i,1000)!=(int32_t)i) {
		syslog(LOG_WARNING,"master(%s): register error (read data: %m)", master_item->masterstrip);
		tcpclose(newfd);
		master_item->fd=-1;
		return;
	}
	master_stats_add(MASTER_BYTESRCVD,i);
	master_stats_inc(MASTER_PACKETSRCVD);
	rptr = regbuff;
	if (rptr[0]!=0) {
		master_item->sessionlost=1;
		syslog(LOG_WARNING,"master(%s): register status: %s", master_item->masterstrip, mfs_strerror(rptr[0]));
		tcpclose(newfd);
		master_item->fd=-1;
		return;
	}
	master_item->fd=newfd;
	master_item->lastwrite=time(NULL);
	syslog(LOG_NOTICE,"registered to master(%s)", master_item->masterstrip);
}

int fs_connect(master_info* master_item,uint8_t meta,const char *info,const char *subfolder,const uint8_t passworddigest[16],uint8_t *sesflags,uint32_t *rootuid,uint32_t *rootgid,uint32_t *mapalluid,uint32_t *mapallgid) {
	uint32_t i;
	uint8_t *wptr,*regbuff;
	md5ctx ctx;
	uint8_t digest[16];
	const uint8_t *rptr;
	uint8_t havepassword;
	uint32_t pleng,ileng;

	havepassword=(passworddigest==NULL)?0:1;
	ileng=strlen(info)+1;
	if (meta) {
		pleng=0;
		regbuff = malloc(8+64+9+ileng+16);
	} else {
		pleng=strlen(subfolder)+1;
		regbuff = malloc(8+64+13+pleng+ileng+16);
	}

	master_item->fd = tcpsocket();
	if (tcpnodelay(master_item->fd)<0) {
//		syslog(LOG_WARNING,"can't set TCP_NODELAY: %m");
		fprintf(stderr,"can't set RCP_NODELAY\n");
	}
	if (srcip>0) {
		if (tcpnumbind(master_item->fd,srcip,0)<0) {
			fprintf(stderr,"can't bind socket to given ip (\"%s\")\n",srcstrip);
			tcpclose(master_item->fd);
			master_item->fd=-1;
			free(regbuff);
			return -1;
		}
	}
	if (tcpnumconnect(master_item->fd,master_item->masterip,master_item->masterport)<0) {
//		syslog(LOG_WARNING,"can't connect to master (\"%s\":\"%"PRIu16"\")",master_item->masterstrip,master_item->masterport);
		fprintf(stderr,"can't connect to mfsmaster (\"%s\":\"%"PRIu16"\")\n",master_item->masterstrip,master_item->masterport);
		tcpclose(master_item->fd);
		master_item->fd=-1;
		free(regbuff);
		return -1;
	}
	if (havepassword) {
		wptr = regbuff;
		put32bit(&wptr,CUTOMA_FUSE_REGISTER);
		put32bit(&wptr,65);
		memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
		wptr+=64;
		put8bit(&wptr,REGISTER_GETRANDOM);
		if (tcptowrite(master_item->fd,regbuff,8+65,1000)!=8+65) {
//			syslog(LOG_WARNING,"master: register error (write: %m)");
			fprintf(stderr,"error sending data to mfsmaster\n");
			tcpclose(master_item->fd);
			master_item->fd=-1;
			free(regbuff);
			return -1;
		}
		if (tcptoread(master_item->fd,regbuff,8,1000)!=8) {
//			syslog(LOG_WARNING,"master: register error (read header: %m)");
			fprintf(stderr,"error receiving data from mfsmaster\n");
			tcpclose(master_item->fd);
			master_item->fd=-1;
			free(regbuff);
			return -1;
		}
		rptr = regbuff;
		i = get32bit(&rptr);
		if (i!=MATOCU_FUSE_REGISTER) {
//			syslog(LOG_WARNING,"master: register error (bad answer: %"PRIu32")",i);
			fprintf(stderr,"got incorrect answer from mfsmaster\n");
			tcpclose(master_item->fd);
			master_item->fd=-1;
			free(regbuff);
			return -1;
		}
		i = get32bit(&rptr);
		if (i!=32) {
			fprintf(stderr,"got incorrect answer from mfsmaster\n");
			tcpclose(master_item->fd);
			master_item->fd=-1;
			free(regbuff);
			return -1;
		}
		if (tcptoread(master_item->fd,regbuff,32,1000)!=32) {
//			syslog(LOG_WARNING,"master: register error (read header: %m)");
			fprintf(stderr,"error receiving data from mfsmaster\n");
			tcpclose(master_item->fd);
			master_item->fd=-1;
			free(regbuff);
			return -1;
		}
//		memcpy(passwordblock+32,passwordblock+16,16);
//		memcpy(passwordblock+16,passworddigest,16);
		md5_init(&ctx);
		md5_update(&ctx,regbuff,16);
		md5_update(&ctx,passworddigest,16);
		md5_update(&ctx,regbuff+16,16);
		md5_final(digest,&ctx);
	}
	wptr = regbuff;
	put32bit(&wptr,CUTOMA_FUSE_REGISTER);
	if (meta) {
		if (havepassword) {
			put32bit(&wptr,64+9+ileng+16);
		} else {
			put32bit(&wptr,64+9+ileng);
		}
	} else {
		if (havepassword) {
			put32bit(&wptr,64+13+ileng+pleng+16);
		} else {
			put32bit(&wptr,64+13+ileng+pleng);
		}
	}
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,(meta)?REGISTER_NEWMETASESSION:REGISTER_NEWSESSION);
	put16bit(&wptr,VERSMAJ);
	put8bit(&wptr,VERSMID);
	put8bit(&wptr,VERSMIN);
	put32bit(&wptr,ileng);
	memcpy(wptr,info,ileng);
	wptr+=ileng;
	if (!meta) {
		put32bit(&wptr,pleng);
		memcpy(wptr,subfolder,pleng);
	}
	if (havepassword) {
		memcpy(wptr+pleng,digest,16);
	}
	if (tcptowrite(master_item->fd,regbuff,8+64+(meta?9:13)+ileng+pleng+(havepassword?16:0),1000)!=(int32_t)(8+64+(meta?9:13)+ileng+pleng+(havepassword?16:0))) {
//		syslog(LOG_WARNING,"master: register error (write: %m)");
		fprintf(stderr,"error sending data to mfsmaster\n");
		tcpclose(master_item->fd);
		master_item->fd=-1;
		free(regbuff);
		return -1;
	}
	if (tcptoread(master_item->fd,regbuff,8,1000)!=8) {
//		syslog(LOG_WARNING,"master: register error (read header: %m)");
		fprintf(stderr,"error receiving data from mfsmaster\n");
		tcpclose(master_item->fd);
		master_item->fd=-1;
		free(regbuff);
		return -1;
	}
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCU_FUSE_REGISTER) {
//		syslog(LOG_WARNING,"master: register error (bad answer: %"PRIu32")",i);
		fprintf(stderr,"got incorrect answer from mfsmaster\n");
		tcpclose(master_item->fd);
		master_item->fd=-1;
		free(regbuff);
		return -1;
	}
	i = get32bit(&rptr);
	if ( !(i==1 || (meta && i==5) || (meta==0 && (i==13 || i==21)))) {
//		syslog(LOG_WARNING,"master: register error (bad length: %"PRIu32")",i);
		fprintf(stderr,"got incorrect answer from mfsmaster\n");
		tcpclose(master_item->fd);
		master_item->fd=-1;
		free(regbuff);
		return -1;
	}
	if (tcptoread(master_item->fd,regbuff,i,1000)!=(int32_t)i) {
//		syslog(LOG_WARNING,"master: register error (read data: %m)");
		fprintf(stderr,"error receiving data from mfsmaster\n");
		tcpclose(master_item->fd);
		master_item->fd=-1;
		free(regbuff);
		return -1;
	}
	rptr = regbuff;
	if (i==1) {
//		syslog(LOG_WARNING,"master: register status: %"PRIu8,rptr[0]);
		fprintf(stderr,"mfsmaster register error: %s\n",mfs_strerror(rptr[0]));
		tcpclose(master_item->fd);
		master_item->fd=-1;
		free(regbuff);
		return -1;
	}
	master_item->sessionid = get32bit(&rptr);
	if (sesflags) {
		*sesflags = get8bit(&rptr);
	} else {
		rptr++;
	}
	if (!meta) {
		if (rootuid) {
			*rootuid = get32bit(&rptr);
		} else {
			rptr+=4;
		}
		if (rootgid) {
			*rootgid = get32bit(&rptr);
		} else {
			rptr+=4;
		}
		if (i==21) {
			if (mapalluid) {
				*mapalluid = get32bit(&rptr);
			} else {
				rptr+=4;
			}
			if (mapallgid) {
				*mapallgid = get32bit(&rptr);
			} else {
				rptr+=4;
			}
		} else {
			if (mapalluid) {
				*mapalluid = 0;
			}
			if (mapallgid) {
				*mapallgid = 0;
			}
		}
	}
	free(regbuff);
	master_item->lastwrite = time(NULL);
//	syslog(LOG_NOTICE,"registered to master");
	return 0;
}


void* fs_nop_thread(void *arg) {
	uint8_t *ptr,hdr[12],*inodespacket;
	int32_t inodesleng;
	aquired_file *afptr;
	int now;
	int lastinodeswrite=0;
	master_info *master_item = (master_info*)arg;
	for (;;) {
		now = time(NULL);
		pthread_mutex_lock(&master_item->fdlock);
		if (master_item->disconnect==0 && master_item->fd>=0) {
			if (master_item->lastwrite+2<now) {	// NOP
				ptr = hdr;
				put32bit(&ptr,ANTOAN_NOP);
				put32bit(&ptr,4);
				put32bit(&ptr,0);
				if (tcptowrite(master_item->fd,hdr,12,1000)!=12) {
					master_item->disconnect=1;
				} else {
					master_stats_add(MASTER_BYTESSENT,12);
					master_stats_inc(MASTER_PACKETSSENT);
				}
				master_item->lastwrite=now;
			}
			if ((master_item==master_head)&&(lastinodeswrite+60<now)) {	// RESERVED INODES
				pthread_mutex_lock(&aflock);
				inodesleng=8;
				for (afptr=afhead ; afptr ; afptr=afptr->next) {
					//syslog(LOG_NOTICE,"reserved inode: %"PRIu32,afptr->inode);
					inodesleng+=4;
				}
				inodespacket = malloc(inodesleng);
				ptr = inodespacket;
				put32bit(&ptr,CUTOMA_FUSE_RESERVED_INODES);
				put32bit(&ptr,inodesleng-8);
				for (afptr=afhead ; afptr ; afptr=afptr->next) {
					put32bit(&ptr,afptr->inode);
				}
				if (tcptowrite(master_item->fd,inodespacket,inodesleng,1000)!=inodesleng) {
					master_item->disconnect=1;
				} else {
					master_stats_add(MASTER_BYTESSENT,inodesleng);
					master_stats_inc(MASTER_PACKETSSENT);
				}
				free(inodespacket);
				pthread_mutex_unlock(&aflock);
				lastinodeswrite=now;
			}
		}
		pthread_mutex_unlock(&master_item->fdlock);
		sleep(1);
	}
}

void* fs_receive_thread(void *arg) {
	const uint8_t *ptr;
	uint8_t hdr[12];
	threc *rec;
	uint32_t cmd,size,packetid;
	int r;

	master_info *master_item = (master_info*)arg;
	for (;;) {
		pthread_mutex_lock(&master_item->fdlock);
		if (master_item->disconnect) {
			tcpclose(master_item->fd);
			// send to any threc status error and unlock them
			pthread_mutex_lock(&reclock);
			for (rec=threchead ; rec ; rec=rec->next) {
				if ((rec->sent)&&(rec->master_used==master_item)) {
					rec->status = 1;
					pthread_mutex_lock(&(rec->mutex));
					rec->release = 1;
					if (rec->waiting) {
						pthread_cond_signal(&(rec->cond));
					}
					pthread_mutex_unlock(&(rec->mutex));
				}
			}
			pthread_mutex_unlock(&reclock);
			master_item->fd=-1;
			master_item->disconnect=0;
		}
		if (master_item->fd==-1) {
			fs_reconnect(master_item);
		}
		if (master_item->fd==-1) {
			pthread_mutex_unlock(&master_item->fdlock);
			sleep(2);	// reconnect every 2 seconds
			continue;
		}
		pthread_mutex_unlock(&master_item->fdlock);
		r = tcptoread(master_item->fd,hdr,12,RECEIVE_TIMEOUT*1000);	// read timeout - 4 seconds
		// syslog(LOG_NOTICE,"master: header size: %d",r);
		if (r==0) {
			syslog(LOG_WARNING,"master(%s): connection lost (1)", master_item->masterstrip);
			master_item->disconnect=1;
			continue;
		}
		if (r!=12) {
			syslog(LOG_WARNING,"master(%s): tcp recv error: %m (1)", master_item->masterstrip);
			master_item->disconnect=1;
			continue;
		}
		master_stats_add(MASTER_BYTESRCVD,12);

		ptr = hdr;
		cmd = get32bit(&ptr);
		size = get32bit(&ptr);
		packetid = get32bit(&ptr);
		if (cmd==ANTOAN_NOP && size==4) {
			// syslog(LOG_NOTICE,"master: got nop");
			master_stats_inc(MASTER_PACKETSRCVD);
			continue;
		}
		if (size<4) {
			syslog(LOG_WARNING,"master(%s): packet too small", master_item->masterstrip);
			master_item->disconnect=1;
			continue;
		}
		size-=4;
		rec = fs_get_threc_by_id(packetid);
		if (rec==NULL) {
			syslog(LOG_WARNING,"master(%s): got unexpected queryid", master_item->masterstrip);
			master_item->disconnect=1;
			continue;
		}
		fs_buffer_init(rec,rec->size+size);
		if (rec->buff==NULL) {
			master_item->disconnect=1;
			continue;
		}
		// syslog(LOG_NOTICE,"master: expected data size: %"PRIu32,size);
		if (size>0) {
			r = tcptoread(master_item->fd,rec->buff+rec->size,size,1000);
			// syslog(LOG_NOTICE,"master: data size: %d",r);
			if (r==0) {
				syslog(LOG_WARNING,"master(%s): connection lost (2)", master_item->masterstrip);
				master_item->disconnect=1;
				continue;
			}
			if (r!=(int32_t)(size)) {
				syslog(LOG_WARNING,"master(%s): tcp recv error: %m (2)", master_item->masterstrip);
				master_item->disconnect=1;
				continue;
			}
			master_stats_add(MASTER_BYTESRCVD,size);
		}
		master_stats_inc(MASTER_PACKETSRCVD);
		rec->sent=0;
		rec->status=0;
		rec->size = size;
		rec->cmd = cmd;
		// syslog(LOG_NOTICE,"master: unlock: %"PRIu32,rec->packetid);
		pthread_mutex_lock(&(rec->mutex));
		rec->release = 1;
		if (rec->waiting) {
			pthread_cond_signal(&(rec->cond));
		}
		pthread_mutex_unlock(&(rec->mutex));
	}
}

void* update_flag_thread(void *arg) {
	uint64_t usecnow;

	for (;;) {
		usleep(100000);

		if (write_flag==0) {
			continue;
		}

		usecnow = get_usec_now();
		if ((usecnow - usec_last_write) < WRITE_EFFECT_USEC){
			continue;
		}

		pthread_mutex_lock(&write_flag_lock);
		if((usecnow - usec_last_write) < WRITE_EFFECT_USEC) {
			pthread_mutex_unlock(&write_flag_lock);
			continue;
		}

		write_flag = 0;
		pthread_mutex_unlock(&write_flag_lock);
	}

}


// called before fork
int fs_init_master_connection(const char *masterhostname,const char *masterportname,const char *bindhost,uint8_t meta,const char *info,const char *subfolder,const uint8_t passworddigest[16],uint8_t *flags,uint32_t *rootuid,uint32_t *rootgid,uint32_t *mapalluid,uint32_t *mapallgid) {
	char *masterhostname_cpy;
	char* master_name_item = NULL;
	master_info *master_last,*master_new;
	int fd_max = -1;
	uint8_t shdw_flags;
	uint32_t shdw_rootuid,shdw_rootgid;
	uint32_t shdw_mapalluid, shdw_mapallgid;

	masterhostname_cpy = (char*)malloc(strlen(masterhostname)+1);
	strcpy(masterhostname_cpy, masterhostname);

	master_statsptr_init();

	if (bindhost) {
		if (tcpresolve(bindhost,NULL,&srcip,NULL,1)<0) {
			fprintf(stderr,"can't resolve source hostname (%s)\n",bindhost);
			return -1;
		}
	} else {
		srcip=0;
	}
	snprintf(srcstrip,17,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,(srcip>>24)&0xFF,(srcip>>16)&0xFF,(srcip>>8)&0xFF,srcip&0xFF);
	srcstrip[16]=0;

	master_last = master_head;
	master_name_item = strtok(masterhostname_cpy, ",");
	while((master_name_item != NULL)&&(master_count < MAX_MASTER_MUN)){
		master_new = (master_info *)malloc(sizeof(master_info));
		memset(master_new,  0,sizeof(master_info));
		if (tcpresolve(master_name_item,masterportname,&master_new->masterip,&master_new->masterport,0)<0) {
			fprintf(stderr,"can't resolve master hostname and/or portname (%s:%s)\n",master_name_item,masterportname);
			master_new->fd = -1;
		}
		snprintf(master_new->masterstrip,17,"%"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8,(master_new->masterip>>24)&0xFF,(master_new->masterip>>16)&0xFF,(master_new->masterip>>8)&0xFF,master_new->masterip&0xFF);
		master_new->masterstrip[16]=0;
		master_new->fd = -1;
		master_new->sessionlost = 0;
		master_new->sessionid = 0;
		master_new->disconnect = 0;
		if(master_head==NULL){
			master_new->fd = fs_connect(master_new,meta,info,subfolder,passworddigest,flags,rootuid,rootgid,mapalluid,mapallgid);
		}
		else{
			master_new->fd = fs_connect(master_new,meta,info,subfolder,passworddigest,&shdw_flags,&shdw_rootuid,&shdw_rootgid,&shdw_mapalluid,&shdw_mapallgid);
		}
		if(master_new->fd > fd_max){
			fd_max = master_new->fd;
		}
		if(master_last==NULL){
			master_head = master_new;
		}
		else{
			master_last->next = master_new;
		}
		master_last = master_new;
		master_last->next = NULL;
		master_count++;
		master_name_item = strtok( NULL, ",");
	}
	slave_selected = master_head->next;
	free(masterhostname_cpy);
	return fd_max;
}

// called after fork
void fs_init_threads(uint32_t retries) {
	pthread_attr_t thattr;
	master_info *master_item;
	maxretries = retries;
	pthread_mutex_init(&reclock,NULL);
	pthread_mutex_init(&aflock,NULL);
	pthread_mutex_init(&slave_select_lock,NULL);
	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr,0x100000);

	master_item = master_head;
	while(master_item!=NULL){
		pthread_mutex_init(&master_item->fdlock,NULL);
		pthread_create(&master_item->rpthid,&thattr,fs_receive_thread,master_item);
		pthread_create(&master_item->npthid,&thattr,fs_nop_thread,master_item);
		master_item = master_item->next;
	}

	if(slave_selected!=NULL){
		pthread_mutex_init(&write_flag_lock,NULL);
		usec_last_write = get_usec_now();
		write_flag = 0;
		pthread_create(&update_flag_pthid,&thattr,update_flag_thread,NULL);
	}

	pthread_attr_destroy(&thattr);
}


void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *reservedspace,uint32_t *inodes) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_STATFS,0);
	if (wptr==NULL) {
		*totalspace = 0;
		*availspace = 0;
		*trashspace = 0;
		*reservedspace = 0;
		*inodes = 0;
		return;
	}
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_STATFS,&i);
	if (rptr==NULL || i!=36) {
		*totalspace = 0;
		*availspace = 0;
		*trashspace = 0;
		*reservedspace = 0;
		*inodes = 0;
	} else {
		*totalspace = get64bit(&rptr);
		*availspace = get64bit(&rptr);
		*trashspace = get64bit(&rptr);
		*reservedspace = get64bit(&rptr);
		*inodes = get32bit(&rptr);
	}
}

uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t modemask) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_ACCESS,13);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,modemask);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_ACCESS,&i);
	if (!rptr || i!=1) {
		ret = ERROR_IO;
	} else {
		ret = rptr[0];
	}
	return ret;
}

uint8_t fs_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_LOOKUP,13+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_LOOKUP,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_GETATTR,12);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_GETATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_setattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_SETATTR,31);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,setmask);
	put16bit(&wptr,attrmode);
	put32bit(&wptr,attruid);
	put32bit(&wptr,attrgid);
	put32bit(&wptr,attratime);
	put32bit(&wptr,attrmtime);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_SETATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_truncate(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint64_t attrlength,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_TRUNCATE,21);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,opened);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put64bit(&wptr,attrlength);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_TRUNCATE,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_readlink(uint32_t inode,const uint8_t **path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_READLINK,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_READLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		pleng = get32bit(&rptr);
		if (i!=4+pleng || pleng==0 || rptr[pleng-1]!=0) {
			pthread_mutex_lock(&rec->master_used->fdlock);
			rec->master_used->disconnect = 1;
			pthread_mutex_unlock(&rec->master_used->fdlock);
			ret = ERROR_IO;
		} else {
			*path = rptr;
			//*path = malloc(pleng);
			//memcpy(*path,ptr,pleng);
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_symlink(uint32_t parent,uint8_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	t32 = strlen((const char *)path)+1;
	wptr = fs_createpacket(rec,CUTOMA_FUSE_SYMLINK,t32+nleng+17);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,t32);
	memcpy(wptr,path,t32);
	wptr+=t32;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_SYMLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_mknod(uint32_t parent,uint8_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_MKNOD,20+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put8bit(&wptr,type);
	put16bit(&wptr,mode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put32bit(&wptr,rdev);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_MKNOD,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_mkdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_MKDIR,15+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put16bit(&wptr,mode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_MKDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_unlink(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_UNLINK,13+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_UNLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_rmdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_RMDIR,13+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_RMDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_rename(uint32_t parent_src,uint8_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_RENAME,18+nleng_src+nleng_dst);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent_src);
	put8bit(&wptr,nleng_src);
	memcpy(wptr,name_src,nleng_src);
	wptr+=nleng_src;
	put32bit(&wptr,parent_dst);
	put8bit(&wptr,nleng_dst);
	memcpy(wptr,name_dst,nleng_dst);
	wptr+=nleng_dst;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_RENAME,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_LINK,17+nleng_dst);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode_src);
	put32bit(&wptr,parent_dst);
	put8bit(&wptr,nleng_dst);
	memcpy(wptr,name_dst,nleng_dst);
	wptr+=nleng_dst;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_LINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdir(uint32_t inode,uint32_t uid,uint32_t gid,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_GETDIR,12);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_GETDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdir_plus(uint32_t inode,uint32_t uid,uint32_t gid,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_GETDIR,13);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,GETDIR_FLAG_WITHATTR);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_GETDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

/*
uint8_t fs_check(uint32_t inode,uint8_t dbuff[22]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint16_t cbuff[11];
	uint8_t copies;
	uint16_t chunks;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_CHECK,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_CHECK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i%3!=0) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		for (copies=0 ; copies<11 ; copies++) {
			cbuff[copies]=0;
		}
		while (i>0) {
			copies = get8bit(&rptr);
			chunks = get16bit(&rptr);
			if (copies<10) {
				cbuff[copies]+=chunks;
			} else {
				cbuff[10]+=chunks;
			}
			i-=3;
		}
		wptr = dbuff;
		for (copies=0 ; copies<11 ; copies++) {
			chunks = cbuff[copies];
			put16bit(&wptr,chunks);
		}
		ret = STATUS_OK;
	}
	return ret;
}
*/
// FUSE - I/O

uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t flags,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_OPEN,13);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,flags);
	fs_inc_acnt(inode);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_OPEN,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		if (attr) {
			memset(attr,0,35);
		}
		ret = rptr[0];
	} else if (i==35) {
		if (attr) {
			memcpy(attr,rptr,35);
		}
		ret = STATUS_OK;
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	if (ret) {	// release on error
		fs_dec_acnt(inode);
	}
	return ret;
}

void fs_release(uint32_t inode) {
	fs_dec_acnt(inode);
}

// release - decrease aquire cnt - if reach 0 send CUTOMA_FUSE_RELEASE
/*
uint8_t fs_release(uint32_t inode) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	ptr = fs_createpacket(rec,CUTOMA_FUSE_RELEASE,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&ptr,inode);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_RELEASE,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}
*/

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint64_t t64;
	uint32_t t32;
	threc *rec = fs_get_my_threc();
	*csdata=NULL;
	*csdatasize=0;
	wptr = fs_createpacket(rec,CUTOMA_FUSE_READ_CHUNK,8);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,indx);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_READ_CHUNK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<20 || ((i-20)%6)!=0) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		t64 = get64bit(&rptr);
		*length = t64;
		t64 = get64bit(&rptr);
		*chunkid = t64;
		t32 = get32bit(&rptr);
		*version = t32;
		if (i>20) {
			*csdata = rptr;
			*csdatasize = i-20;
		}
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint64_t t64;
	uint32_t t32;
	threc *rec = fs_get_my_threc();
	*csdata=NULL;
	*csdatasize=0;
	wptr = fs_createpacket(rec,CUTOMA_FUSE_WRITE_CHUNK,8);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,indx);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_WRITE_CHUNK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<20 || ((i-20)%6)!=0) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		t64 = get64bit(&rptr);
		*length = t64;
		t64 = get64bit(&rptr);
		*chunkid = t64;
		t32 = get32bit(&rptr);
		*version = t32;
		if (i>20) {
			*csdata = rptr;
			*csdatasize = i-20;
		}
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_writeend(uint64_t chunkid, uint32_t inode, uint64_t length) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_WRITE_CHUNK_END,20);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put64bit(&wptr,chunkid);
	put32bit(&wptr,inode);
	put64bit(&wptr,length);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_WRITE_CHUNK_END,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}


// FUSE - META


uint8_t fs_getreserved(const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_GETRESERVED,0);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_GETRESERVED,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrash(const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_GETTRASH,0);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_GETTRASH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_GETDETACHEDATTR,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_GETDETACHEDATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrashpath(uint32_t inode,const uint8_t **path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_GETTRASHPATH,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_GETTRASHPATH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	} else {
		pleng = get32bit(&rptr);
		if (i!=4+pleng || pleng==0 || rptr[pleng-1]!=0) {
			pthread_mutex_lock(&rec->master_used->fdlock);
			rec->master_used->disconnect = 1;
			pthread_mutex_unlock(&rec->master_used->fdlock);
			ret = ERROR_IO;
		} else {
			*path = rptr;
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_settrashpath(uint32_t inode,const uint8_t *path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	t32 = strlen((const char *)path)+1;
	wptr = fs_createpacket(rec,CUTOMA_FUSE_SETTRASHPATH,t32+8);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,t32);
	memcpy(wptr,path,t32);
//	ptr+=t32;
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_SETTRASHPATH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_undel(uint32_t inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_UNDEL,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_UNDEL,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_purge(uint32_t inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_PURGE,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_PURGE,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

/*
uint8_t fs_append(uint32_t inode,uint32_t ainode,uint32_t uid,uint32_t gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CUTOMA_FUSE_APPEND,16);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,ainode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCU_FUSE_APPEND,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		pthread_mutex_lock(&rec->master_used->fdlock);
		rec->master_used->disconnect = 1;
		pthread_mutex_unlock(&rec->master_used->fdlock);
		ret = ERROR_IO;
	}
	return ret;
}
*/
