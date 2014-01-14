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

#ifndef _MATOMLSERV_H_
#define _MATOMLSERV_H_

#include <stdio.h>
#include <inttypes.h>
#include "main.h"

void matomlserv_worker_read(serventry *eptr);
void matomlserv_worker_write(serventry *eptr);
char* matomlserv_makestrip(uint32_t ip);
void matoml_sync_thread(serventry *eptr,uint8_t flag);
void matomlserv_download_end(serventry *eptr,const uint8_t *data,uint32_t length);
int matomlserv_get_log(serventry *eptr,char *work_buff);
int matomlserv_send_log(serventry *eptr,uint8_t log_count);
int matomlserv_pack_log(serventry *eptr,int flag);
void matomlserv_changelog_start(serventry *eptr,const uint8_t *data,uint32_t length);
int matomlserv_changelog(serventry *eptr,const uint8_t *data,uint32_t length);
void matomlserv_gotpacket(serventry *eptr,uint32_t type,const uint8_t *data,uint32_t length);
uint8_t* matomlserv_createpacket(serventry *eptr,uint32_t type,uint32_t size);
uint32_t matomlserv_mloglist_size(void);
void matomlserv_mloglist_data(uint8_t *ptr);
void matomlserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize);
void matomlserv_logrotate(serventry *eptr);
int matomlserv_init();

#endif
