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

#ifndef _MATOSLASERV_H_
#define _MATOSLASERV_H_

#include <stdio.h>
#include <inttypes.h>

#include "main.h"

uint32_t matoslaserv_mloglist_size(void);
void matoslaserv_worker_write(serventry *eptr);
void matosla_sync_thread(serventry *eptr,uint8_t flag);
void matoslaserv_download_end(serventry *eptr,const uint8_t *data,uint32_t length);
int matoslaserv_get_log(serventry *eptr,char *work_buff);
int matoslaserv_send_log(serventry *eptr,uint8_t log_count);
int matoslaserv_pack_log(serventry *eptr,int flag);
void matoslaserv_changelog_start(serventry *eptr,const uint8_t *data,uint32_t length);
int matoslaserv_changelog(serventry *eptr,const uint8_t *data,uint32_t length);
void matoslaserv_gotpacket(serventry *eptr,uint32_t type,const uint8_t *data,uint32_t length);
uint8_t* matoslaserv_createpacket(serventry *eptr,uint32_t type,uint32_t size);
uint32_t matoslaserv_slaoglist_size(void);
void matoslaserv_slaoglist_data(uint8_t *ptr);
void matoslaserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize);
void matoslaserv_logrotate(serventry *eptr);
int matoslaserv_init();
#endif
