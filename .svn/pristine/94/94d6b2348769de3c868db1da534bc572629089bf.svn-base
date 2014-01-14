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

#ifndef _STATE_H_
#undef _STATE_H_

extern int g_master_state;

enum master_state {
	MFS_STATE_SLAVE    = 0,
	MFS_STATE_MASTER ,
	MFS_STATE_SHUTDOWN,
};

inline int ismaster();
inline int isslave();
inline int isshutdown();
inline void set_state(int state);
int state_init();

#endif
