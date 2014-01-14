noinst_PROGRAMS=test_matocsserv

LDADD=/usr/local/lib/libcunit.a
AM_CPPFLAGS=-lpthread -std=c99 -I$(top_srcdir)/mfscommon -I$(top_srcdir)/mfsmaster -I/usr/local/include -DAPPNAME=mfsmaster -DUNITTEST
AM_LDFLAGS=-lpthread $(PTHREAD_LIBS) $(ZLIB_LIBS) -lcunit -all-static

test_matocsserv_SOURCES=\
	run_test.c \
	test_matocsserv.c \
	../mfsmaster/acl.h ../mfsmaster/acl.c \
	../mfsmaster/changelog.c ../mfsmaster/changelog.h \
	../mfsmaster/chunks.c ../mfsmaster/chunks.h \
	../mfsmaster/filesystem.c ../mfsmaster/filesystem.h \
	../mfsmaster/matocsserv.c ../mfsmaster/matocsserv.h \
	../mfsmaster/matocuserv.c ../mfsmaster/matocuserv.h \
	../mfsmaster/matomlserv.c ../mfsmaster/matomlserv.h \
	../mfsmaster/random.c ../mfsmaster/random.h \
	../mfsmaster/datacachemgr.c ../mfsmaster/datacachemgr.h \
	../mfsmaster/chartsdata.c ../mfsmaster/chartsdata.h \
	../mfsmaster/nettopology.c ../mfsmaster/nettopology.h \
	../mfsmaster/masterconn.c ../mfsmaster/masterconn.h \
	../mfsmaster/matoslaserv.c ../mfsmaster/matoslaserv.h \
	../mfsmaster/replay.c ../mfsmaster/replay.h \
	../mfsmaster/state.c ../mfsmaster/state.h \
	../mfsmaster/init.h \
	../mfscommon/main.c ../mfscommon/main.h \
	../mfscommon/cfg.c ../mfscommon/cfg.h \
	../mfscommon/md5.c ../mfscommon/md5.h \
	../mfscommon/crc.c ../mfscommon/crc.h \
	../mfscommon/sockets.c ../mfscommon/sockets.h \
	../mfscommon/charts.c ../mfscommon/charts.h \
	../mfscommon/datapack.h \
	../mfscommon/MFSCommunication.h
