if BUILD_MASTER
MASTERDIR=mfsmaster mfsmetarestore mfsmetadump 
else
MASTERDIR=
endif

if BUILD_SHADOWMASTER
SHADOWMASTER=mfsshadowmaster
else
SHADOWMASTER=
endif

if BUILD_CHUNKSERVER
CHUNKSERVERDIR=mfschunkserver
else
CHUNKSERVERDIR=
endif

if BUILD_MOUNT
MOUNTDIR=mfsmount
else
MOUNTDIR=
endif

if BUILD_TEST
TESTDIR=mfstest mfsmaster
else
TESTDIR=
endif

if INSTALL_CGI
CGISUBDIR=mfscgi
else
CGISUBDIR=
endif

ACLOCAL_AMFLAGS = -I m4 $(shell test -d /opt/local/share/aclocal && echo -I/opt/local/share/aclocal)

SUBDIRS=mfsdata doc $(MASTERDIR) $(SHADOWMASTER) $(CHUNKSERVERDIR) $(MOUNTDIR) $(TESTDIR) $(CGISUBDIR)

EXTRA_DIST=UPGRADE m4/ax_pthread.m4
