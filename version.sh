#!/bin/bash
svn_version=`svn info| sed -n '5p' | awk '{print $2}'`
echo "#define SVNVER $svn_version" > mfscommon/version.h
