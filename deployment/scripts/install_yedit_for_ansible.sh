#! /bin/bash
#
# List of expected input args:
# - $1 is an env dir, i.e '/home/username/.../.tox/ocp3.6'
# - $2 is a tag or branch name to checkout from.

YEDIT_GIT_URL='git://github.com/vponomaryov/yedit.git'
TARGET_DIR=$1/src/yedit

if [[ ! -d $TARGET_DIR ]]; then
    mkdir -p $TARGET_DIR
    git clone $YEDIT_GIT_URL --single-branch --branch $2 $TARGET_DIR
else
    cd $TARGET_DIR
    git fetch -t --all
    git reset --hard $2
fi
