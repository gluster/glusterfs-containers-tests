#! /bin/bash
#
# List of expected input args:
# - $1 is an env dir, i.e '/home/username/.../.tox/ocp3.6'
# - $2 is a tag or PR to checkout from,
#   1) TAG -> i.e. 'openshift-ansible-3.6.173.0.96-1' for OCP v3.6
#   See list of tags here: https://github.com/openshift/openshift-ansible/tags
#   2) PR -> 'pull/12345/head'. Where '12345' is ID of a PR.
#   See list of PRs here: https://github.com/openshift/openshift-ansible/pulls
#   Note that PR is checked out, not cherry-picked.

OPENSHIFT_ANSIBLE_GIT_URL='git://github.com/openshift/openshift-ansible.git'
TARGET_DIR=$1/usr/share/ansible/openshift-ansible
TAG=$2

if [ -z "$TAG" ]; then
    # NOTE(vponomar): get latest tag by 3.X branch
    TAG=$(git ls-remote --tags $OPENSHIFT_ANSIBLE_GIT_URL \
        "refs/tags/openshift-ansible-$(echo $1 | grep -oE '[^tox\/ocp]+$').*" \
        | grep -v "\{\}" | sort -t / -k 3 -V | tail -n 1 | awk '{print $2}' )
    echo "Custom Git tag hasn't been specified, using latest Git tag '$TAG'"
else
    echo "Using custom Git tag '$TAG'"
fi

TAG=${TAG/refs\/tags\//}

if [[ ! -d $TARGET_DIR ]]; then
    mkdir -p $TARGET_DIR
    git clone --single-branch $OPENSHIFT_ANSIBLE_GIT_URL $TARGET_DIR
fi

cd $TARGET_DIR
git fetch origin $TAG
git reset --hard FETCH_HEAD
