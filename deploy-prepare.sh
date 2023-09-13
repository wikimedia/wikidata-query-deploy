#!/bin/bash
set -e

# print error line

err_report() {
    echo "Error on line $1"
}

trap 'err_report $LINENO' ERR

CURRENT_VERSION_NUMBER=${1?Need the current version number in format x.y.z}

echo "ensure git fat is activated"
git fat init

echo "bail if there are uncommitted changes"
git diff --exit-code --quiet
git diff --cached --exit-code --quiet

echo "remove all files except dotfiles, this script, README, scap directory and files required for deployment"
shopt -s extglob
rm -rv !(.git*|scap|flink|deploy-prepare.sh|README|agents.txt|patterns.txt|RWStore.categories.properties)
shopt -u extglob

echo "downloading latest tar file"
curl -o service-${CURRENT_VERSION_NUMBER}-dist.tar.gz --fail \
 https://archiva.wikimedia.org/repository/releases/org/wikidata/query/rdf/service/${CURRENT_VERSION_NUMBER}/service-${CURRENT_VERSION_NUMBER}-dist.tar.gz

echo "extracting into current directory"
tar --strip-components=1 -xvf service-${CURRENT_VERSION_NUMBER}-dist.tar.gz

echo "removing tar file"
rm -rf service-${CURRENT_VERSION_NUMBER}-dist.tar.gz

echo "creating commit"
git add .
git commit -m "deploy version ${CURRENT_VERSION_NUMBER}"
