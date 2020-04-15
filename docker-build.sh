#!/usr/bin/env sh

CASSANDRA_BACKUP_RESTORE_REPO=$1
MAVEN_REPOSITORY=$2
THIS_REPOSITORY=$3

docker run --rm \
  -v $THIS_REPOSITORY:/repo \
  -v $CASSANDRA_BACKUP_RESTORE_REPO:/backup-restore \
  -v $MAVEN_REPOSITORY:/root/.m2 \
  gcr.io/cassandra-operator/jdk8-gcloud-sdk:2.0.2 \
  /bin/bash -c "(cd /backup-restore && mvn clean install -DskipTests) && (cd /repo && mvn clean install -DskipTests)"
