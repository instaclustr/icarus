BACKUP_RESTORE_DIR:=$(shell dirname $(realpath .))/cassandra-backup

.PHONY: all
all: docker

# Build this Sidecar via Docker
.PHONY: docker
docker:
	./docker-build.sh $(BACKUP_RESTORE_DIR) $(HOME)/.m2 $(CURDIR)

.DEFAULT_GOAL := all
