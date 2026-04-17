#!/usr/bin/env bash
# Environment variables for local development.
#   source deploy/scripts/dev-env.sh
# Matches docker-compose.yml in deploy/docker/.

export OPENTRADE_ENV="dev"

export OPENTRADE_KAFKA_BROKERS="localhost:9092"
export OPENTRADE_ETCD_ENDPOINTS="http://localhost:2379"
export OPENTRADE_MYSQL_DSN="opentrade:opentrade@tcp(localhost:3306)/opentrade?parseTime=true&multiStatements=true"

export OPENTRADE_S3_ENDPOINT="http://localhost:9000"
export OPENTRADE_S3_ACCESS_KEY="minioadmin"
export OPENTRADE_S3_SECRET_KEY="minioadmin"
export OPENTRADE_S3_BUCKET="opentrade-snapshots"

echo "opentrade dev env loaded:"
env | grep ^OPENTRADE_ | sort
