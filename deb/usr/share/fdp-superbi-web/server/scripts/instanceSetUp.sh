#!/usr/bin/env bash

echo "deb http://10.47.4.220:80/repos/infra-cli/3 /" > /etc/apt/sources.list.d/infra-cli-svc.list
echo "10.84.182.81 d42-a-0002.nm.flipkart.com" >> /etc/hosts
echo "10.85.51.142 repo-svc-app-0001.nm.flipkart.com" >> /etc/hosts

APP=fdp-superbi-web
repo_service_host=repo-svc-app-0001.nm.flipkart.com
repo_service_port=8080

#AppId should start with prod (prd-fdp-qaas) or stage (stage-fdp-qaas)
tmpEnv=$(echo `hostname` | cut -d'-' -f1)

if [ "$tmpEnv" = "prod" ]; then
        ENV=prod
else
        ENV=stage
fi

REPO_KEY=$ENV-$APP

echo "env="$ENV >> /etc/default/$APP

sudo apt-get update

sudo apt-get install --yes --allow-unauthenticated infra-cli

sudo reposervice --host $repo_service_host --port $repo_service_port env --name $REPO_KEY --appkey $REPO_KEY > /etc/apt/sources.list.d/$REPO_KEY.list

sudo apt-get update