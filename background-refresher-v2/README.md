-Dconfig.svc.buckets=gke-stage-fdp-brv2

server ./background-refresher-v2/local/server.yaml

Add a line in file ```/etc/default/cfg-api```
host=10.24.2.28

PUBSUB

add ```superbi.MessageQueue, superbi.topicToInfraConfigRefMap, brv2.pubsubLiteInfraConfig```

in both superbi(prod-fdp-hydra-gcp) & Brv2(gke-prod-fdp-brv2) config bucket

Number of partition should be more than or equal to number of brv2 pods