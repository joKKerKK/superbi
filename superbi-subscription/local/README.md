Lets setup local kafka if not done already


https://www.baeldung.com/ops/kafka-docker-setup#single-node-cluster
```shell script
docker-compose up -d

# to uninstall
docker-compose down 
```

1) Create couchbase buckets and Kafka Topics

To build docker image and push it to local registry
```shell script
cd local
sh build_and_push_to_local 
```

Let's create Namespace `fdp-superbi-subscription` and ServiceAccount `superbi-subscription-ci`in Kubernetes
```shell script
cd local
kubectl apply -f ./../k8s/namespace.yaml
kubectl apply -f ./../k8s/service-account.yaml 
```
Now install Helm-Cart with name `fdp-superbi-subscription` in namespace `fdp-superbi-subscription` from local chart definitions
```shell script
helm install fdp-superbi-subscription -n fdp-superbi-subscription -f override_values.yaml ./../k8s/helm-chart/service/
```
You can use `--dry-run` just to check the YAML content being applied
```shell script
helm install fdp-superbi-subscription -n fdp-superbi-subscription -f override_values.yaml ./../k8s/helm-chart/service/ --dry-run
```
To uninstall
```shell script
helm uninstall fdp-superbi-subscription -n fdp-superbi-subscription
kubectl delete -f ./../k8s/namespace.yaml
```

If not using the Ingress, then access the service outside cluster using `port-forward`
```hell script
kubectl --namespace fdp-superbi-subscription port-forward service/fdp-superbi-subscription 8080:21212
```
Access http://localhost:8080 in host machine
