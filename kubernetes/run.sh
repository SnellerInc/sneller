#!/bin/bash
CLUSTER_NAME=sneller-k8s
CLUSTER_REGION=eu-west-2

# Create the EKS cluster
echo "Create the EKS cluster (this may take up to 20 minutes)"
eksctl create cluster --region $CLUSTER_REGION --name $CLUSTER_NAME --nodegroup-name 'ng-1' --nodes 4 --node-type 'm5.2xlarge'

# Create the IAM service account
AWS_ACCOUNT_ID=`aws sts get-caller-identity --query "Account" --output text`
curl -o iam_policy.json https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/v2.4.0/docs/install/iam_policy.json
aws iam create-policy --policy-name AWSLoadBalancerControllerIAMPolicy --policy-document file://iam_policy.json
eksctl utils associate-iam-oidc-provider --region $CLUSTER_REGION --cluster $CLUSTER_NAME --approve
eksctl create iamserviceaccount --region $CLUSTER_REGION --cluster $CLUSTER_NAME --namespace=kube-system --name=aws-load-balancer-controller --attach-policy-arn=arn:aws:iam::$AWS_ACCOUNT_ID:policy/AWSLoadBalancerControllerIAMPolicy --override-existing-serviceaccounts --approve

# Create K8s namespace
K8S_NAMESPACE=sneller
kubectl create namespace $K8S_NAMESPACE

# Install NGINX and cert-manager
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install --namespace $K8S_NAMESPACE -f values-nginx.yaml nginx-sneller bitnami/nginx-ingress-controller
helm install --namespace $K8S_NAMESPACE --set installCRDs=true certmanager-sneller bitnami/cert-manager
kubectl --namespace $K8S_NAMESPACE apply -f issuer.yaml

# Install Minio
ACCESS_KEY_ID=AKI`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-20} | head -n 1`
SECRET_ACCESS_KEY=SAK`cat /dev/urandom | tr -dc '[:alpha:]' | fold -w ${1:-40} | head -n 1`
helm repo add minio https://charts.min.io/
helm install --namespace $K8S_NAMESPACE -f values-minio.yaml --set rootUser=$ACCESS_KEY_ID,rootPassword=$SECRET_ACCESS_KEY minio-sneller minio/minio

# Re-read Minio keys
ACCESS_KEY_ID=$(kubectl --namespace $K8S_NAMESPACE get secret minio-sneller -o jsonpath='{.data.rootUser}' | base64 -d)
SECRET_ACCESS_KEY=$(kubectl --namespace $K8S_NAMESPACE get secret minio-sneller -o jsonpath='{.data.rootPassword}' | base64 -d)

# Install Sneller
(cd helm; helm package .)
helm install --namespace $K8S_NAMESPACE -f values-sneller.yaml --set secrets.s3.values.awsAccessKeyId=$ACCESS_KEY_ID,secrets.s3.values.awsSecretAccessKey=$SECRET_ACCESS_KEY sneller ./helm/sneller-0.0.1-unofficial.tgz

# Wait until
echo "Make sure you continue when the certificates are ready (you may need to alter your Route53 config)"
read

# Setup Minio bucket
AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY aws --endpoint https://kubernetes-minio.sneller-dev.io s3 mb s3://test

# Add data
TEMPFILE=$(mktemp)
curl --output $TEMPFILE 'https://sneller-example-data.s3.amazonaws.com/docker/sf1/customer.json'
AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY aws --endpoint https://kubernetes-minio.sneller-dev.io s3 cp $TEMPFILE s3://test/sf1/customer.json
echo '{"name": "customer", "input": [{"pattern": "s3://test/sf1/*.json","format": "json"}]}' > $TEMPFILE
AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY aws --endpoint https://kubernetes-minio.sneller-dev.io s3 cp $TEMPFILE s3://test/db/sf1/customer/definition.json
rm $TEMPFILE

# Wait until SDB has ran at least once
echo "Make sure SDB has ran"
read

SNELLER_TOKEN=$(kubectl --namespace $K8S_NAMESPACE get secret sneller-token -o jsonpath='{.data.snellerToken}' | base64 -d)
curl -G -H "Authorization: Bearer $SNELLER_TOKEN" \
    --data-urlencode "database=sf1" \
    --data-urlencode 'json' \
    --data-urlencode 'query=SELECT COUNT(*) FROM customer' \
    https://kubernetes-sneller.sneller-dev.io/executeQuery