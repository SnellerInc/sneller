## Install Minio
Run the following commands:
```
helm repo add minio https://charts.min.io/
helm install --namespace minio --set rootUser=rootuser,rootPassword=rootpass123 minio-snellerd minio/minio
```