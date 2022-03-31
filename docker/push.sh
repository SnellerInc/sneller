REPO=671229366946.dkr.ecr.us-east-1.amazonaws.com
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $REPO
for image in snellerd sdb
do
  docker tag sneller/$image:latest $REPO/sneller/$image:latest
  docker push $REPO/sneller/$image:latest
done
