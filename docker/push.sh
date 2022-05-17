for image in snellerd sdb
do
  docker tag snellerinc/$image:latest
  docker push snellerinc/$image:latest
done
