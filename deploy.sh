echo "This will stop and remove data_engineering_services container, then remove data_engineering_services image, and run"

echo "Stoping docker for data_engineering_services...."
docker stop  data_engineering_services

echo "Removing docker for data_engineering_services...."
docker rm  data_engineering_services

echo "Removing docker image for data_engineering_services...."
docker image rm  data_engineering_services:latest

echo "Building docker image for data_engineering_services...."
docker build -t data_engineering_services:latest -f Dockerfile .

echo "Running docker image for data_engineering_services...."
docker run data_engineering_services -d --name data_engineering_services --restart=always -p 3001:3001