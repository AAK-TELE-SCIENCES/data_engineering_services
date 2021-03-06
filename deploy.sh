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
docker run -d data_engineering_services --name data_engineering_services --restart=always -p 3000:3000


docker run <arguments> <image> <command>

docker run -p 5000:5000 -d data_engineering_services