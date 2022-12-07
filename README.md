## Create a Docker Compose YAML File for a PostgreSQL Docker Container
Let’s create a directory, postgres, and then create a docker-compose.yml file in that directory:

```bash
mkdir postgres
cd postgres
touch docker-compose.yml
```

Basically, here, we will specify the services we are going to use and set up the environment variables related to those.

We will change this file multiple times throughout this article.

Add the following in the docker-compose.yml file we just created:

![Screenshot 2022-12-07 at 19 03 49](https://user-images.githubusercontent.com/113560228/206274280-2977147a-f8db-4909-b5b2-c7adf4bf0961.png)

We specified the name of our PostgreSQL container as demo and the Docker image to be used is postgres.

The next thing we need to specify is the environment variables, i.e. the user, password, and database. If you don’t specify the user, by default it will be <b>root</b>.

Now, after creating the .yml file, we need to run the following command in the same directory where the .yml file is located:

```bash
docker-compose up
```

This will pull the Docker image (if the image is not available locally, it will pull from Docker Hub) and then run the container.

We can check the status with:

```bash
docker-compose ps
```
![Screenshot 2022-12-07 at 19 31 47](https://user-images.githubusercontent.com/113560228/206277976-688302ec-2713-435a-9d34-8f911ed71819.png)

This will show the name of the container, command, and state of the container, which shows, for example, that the container is running. It also shows port mapping.

## Connect to the PostgreSQL Database Running in a Container

Now, we can go to our browser and go to localhost:8080 for Adminer. As Adminer runs on the same Docker network as PostgreSQL, it can access the Postgres container via port 5432 (or simply, by the container’s name).

![Screenshot 2022-12-07 at 19 41 37](https://user-images.githubusercontent.com/113560228/206280979-f1cfb886-e6a9-458e-abff-f5fd53bb4f9c.png)
