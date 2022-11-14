# udacity-data-engineering-projects
Projects for the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)

See individual directories for the appropriate projects.


# Setup Docker network

    docker network create dbnet    

# Use local postgres container
Stop running container:

    docker kill pgres ; sleep 2; docker rm pgres

Start new container:

    docker run --network dbnet \
    -p5432:5432 \
    --name pgres \
    -e POSTGRES_USER=student \
    -e POSTGRES_PASSWORD=student \
    -e POSTGRES_DB=studentdb \
    -d postgres


# Use local cassandra container

    docker run --network dbnet --name cass_cluster -d cassandra:latest

or pin to a specific version (latest as of this writing, 4.1):

    docker kill cass_cluster ; sleep 2; docker rm cass_cluster ; sleep 1; docker run --network dbnet -p9042:9042 --name cass_cluster -d cassandra:4.1

