# AirflowBasics

To launch, navigate to the directory in Terminal and run the command

```
docker-compose up
```

This will launch Airflow locally on port 8080 (can be changed in the `.yml` file to another port.

Similarly, you can remove the docker resources by using

```
docker-compose down
```

if you need to reclaim the port. Otherwise, you can use start/stop commands instead of up/down to halt or initiate services without removing or adding the containers
