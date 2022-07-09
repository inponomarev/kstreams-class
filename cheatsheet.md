## Command Line Cheat Sheet

* List of running containers

```
docker ps -a
```

* Cleaning stopped containers and volumes (recommended before starting the second lesson)


```
docker container prune
docker volume prune
```

* Launching a mini Kafka cluster (from the project root folder)

```
docker-compose up
```

* Log in to a container with kcat installed


```
docker exec -it kafkacat /bin/bash
```

## kcat cheat sheet

* Topics list

```
kcat -L -b broker:29092
```

* Reading data from topic in a consumer group

```
kcat -C -b broker:29092 -G <group name> <topic name>
```

* Reading data from topic with output formatting 

```
kcat -q -C -b broker:29092 -t <topic name> -f '%k;%p;%s\n'
```

 * `%k` — key
 * `%p` — partition number
 * `%s` — value (deserialized as a string)
 * `%S` — message length in bytes

Do not forget `\n`, otherwise everything will be dumped into one line!
