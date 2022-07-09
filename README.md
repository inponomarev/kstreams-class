# Kafka Streams Class

[![Actions Status: build](https://github.com/inponomarev/kstreams-class/workflows/build/badge.svg)](https://github.com/inponomarev/kstreams-class/actions?query=workflow%3A"build")

Preparation for Kafka Streams API hands-on practice lab

* [Lessons plan](plan.md)

* [Command Line cheat sheet](cheatsheet.md)

* [Home assignment](homework.md)

## Instructions for preparing for the lesson

In order not to waste precious time during the training on downloading hundreds of megabytes from the Internet and solving technical problems, please carefully read and fully follow this instruction _before the training starts_.

Your machine should have:
* Webcam and microphone (participation in the training takes place with the webcam turned on and will require periodic screen sharing!) 
* Docker,
* Java 8/11,
* Maven,
* IntelliJ IDEA (with Lombok plugin).

You may use a more recent version of Java or other IDE (more familiar to you), but with no guarantee that I can help in case of problems. 

1. Clone the project https://github.com/inponomarev/kstreams-class to your local drive

2. Run `docker-compose up` at the root of the project. This command will download the necessary Docker images and start a mini Kafka cluster on your machine.

3. Check the health of Kafka and kcat. To do this, log into the docker container

```
docker exec -it kafkacat /bin/bash
```

and run the command

```
kcat -L -b broker:29092
```

If you see text like "Metadata for all topics... broker1 at localhost:9092", then everything is fine with Kafka and kcat running on your machine!

4. Run `mvn clean install` at the root of the project. This command will download and cache the required libraries. The build should succeed!

5. Open the project in IntelliJ IDEA. If necessary, install the Lombok plugin (Shift-Shift, Plugins, find and install the Lombok plugin). Once the import and indexing is complete, there should be no "red underlined" code.

6. Make sure you can run the Producer module (as in the screenshot) and it starts producing data into your locally deployed Kafka (below, the log runs as in the screenshot):

<img align="left" src="producer.png" width="400px" alt="producer screenshot"> 


Congratulations, you are now fully prepared for the training!

