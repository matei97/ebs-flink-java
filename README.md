Requirements:
RabbitMQ servers started on ports 5672/5673/5674 with username guest and password guest.
Documentation
https://docs.google.com/document/d/1jjNCsJmUIqf1t_217fsKjdZB7G4B8ErC8bcAieKH1ck
How to run RabbitMQ?
The easiest option is with docker, running the following command:

docker run -d --hostname publisher-rabbit --name publisher-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d --hostname publisher1-rabbit --name publisher1-rabbit -p 5673:5672 -p 15673:15672 rabbitmq:3-management
docker run -d --hostname publisher2-rabbit --name publisher2-rabbit -p 5674:5672 -p 15674:15672 rabbitmq:3-management
In order to see how RabbitMQ is running go to http://localhost:15672/. Use username guest and password guest to log in.

How to run the nodes (Brokers/Publishers/Subscribers)?
To build docker and run the module images:

from /publisher module
- docker build -t ebs/publishers:1.0-SNAPSHOT .
- docker run -d --name publishers -p 8007:8007 ebs/publishers:1.0-SNAPSHOT

from /subscriber module
- docker build -t ebs/subscriber:1.0-SNAPSHOT .
- docker run -d --name subscribers -p 8008:8008 ebs/subscriber:1.0-SNAPSHOT

from /broker module
- docker build -t ebs/broker:1.0-SNAPSHOT .
- docker run -d --name brokers -p 8009:8009 ebs/broker:1.0-SNAPSHOT
