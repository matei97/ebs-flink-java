FROM openjdk:14
 
COPY target/subscriber-1.0-SNAPSHOT-shaded.jar /instance.jar
 
ADD start.sh .

CMD ["bash", "start.sh"]