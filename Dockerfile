FROM openjdk:8-jre-alpine
COPY newsriver-mill-*.jar /home/newsriver-mill.jar
WORKDIR /home
EXPOSE 31000-32000
ENV PORT 31113
ENTRYPOINT ["java","-Duser.timezone=GMT","-Dfile.encoding=utf-8","-Xms512m","-Xmx1g","-Xss1m","-XX:MaxMetaspaceSize=512m","-XX:+UseConcMarkSweepGC","-XX:+CMSParallelRemarkEnabled","-XX:+UseCMSInitiatingOccupancyOnly","-XX:CMSInitiatingOccupancyFraction=70","-XX:OnOutOfMemoryError='kill -9 %p'","-jar","/home/newsriver-mill.jar"]
