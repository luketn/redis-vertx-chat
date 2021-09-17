ARG BUILD_IMAGE=maven:3.5-jdk-11
ARG RUNTIME_IMAGE=openjdk:11-jdk-slim

#############################################################################################
###                Stage where Docker is pulling all maven dependencies                   ###
#############################################################################################
FROM ${BUILD_IMAGE} as dependencies
COPY pom.xml ./
RUN mvn -B dependency:go-offline
#############################################################################################

#############################################################################################
###              Stage where Docker is building Vert.x app using maven                    ###
#############################################################################################
FROM dependencies as build
COPY assembly.xml ./
COPY src ./src
RUN mvn -B clean package
#############################################################################################

#############################################################################################
### Stage where Docker is running a java process to run a service built in previous stage ###
#############################################################################################
FROM ${RUNTIME_IMAGE}
COPY --from=build target/redis-vertx-chat/redis-vertx-chat/lib/ /app/lib/
COPY --from=build target/redis-vertx-chat/redis-vertx-chat/redis-vertx-chat.jar /app/redis-vertx-chat.jar
WORKDIR /app
CMD ["java", "-jar", "redis-vertx-chat.jar"]
#############################################################################################