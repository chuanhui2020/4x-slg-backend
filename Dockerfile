FROM eclipse-temurin:25-jdk-alpine AS build

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN apk add --no-cache maven \
    && mvn package -DskipTests -q \
    && mv target/*.jar app.jar

FROM eclipse-temurin:25-jre-alpine

WORKDIR /app
COPY --from=build /app/app.jar .

ENV SERVER_ID=1 \
    SERVER_ROLE=master \
    POD_ID=local \
    JAVA_OPTS="-XX:+UseZGC -XX:MaxRAMPercentage=75.0"

EXPOSE 8080 9090

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
