FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline --no-transfer-progress -q
COPY src ./src
RUN mvn package -DskipTests --no-transfer-progress -q

FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
RUN apk add --no-cache libstdc++ \
    && addgroup -S opsd && adduser -S opsd -G opsd \
    && mkdir -p /var/lib/kafka-streams \
    && chown opsd:opsd /var/lib/kafka-streams
COPY --from=build /build/target/detector-streams-1.0.0.jar detector-streams.jar
USER opsd
ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=75.0", "-jar", "detector-streams.jar"]
