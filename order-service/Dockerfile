# Use an official Maven image to build the application
FROM maven:3.8.5-openjdk-17 AS build

# Set the working directory inside the container
WORKDIR /app

# Copy the pom.xml and the source code to the container
COPY pom.xml .
COPY src ./src

# Package the Spring Boot application using Maven
RUN mvn clean package -DskipTests

# Use a JDK image to run the built JAR
FROM openjdk:17-jdk-slim

# Set the working directory inside the container
WORKDIR /order-service

# Copy the JAR file from the build stage to the runtime stage
COPY --from=build /order-service/target/customer-1.0-SNAPSHOT.jar order-service.jar

# Expose the port on which the application will run
EXPOSE 8081

# Run the Spring Boot application
ENTRYPOINT ["java", "-jar", "order-service.jar"]
