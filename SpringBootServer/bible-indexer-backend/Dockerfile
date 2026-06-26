# Use official Java base image
FROM eclipse-temurin:17-jdk

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Build the app
RUN ./mvnw clean package -DskipTests

# Expose Render's port
EXPOSE 8080

# Run the jar
CMD ["sh", "-c", "java -jar target/*.jar"]