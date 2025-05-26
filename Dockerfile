FROM node:18

WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install

# Copy the rest of the application code
COPY . .

# Set environment variables
ENV NODE_ENV=production
ENV DB_HOST=localhost
ENV DB_USER=nickybreezy
ENV DB_PASS=laura
ENV DB_PORT=5434
ENV DB_NAME=tweets_query
ENV KAFKA_BROKER=kafka:9092

# Command to run the service
CMD ["npm", "start"]