import { Kafka } from "kafkajs";
import { Filter } from 'bad-words'
import pkg from "pg";
const { Pool } = pkg;
import dotenv from "dotenv";
dotenv.config();

const filter = new Filter();
filter.addWords("poep", "poop");

const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
});

const kafka = new Kafka({
  clientId: "moderation-service",
  brokers: [process.env.KAFKA_BROKER || "kafka:9092"],
   retry: {
    initialRetryTime: 1000,
    retries: 10
  }
});
console.log('Starting moderation service...');
console.log('Connecting to Kafka at:', process.env.KAFKA_BROKER);
console.log('Database connection:', {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT
});
const consumer = kafka.consumer({ groupId: "moderation-service-group" });
const producer = kafka.producer();

async function startModerationService() {
  try {
    console.log('Connecting to Kafka...');
    await consumer.connect();
    await producer.connect();
    
    console.log('Subscribing to tweet-events topic...');
    await consumer.subscribe({ topic: 'tweet-events', fromBeginning: true });
    
    console.log('Moderation service started. Listening for tweets...');
    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          console.log('Received event:', event.type);
          
          if (event.type === "TWEET_CREATED") {
            const tweet = event.data;
            const originalContent = tweet.content;
            const censored = filter.clean(originalContent);

            if (censored !== originalContent) {
              console.log(`Censored tweet ${tweet.id}: "${originalContent}" -> "${censored}"`);
              tweet.content = censored;

              const moderatedEvent = {
                type: "TWEET_MODERATED",
                data: tweet,
                timestamp: Date.now()
              };

              // Store moderated event in the event store
              console.log('Storing moderated event in database...');
              await pool.query(
                `INSERT INTO event_store (event_type, event_data) VALUES ($1, $2)`,
                [moderatedEvent.type, JSON.stringify(moderatedEvent.data)]
              );

              // Send it to Kafka again
              console.log('Publishing moderated event to Kafka...');
              await producer.send({
                topic: "tweet-events",
                messages: [{ value: JSON.stringify(moderatedEvent) }]
              });

              console.log(`Tweet ${tweet.id} was moderated and re-published`);
            } else {
              console.log(`No moderation needed for tweet ${tweet.id}`);
            }
          }
        } catch (error) {
          console.error('Error processing message:', error);
        }
      }
    });
  } catch (error) {
    console.error('Failed to start moderation service:', error);
    // Try to reconnect after delay
    console.log('Attempting to restart in 5 seconds...');
    setTimeout(() => startModerationService(), 5000);
  }
}

// Start the service
console.log('Starting moderation service...');
startModerationService();