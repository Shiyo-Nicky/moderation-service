import { Kafka } from "kafkajs";
import Filter from "bad-words";
import pkg from 'pg';
const { Pool } = pkg;
import dotenv from "dotenv";
dotenv.config();

const filter = new Filter();
filter.addWords('poep', 'poop'); 

const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME
});

const kafka = new Kafka({
  clientId: "moderation-service",
  brokers: [process.env.KAFKA_BROKER || "kafka:9092"]
});
const consumer = kafka.consumer({ groupId: 'moderation-service-group' });
const producer = kafka.producer();
async function startModerationService() {
  await consumer.connect();
  await producer.connect();
  
  await consumer.subscribe({ topic: 'tweet-events', fromBeginning: true });
  
  console.log('Moderation service started. Listening for tweets...');
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        // Process the Kafka event
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
              data: tweet
            };

            // Store moderated event in the event store
            await pool.query(
              `INSERT INTO event_store (event_type, event_data) VALUES ($1, $2)`,
              [moderatedEvent.type, JSON.stringify(moderatedEvent.data)]
            );

            // Send it to Kafka again
            await producer.send({
              topic: "tweet-events",
              messages: [{ value: JSON.stringify(moderatedEvent) }]
            });

            console.log("Tweet moderated and re-published:", tweet.id);
          } else {
            console.log(`No moderation needed for tweet ${tweet.id}`);
          }
        }
      } catch (error) {
        console.error('Error processing message:', error);
      }
    }
  });
}
// Change from export default to module.exports
export default async function (context, kafkaEvent) {
  context.log('Azure Function triggered with Kafka event');
  try {
    await producer.connect();
    
    // Process the Kafka event
    const event = JSON.parse(kafkaEvent);
    
    if (event.type === "TWEET_CREATED") {
    const tweet = event.data;
    const originalContent = tweet.content;
    const censored = filter.clean(originalContent);

    if (censored !== originalContent) {
      tweet.content = censored;

      const moderatedEvent = {
        type: "TWEET_MODERATED",
        data: tweet
      };

      // Store moderated event in the event store (optional)
      await pool.query(
        `INSERT INTO event_store (event_type, event_data) VALUES ($1, $2)`,
        [moderatedEvent.type, moderatedEvent.data]
      );

      // Send it to Kafka again
      await producer.send({
        topic: "tweet-events",
        messages: [{ value: JSON.stringify(moderatedEvent) }]
      });

      context.log("Tweet moderated and re-published:", tweet);
    }
  }
    
    await producer.disconnect();
  } catch (error) {
    context.log.error('Error in Azure Function:', error);
    throw error;
  }
}
if (require.main === module) {
  startModerationService().catch(console.error);
}