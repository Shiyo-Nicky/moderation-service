import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
dotenv.config();

// Create a Kafka instance with explicit, single broker configuration
const kafka = new Kafka({
  clientId: 'test-producer',
  brokers: ['localhost:9092'],
  connectionTimeout: 3000,  // Lower timeout for faster testing
  retry: {
    initialRetryTime: 100,
    retries: 3
  }
});

const producer = kafka.producer();
const admin = kafka.admin();

async function sendTestTweet() {
  try {
    console.log('Connecting to Kafka broker at localhost:9092...');
    await admin.connect();
    await producer.connect();
    
    console.log('Connected successfully! Checking for tweets topic...');
    const topics = await admin.listTopics();
    
    if (!topics.includes('tweets')) {
      console.log('Creating tweets topic...');
      await admin.createTopics({
        topics: [{ topic: 'tweets', numPartitions: 1, replicationFactor: 1 }],
      });
      console.log('Topic created! Waiting for it to become ready...');
      await new Promise(resolve => setTimeout(resolve, 3000));
    } else {
      console.log('Topic "tweets" already exists.');
    }
    
    const testTweet = {
      type: 'TWEET_CREATED',
      data: {
        id: 999,
        user_id: 1,
        content: 'This is a test tweet with poop words',
        created_at: new Date().toISOString()
      }
    };

    console.log('Sending test tweet...');
    await producer.send({
      topic: 'tweets',
      messages: [{ value: JSON.stringify(testTweet) }]
    });

    console.log('✅ Test tweet sent successfully:', testTweet);
  } catch (err) {
    console.error('❌ Error:', err.message);
    if (err.stack) console.error(err.stack.split('\n')[0]);
  } finally {
    console.log('Disconnecting from Kafka...');
    await producer.disconnect();
    await admin.disconnect();
    console.log('Disconnected.');
  }
}

// Execute the function
sendTestTweet();