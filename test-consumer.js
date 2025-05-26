const { Kafka } = require('kafkajs');
const Filter = require('bad-words');
const dotenv = require('dotenv');
dotenv.config();

const filter = new Filter();
filter.addWords('poep', 'poop');

const kafka = new Kafka({
  clientId: 'test-consumer',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'test-moderation-group' });
const producer = kafka.producer();

async function start() {
  await consumer.connect();
  await producer.connect();
  
  await consumer.subscribe({ topic: 'tweets', fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      console.log('Received event:', event);
      
      if (event.type === 'TWEET_CREATED') {
        const tweet = event.data;
        const originalContent = tweet.content;
        const censored = filter.clean(originalContent);
        
        if (censored !== originalContent) {
          console.log(`Censored: "${originalContent}" -> "${censored}"`);
          
          tweet.content = censored;
          const moderatedEvent = {
            type: 'TWEET_MODERATED',
            data: tweet
          };
          
          await producer.send({
            topic: 'tweets',
            messages: [{ value: JSON.stringify(moderatedEvent) }]
          });
          
          console.log('Published moderated event:', moderatedEvent);
        } else {
          console.log('No moderation needed');
        }
      }
    },
  });
}

start().catch(console.error);