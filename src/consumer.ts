import { Kafka } from "kafkajs";
import { v4 as uuidv4 } from "uuid";

const redpanda = new Kafka({
  brokers: ["localhost:19092"],
});

const MAX_RETRY_ATTEMPTS = 10; // Maximum number of retry attempts


const consumer = redpanda.consumer({ groupId: '3ab53af2-06c8-458a-936d-b2dab16d6d44' });
// const consumer2 = redpanda.consumer({ groupId: uuidv4() });
// const consumer3 = redpanda.consumer({ groupId: uuidv4() });
// const consumer4 = redpanda.consumer({ groupId: uuidv4() });
(async () => {  
  try {
    await consumer.connect();
    console.log("Connected to Kafka!");

    await consumer.subscribe({ topic: "testtopic" });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const formattedValue = JSON.parse((message.value as Buffer).toString());
        console.log(`${formattedValue.user}: ${formattedValue.message}`);
      },
    });
  } catch (error) {
    console.error("Error:", error); 
  }
})(); 

// (async () => {  
//   try {
//     await consumer2.connect();
//     console.log("Connected to Kafka!");

//     await consumer2.subscribe({ topic: "testtopic" });
//     await consumer2.run({
//       eachMessage: async ({ topic, partition, message }) => {
//         const formattedValue = JSON.parse((message.value as Buffer).toString());
//         console.log(`${formattedValue.user}: ${formattedValue.message}`);
//       },
//     });
//   } catch (error) {
//     console.error("Error:", error); 
//   }
// })(); 

// (async () => {  
//   try {
//     await consumer3.connect();
//     console.log("Connected to Kafka!");

//     await consumer3.subscribe({ topic: "testtopic" });
//     await consumer3.run({
//       eachMessage: async ({ topic, partition, message }) => {
//         const formattedValue = JSON.parse((message.value as Buffer).toString());
//         console.log(`${formattedValue.user}: ${formattedValue.message}`);
//       },
//     });
//   } catch (error) {
//     console.error("Error:", error); 
//   }
// })(); 


// (async () => {  
//   try {
//     await consumer4.connect();
//     console.log("Connected to Kafka!");

//     await consumer4.subscribe({ topic: "geolocation" });
//     await consumer4.run({
//       eachMessage: async ({ topic, partition, message }) => {
//         const formattedValue = JSON.parse((message.value as Buffer).toString());
//         console.log(`${formattedValue.user}: ${formattedValue.message}`);
//       },
//     });
//   } catch (error) {
//     console.error("Error:", error); 
//   }
// })(); 

