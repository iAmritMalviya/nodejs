import { Kafka } from "kafkajs";
import readline from "readline";

const redpanda = new Kafka({
  brokers: ["localhost:19092"],  // Standard Redpanda port
});

const producer = redpanda.producer();

async function main() {
  await producer.connect();

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  console.log("Enter messages to send (type 'exit' to quit):");

  rl.on("line", async (line) => {
    if (line === "exit") {
      await disconnect();
      rl.close();
      return;
    }

    const user = "terminalUser"; 
    const message = line;

    try {
      await producer.send({
        topic: "testtopic",
        messages: [{ value: JSON.stringify({ message, user }) }],
      });
      console.log("Message sent successfully!");
    } catch (error) {
      console.error("Error sending message:", error);
    }
  });
}

async function disconnect() {
  try {
    await producer.disconnect();
    console.log("Producer disconnected.");
  } catch (error) {
    console.error("Error disconnecting:", error);
  }
}

main();
