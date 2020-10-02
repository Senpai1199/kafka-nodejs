const { Kafka } = require("kafkajs");
const msg = process.argv[2];

run();
async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["127.0.0.1:9092"],
    });

    const consumer = kafka.consumer({ groupId: "testGroup" });
    await consumer.connect();
    console.log("Consumer connected!");
    consumer.subscribe({
      topic: "Users1",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async (result) => {
        console.log(
          `Received message: ${result.message.value} on partition: ${result.partition}.`
        );
      },
    });

    console.log("Consumed successfully");
  } catch (err) {
    console.log(err);
  } finally {
    // process.exit(1);
  }
}
