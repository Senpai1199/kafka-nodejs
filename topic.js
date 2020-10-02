const { Kafka } = require("kafkajs");

run();
async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["127.0.0.1:9092"],
    });

    const admin = kafka.admin();
    await admin.connect();
    console.log("Admin connected!");
    await admin.createTopics({
      topics: [
        {
          topic: "Users1",
          numPartitions: 2,
          replicationFactor: 1,
        },
      ],
    });
    console.log("Topic created successfully!");
    await admin.disconnect();
  } catch (err) {
    console.log(err);
  } finally {
    process.exit(1);
  }
}
