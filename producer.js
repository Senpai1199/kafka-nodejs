const { Kafka } = require("kafkajs");
const msg = process.argv[2];

run();
async function run() {
  try {
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["127.0.0.1:9092"],
    });

    const producer = kafka.producer();
    await producer.connect();
    console.log("Producer connected!");

    // A-M partition 0, N-Z partition 1
    const partition = msg[0] < "N" ? 0 : 1;
    console.log("Partition: ", partition);
    const result = await producer.send({
      topic: "Users1", // Users1 has 2 partitions, Users has 1 partition
      messages: [
        {
          value: msg,
          partition: partition,
        },
      ],
    });
    console.log("Produced successfully!", result);
    await producer.disconnect();
  } catch (err) {
    console.log(err);
  } finally {
    process.exit(1);
  }
}
