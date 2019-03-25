const kafka = require('kafka-node');
const config = require('./config');
const client = new kafka.KafkaClient({kafkaHost: config.kafka_server});

const offset = new kafka.Offset(client);

function consumer(off){
  try {
    const Consumer = kafka.Consumer;
    const client = new kafka.KafkaClient({kafkaHost: config.kafka_server});
    let consumer = new Consumer(
      client,
      [{ topic: config.kafka_topic, partition: 0,offset:off }],
      {
        autoCommit: false,
        fetchMaxWaitMs: 1000,
        fetchMaxBytes: 1024 * 1024,
        encoding: 'utf8',
        fromOffset: true
      }
    );
    return consumer;
  
  }
  catch(e) {
    console.log(e);
  }

}

module.exports = { consumer,offset }
