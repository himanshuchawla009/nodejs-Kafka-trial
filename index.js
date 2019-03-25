// const bp = require('body-parser');
const config = require('./config');
const express = require('express');
const app = express();
const kafka = require('kafka-node')
const kafkaConsumer =  require('./consumer');
kafkaConsumer.consumer();
const { offset } = require('./consumer');




const abi = [
	{
		"constant": true,
		"inputs": [],
		"name": "storedData",
		"outputs": [
			{
				"name": "",
				"type": "uint256"
			}
		],
		"payable": false,
		"stateMutability": "view",
		"type": "function"
	},
	{
		"constant": false,
		"inputs": [
			{
				"name": "x",
				"type": "uint256"
			}
		],
		"name": "set",
		"outputs": [],
		"payable": false,
		"stateMutability": "nonpayable",
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "get",
		"outputs": [
			{
				"name": "retVal",
				"type": "uint256"
			}
		],
		"payable": false,
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [
			{
				"name": "initVal",
				"type": "uint256"
			}
		],
		"payable": false,
		"stateMutability": "nonpayable",
		"type": "constructor"
	}
];


app.listen(4545,()=>{
    console.log("listening on port 4545")

})




const setValue = async (payload)=>{
    try {
       return new Promise(async (resolve,reject)=>{
            const Producer = kafka.Producer;
            const client = new kafka.KafkaClient({kafkaHost: config.kafka_server});
            const producer = new Producer(client);
            const kafka_topic = config.kafka_topic;
           
            producer.on('ready', async function() {
            console.log("ready")
       
               
                  
                 setTimeout(()=>{

                  let payloads = [
                    {
                      topic: kafka_topic,
                      messages: payload
                    }
                  ];
                let push_status = producer.send(payloads, (err, data) => {
                    console.log("i m in first")
                    if (err) {
                      console.log(err)
                      console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
                    } else {
                      console.log('[kafka-producer -> '+kafka_topic+']: broker update success');
                     
                    }
                  });
                 },5000)
                    
                  
                    
                    resolve(payload)
         
    
                     
                     
                    });
                  
                    producer.on('error', function(err) {
                      console.log(err);
                      console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
                      throw err;
                    });
    
         
            
        })
    } catch (error) {
        reject(error)
    }
}

const setValueAgain = async (payload)=>{
    try {
       return new Promise(async (resolve,reject)=>{
            const Producer = kafka.Producer;
            const client = new kafka.KafkaClient({kafkaHost: config.kafka_server});
            const producer = new Producer(client);
            const kafka_topic = config.kafka_topic;
            
            producer.on('ready', async function() {
            console.log("ready")
       
               
                      
                     setTimeout(()=>{
                   

                    let payloads = [
                        {
                          topic: kafka_topic,
                          messages: payload
                        }
                      ];
                    let push_status = producer.send(payloads, (err, data) => {
                        console.log("i m in second")
                        if (err) {
                          console.log(err)
                          console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
                        } else {
                          console.log('[kafka-producer -> '+kafka_topic+']: broker update success');
                         
                        }
                      });
                    
                    
                   },5000)

                    
                    resolve(payload)
         
    
                     
                     
                    });
                  
                    producer.on('error', function(err) {
                      console.log(err);
                      console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
                      throw err;
                    });
    
         
            
        })
    } catch (error) {
        reject(error)
    }
}

app.get('/produce',async (req,res,next)=>{
  try {
    let val = await setValue("producing");
    offset.fetch([
      { topic: 'test', partition: 0, time: -1, maxNum: 1 }
  ], function (err, data) {
      // data 
      // { 't': { '0': [999] } } 
      console.log(data[config.kafka_topic][0][0],"offset")

    let consumerConnection = kafkaConsumer.consumer(data[config.kafka_topic][0][0]);
  
    consumerConnection.on('message', async function(message) {
    
        if(message.value === 'producing') {
            let dat = new Date();
            console.log(
                'kafka first-> '+ dat.toTimeString(),
                message
              );
              
                consumerConnection.close(true, function(err, message) {
                  console.log("consumer has been closed..");
                });
              }
          
    })
    consumerConnection.on('error', function(err) {
      console.log('error', err);
    });
  });
    res.send({val})
  } catch (error) {
    throw error
  }
})


app.get('/produceAgain',async (req,res,next)=>{
  try {
   let val = await setValueAgain("producing again");
   offset.fetch([
    { topic: 'test', partition: 0, time: -1, maxNum: 1 }
], function (err, data) {
    // data 
    // { 't': { '0': [999] } } 
    console.log(data[config.kafka_topic][0][0],"offset")

  let consumerConnection = kafkaConsumer.consumer(data[config.kafka_topic][0][0]);

    consumerConnection.on('message', async function(message) {
       if(message.value === 'producing again') {
        let dat = new Date();
        console.log(
            'kafka second-> ' + dat.toTimeString(),
            message
          );
          
          consumerConnection.close(true, function(err, message) {
            console.log("consumer has been closed..");
          });
       }
     
     
      
    })
    consumerConnection.on('error', function(err) {
      console.log('error', err);
    });
  })
    
    res.send({val})
  } catch (error) {
     throw error
  }
})

