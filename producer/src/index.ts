import fs from 'fs'
import csv from 'csv-parser'
import { Kafka, CompressionTypes } from 'kafkajs'
import { exit } from 'process'

const KAFKA_CLIENT_ID = 'test-producer'
const KAFKA_BROKERS = ['kafka:9092']
const KAFKA_TOPIC = 'tweets'
const BATCH_LIMIT = 15
const BATCH_TIMEOUT = 60000

;(async () => {
  const data: any[] = []
  const readStream = fs.createReadStream(process.env.dataset)
  readStream.pipe(csv()).on('data', (row) => {
    data.push(row)
  })

  await new Promise((resolve) => {
    readStream.on('end', resolve)
  })

  const kafka = new Kafka({ clientId: KAFKA_CLIENT_ID, brokers: KAFKA_BROKERS })
  const producer = kafka.producer()
  await producer.connect()

  const batch = []
  let item: any
  for (item of data) {
    if (batch.length > BATCH_LIMIT) {
      await sendBatch(producer, batch)
      console.log('[producer]: added messages batch')
      await sleep(BATCH_TIMEOUT)
      batch.length = 0
    } else {
      item.created_at = new Date().toISOString()
      batch.push(item)
    }
  }
  console.log('[producer]: done')
})()

async function sendBatch(producer, data: any[]) {
  await producer
    .send({
      topic: KAFKA_TOPIC,
      // compression: CompressionTypes.GZIP,
      messages: data.map((value) => ({
        value: JSON.stringify(value),
      })),
    })
    .catch((e) => console.error(`[producer] ${e.message}`, e))
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}
