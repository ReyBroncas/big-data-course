import fs from 'fs'
import csv from 'csv-parser'
import { Kafka } from 'kafkajs'

const KAFKA_CLIENT_ID = 'test-producer'
const KAFKA_BROKERS = ['kafka:9092']
const KAFKA_TOPIC = 'transactions'
const BATCH_LIMIT = 20
const BATCH_TIMEOUT = 1000
const RANDOM_DATE_TRESHOLD = 30

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

  let batch = []
  let item: any
  for (item of data) {
    if (batch.length > BATCH_LIMIT) {
      await sendBatch(producer, batch)
      console.log(`[producer]: added ${BATCH_LIMIT} items`)
      await sleep(BATCH_TIMEOUT)
      batch = []
    } else {
      item.transaction_date = getRandomDate().getTime()

      batch.push(item)
    }
  }
  console.log('[producer]: done')
})()

async function sendBatch(producer, data: any[]) {
  await producer
    .send({
      topic: KAFKA_TOPIC,
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

function getRandomDate() {
  const end = new Date()
  const start = new Date(
    end.getTime() - RANDOM_DATE_TRESHOLD * 24 * 60 * 60 * 1000,
  )
  return new Date(
    start.getTime() + Math.random() * (end.getTime() - start.getTime()),
  )
}
