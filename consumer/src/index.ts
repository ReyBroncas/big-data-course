import { promises as fsPromises, existsSync, mkdirSync } from 'fs'
import { join } from 'path'
import { Kafka, CompressionTypes } from 'kafkajs'
import ObjectsToCsv from 'objects-to-csv'
import dayjs from 'dayjs'

const KAFKA_CLIENT_ID = 'test-consumer'
const KAFKA_BROKERS = ['kafka:9092']
const KAFKA_TOPIC = 'tweets'
const BATCH_LIMIT = 10
const BATCH_TIMEOUT = 60000

;(async () => {
  const kafka = new Kafka({ clientId: KAFKA_CLIENT_ID, brokers: KAFKA_BROKERS })
  const consumer = kafka.consumer({ groupId: KAFKA_CLIENT_ID })

  const startupDatetime = new Date()
  const data = []

  const writeBatch = async () => {
    if (!data.length) {
      console.log('[consumer]: sleeping for 6000ms')
      return
    }
    await writeData(data)
    data.length = 0
    console.log('[consumer]: sleeping for 6000ms')
  }
  setTimeout(() => {
    setInterval(writeBatch, 60000)
    writeBatch()
  }, (60 - startupDatetime.getSeconds()) * 1000)

  await consumer.connect()
  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { author_id, created_at, text } = JSON.parse(
        message.value.toString(),
      )
      console.log(`[consumer]: added ${{ author_id, created_at, text }}`)
      data.push({ author_id, created_at, text })
    },
  })
})().catch((e) => console.error(`[consumer]: ${e.message}`, e))

async function writeData(data: any[]) {
  if (!existsSync('data')) {
    mkdirSync('data')
  }

  const filename = join(
    'data',
    `tweets_${dayjs().format('DD_MM_YYYY_h_mm').toString()}.csv`,
  )
  const csv = new ObjectsToCsv(data)
  await csv.toDisk(filename)
  console.log(`[consumer]: added ${filename}`)
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}
