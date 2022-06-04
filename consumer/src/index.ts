import { promises as fsPromises, existsSync, mkdirSync } from 'fs'
import { join } from 'path'

import ObjectsToCsv from 'objects-to-csv'
import dayjs from 'dayjs'
import { Kafka, CompressionTypes } from 'kafkajs'
import * as cassandra from 'cassandra-driver'
import { getInsertQuery } from './queries'

const KAFKA_CLIENT_ID = 'test-consumer'
const KAFKA_BROKERS = ['kafka:9092']
const KAFKA_TOPIC = 'transactions'
const BATCH_LIMIT = 20
const BATCH_TIMEOUT = 60000

;(async () => {
  const kafka = new Kafka({ clientId: KAFKA_CLIENT_ID, brokers: KAFKA_BROKERS })
  const kafkaConsumer = kafka.consumer({ groupId: KAFKA_CLIENT_ID })
  await kafkaConsumer.connect()
  await kafkaConsumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: true })
  const cassandraClient = new cassandra.Client({
    contactPoints: ['cassandra-node:9042'],
    localDataCenter: 'datacenter1',
    keyspace: 'lab_8',
  })

  const data = []

  const writeBatch = async () => {
    if (!data.length) {
      console.log(`[consumer]: sleeping for ${BATCH_TIMEOUT}ms`)
      return
    }
    await saveInDb(cassandraClient, data)
    data.length = 0
    console.log(`[consumer]: sleeping for ${BATCH_TIMEOUT}ms`)
  }

  const startupDatetime = new Date()
  setTimeout(() => {
    setInterval(writeBatch, 60000)
    writeBatch()
  }, (60 - startupDatetime.getSeconds()) * 1000)

  await kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      data.push(JSON.parse(message.value.toString()))
    },
  })
})().catch((e) => console.error(`[consumer]: ${e.message}`, e))

async function saveInDb(cassandraClient: cassandra.Client, data: any[]) {
  for (const item of data) {
    await cassandraClient.execute(getInsertQuery(item))
    console.log(`[consumer]: saved item in db`)
  }
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms)
  })
}
