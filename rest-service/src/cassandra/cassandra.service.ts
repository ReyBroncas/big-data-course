import { Injectable } from '@nestjs/common'
import * as cassandra from 'cassandra-driver'

@Injectable()
export class CassandraService extends cassandra.Client {
  constructor() {
    super({
    contactPoints: ['cassandra-node:9042'],
    localDataCenter: 'datacenter1',
    keyspace: 'lab_8',
  })
  }
}
