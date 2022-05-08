import { Injectable } from '@nestjs/common'
import * as cassandra from 'cassandra-driver'

@Injectable()
export class CassandraService extends cassandra.Client {
  constructor() {
    super({
      contactPoints: ['node0:9042'],
      localDataCenter: 'datacenter1',
      keyspace: 'hw4_bek',
    })
  }
}
