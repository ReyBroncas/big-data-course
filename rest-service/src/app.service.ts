import { Injectable } from '@nestjs/common'
import { CassandraService } from './cassandra/cassandra.service'

@Injectable()
export class AppService {
  constructor(private db: CassandraService) {}

  async getFraudTransactions(id: string) {
    const query = `
    SELECT "amount", "newbalanceOrig", "newbalanceDest", "oldbalanceOrg", "oldbalanceDest"
    FROM "fraudTransactions"
    WHERE "nameOrig"='${id}' 
    AND "isFraud"=true;`

    const res = await this.db.execute(query)
    return {
      result: res.rows.map(
        ({
          amount,
          newbalanceOrig,
          newbalanceDest,
          oldbalanceOrg,
          oldbalanceDest,
        }) => ({
          amount,
          newbalanceOrig,
          newbalanceDest,
          oldbalanceOrg,
          oldbalanceDest,
        }),
      ),
    }
  }
  async getTopTransactions(id: string) {
    const query = `
    SELECT "amount", "newbalanceOrig", "newbalanceDest", "oldbalanceOrg", "oldbalanceDest"
    FROM "topTransactions"
    WHERE "nameOrig"='${id}'
    ORDER BY "amount" DESC
    LIMIT 3`

    const res = await this.db.execute(query)

    return {
      result: res.rows.map(
        ({
          amount,
          newbalanceOrig,
          newbalanceDest,
          oldbalanceOrg,
          oldbalanceDest,
        }) => ({
          amount,
          newbalanceOrig,
          newbalanceDest,
          oldbalanceOrg,
          oldbalanceDest,
        }),
      ),
    }
  }

  async getTransactionsSum(id: string, startDate: string, endDate: string) {
    const query = `
    SELECT SUM("amount"), "amount" 
    FROM "incomingTransactions"
    WHERE "nameDest"='${id}'
    AND "transaction_date">'${startDate}'
    AND "transaction_date"<'${endDate}'`

    const res = await this.db.execute(query)

    return {
      result: {
        amount: res.rows[0]['system.sum(amount)'],
      },
    }
  }
}
