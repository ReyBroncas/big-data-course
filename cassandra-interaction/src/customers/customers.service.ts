import { Injectable } from '@nestjs/common'
import { CassandraService } from '../cassandra/cassandra.service'

@Injectable()
export class CustomersService {
  constructor(private db: CassandraService) {}
  async getTopProductiveCustomers(type, num, dateStart, dateEnd) {
    const query = `
      SELECT customer_id, COUNT(*),  FROM reviewByCustomerIdForGivenTimePeriod 
      WHERE verified_purchase=true AND review_date>'${dateStart}' AND review_date<'${dateEnd}' 
      GROUP BY customer_id ALLOW FILTERING;`

    const res = await this.db.execute(query)

    return {
      result: res.rows
        .sort((a, b) => a.count - b.count)
        .slice(0, num)
        .map(({ customer_id, count }) => ({
          customer_id,
          count,
        })),
    }
  }

  async getTopBakers(count, dateStart, dateEnd) {
    const query = `
      SELECT customer_id, COUNT(*), star_rating FROM reviewByCustomerIdForGivenTimePeriod 
      WHERE star_rating>3 AND star_rating<6 AND review_date>'${dateStart}' AND review_date<'${dateEnd}' 
      GROUP BY customer_id ALLOW FILTERING;`

    const res = await this.db.execute(query)
    return {
      result: res.rows
        .sort((a, b) => b.star_rating - a.star_rating)
        .slice(0, count)
        .map(({ customer_id, star_rating, count }) => ({
          customer_id,
          star_rating,
          count,
        })),
    }
  }

  async getTopHaters(count, dateStart, dateEnd) {
    const query = `
      SELECT customer_id, COUNT(*), star_rating FROM reviewByCustomerIdForGivenTimePeriod 
      WHERE star_rating>0 AND star_rating<3 AND review_date>'${dateStart}' AND review_date<'${dateEnd}' 
      GROUP BY customer_id ALLOW FILTERING;`

    const res = await this.db.execute(query)
    return {
      result: res.rows
        .sort((a, b) => a.star_rating - b.star_rating)
        .slice(0, count)
        .map(({ customer_id, star_rating, count }) => ({
          customer_id,
          star_rating,
          count,
        })),
    }
  }

  async getCustomerById(id: string) {
    const query = `
    SELECT review_id, review_body FROM reviewByCustomerId 
    WHERE customer_id=${id};`

    const res = await this.db.execute(query)

    return {
      result: res.rows.map(({ review_id, review_body }) => ({
        review_id,
        review_body,
      })),
    }
  }
}
