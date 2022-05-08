import { Injectable } from '@nestjs/common'
import { CassandraService } from '../cassandra/cassandra.service'

@Injectable()
export class ProductsService {
  constructor(private db: CassandraService) {}

  async getTopProducts(filter, count, dateStart, dateEnd) {
    const query = `
    SELECT product_id, COUNT(*) FROM productForGivenTimePeriod 
    WHERE review_date>'${dateStart}' AND review_date<'${dateEnd}' 
    GROUP BY product_id ALLOW FILTERING;`

    const res = await this.db.execute(query)

    if (filter === 'review_num') {
      return {
        result: res.rows
          .sort((a, b) => a.count - b.count)
          .slice(0, count)
          .map((row) => ({
            product_id: row.product_id,
            num: row.count,
          })),
      }
    }
    return {}
  }

  async getProduct(id: string, starRating?: number) {
    const query = `
    SELECT review_id, review_body FROM reviewByProductId 
    WHERE product_id='${id}'${
      starRating !== undefined ? ` AND star_rating=${starRating}` : ''
    };`

    const res = await this.db.execute(query)

    return {
      result: res.rows.map((row) => ({
        review_id: row.review_id,
        review_body: row.review_body,
      })),
    }
  }
}
