import { Module } from '@nestjs/common'
import { CassandraModule } from './cassandra/cassandra.module'
import { CustomersModule } from './customers/customers.module'
import { ProductsModule } from './products/products.module'

@Module({
  imports: [CassandraModule, ProductsModule, CustomersModule],
})
export class AppModule {}
