import { Module } from '@nestjs/common'
import { AppController } from './app.controller'
import { AppService } from './app.service'
import { CassandraModule } from './cassandra/cassandra.module'

@Module({
  imports: [CassandraModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
