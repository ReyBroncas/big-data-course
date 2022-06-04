import { Body, Controller, Get, Param, Post } from '@nestjs/common'
import { AppService } from './app.service'

@Controller('transaction')
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get(':id/fraud')
  async getFraud(@Param('id') id: string) {
    return await this.appService.getFraudTransactions(id)
  }

  @Get(':id/top')
  async getTop(@Param('id') id: string) {
    return await this.appService.getTopTransactions(id)
  }

  @Post(':id/sum')
  async getSum(
    @Param('id') id: string,
    @Body('startDate') startDate: string,
    @Body('endDate') endDate: string,
  ) {
    return await this.appService.getTransactionsSum(id, startDate, endDate)
  }
}
