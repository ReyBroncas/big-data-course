import { Get, Param, Controller, Query } from '@nestjs/common'
import { CustomersService } from './customers.service'

@Controller('customer')
export class CustomersController {
  constructor(private readonly customerService: CustomersService) {}

  @Get('top')
  async getTopCustomers(
    @Query('t') type,
    @Query('date_start') dateStart,
    @Query('date_end') dateEnd,
    @Query('count') count,
  ) {
    if (type === 'top_productive') {
      return this.customerService.getTopProductiveCustomers(
        type,
        count,
        dateStart,
        dateEnd,
      )
    } else if (type === 'top_haters') {
      return this.customerService.getTopHaters(count, dateStart, dateEnd)
    } else if (type === 'top_backers') {
      return this.customerService.getTopBakers(count, dateStart, dateEnd)
    }

    return {}
  }

  @Get(':id')
  async getCustomerById(@Param('id') id: string) {
    return await this.customerService.getCustomerById(id)
  }
}
