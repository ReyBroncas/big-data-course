import {
  Body,
  Controller,
  Get,
  Param,
  ParseIntPipe,
  Query,
} from '@nestjs/common'
import { ProductsService } from './products.service'

@Controller('product')
export class ProductsController {
  constructor(private readonly productService: ProductsService) {}

  @Get('top')
  getTop(
    @Query('t') type,
    @Query('date_start') dateStart,
    @Query('date_end') dateEnd,
    @Query('count') count,
  ) {
    return this.productService.getTopProducts(type, count, dateStart, dateEnd)
  }

  @Get(':id')
  async getProduct(
    @Param('id') id: string,
    @Query('star_rating') starRating?: number,
  ) {
    return await this.productService.getProduct(id, starRating)
  }
}
