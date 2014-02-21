package edu.berkeley.velox.benchmark

import edu.berkeley.velox.datamodel.PrimaryKey

object TPCCConstants {
  def formatSDistXX(d_id: Int): Int = {
    return S_DIST_XX_FMT + d_id - 1
  }

  def getDistrictNextOID(w_id: Int, d_id: Int): PrimaryKey = {
    val ret = PrimaryKey.pkey( w_id, d_id)
    ret.table = DISTRICT_TABLE
    return ret
  }

  final val W_NAME_COL: Int = 1
  final val W_STREET_1_COL: Int = 2
  final val W_STREET_2_COL: Int = 3
  final val W_CITY_COL: Int = 4
  final val W_STATE_COL: Int = 5
  final val W_ZIP_COL: Int = 6
  final val W_YTD_COL: Int = 7
  final val W_TAX_COL: Int = 8
  final val D_NAME_COL: Int = 9
  final val D_STREET_1_COL: Int = 10
  final val D_STREET_2_COL: Int = 11
  final val D_CITY_COL: Int = 12
  final val D_STATE_COL: Int = 13
  final val D_ZIP_COL: Int = 14
  final val D_TAX_COL: Int = 15
  final val D_YTD_COL: Int = 16
  final val D_NEXT_O_ID: Int = 17
  final val C_LAST_COL: Int = 18
  final val C_MIDDLE_COL: Int = 19
  final val C_FIRST_COL: Int = 20
  final val C_STREET_1_COL: Int = 21
  final val C_STREET_2_COL: Int = 22
  final val C_CITY_COL: Int = 23
  final val C_STATE_COL: Int = 24
  final val C_ZIP_COL: Int = 25
  final val C_PHONE_COL: Int = 26
  final val C_SINCE_COL: Int = 27
  final val C_CREDIT_COL: Int = 28
  final val C_CREDIT_LIM_COL: Int = 29
  final val C_DISCOUNT_COL: Int = 30
  final val C_BALANCE_COL: Int = 31
  final val C_YTD_PAYMENT_COL: Int = 32
  final val C_PAYMENT_CNT_COL: Int = 33
  final val C_DELIVERY_CNT_COL: Int = 34
  final val C_DATA_COL: Int = 35
  final val H_DATE_COL: Int = 36
  final val H_AMOUNT_COL: Int = 37
  final val H_DATA_COL: Int = 38
  final val O_ID: Int = 39
  final val O_C_ID_COL: Int = 40
  final val O_ENTRY_D: Int = 41
  final val O_CARRIER_ID_COL: Int = 42
  final val O_ALL_LOCAL_COL: Int = 43
  final val O_OL_CNT_COL: Int = 44
  final val I_IM_ID_COL: Int = 45
  final val I_PRICE_COL: Int = 46
  final val I_NAME_COL: Int = 47
  final val I_DATA_COL: Int = 48
  final val S_QUANTITY_COL: Int = 49
  final val S_DATA_COL: Int = 50
  final val S_YTD_COL: Int = 51
  final val S_ORDER_CNT: Int = 52
  final val S_REMOTE_CNT: Int = 53
  final val OL_DELIVERY_D_COL: Int = 54
  final val OL_NUMBER_COL: Int = 55
  final val OL_I_ID_COL: Int = 56
  final val OL_DIST_INFO_COL: Int = 57
  final val OL_SUPPLY_W_ID_COL: Int = 58
  final val OL_QUANTITY_COL: Int = 59
  final val OL_AMOUNT_COL: Int = 60
  private final val S_DIST_XX_FMT: Int = 61
  final val WAREHOUSE_TABLE: Int = 62
  final val STOCK_TABLE_IMMUTABLE: Int = 63
  final val DISTRICT_TABLE: Int = 64
  final val CUSTOMER_TABLE: Int = 65
  final val HISTORY_TABLE: Int = 66
  final val ITEM_TABLE: Int = 67
  final val ORDER_TABLE: Int = 68
  final val NEW_ORDER_TABLE: Int = 69
  final val ORDER_LINE_TABLE: Int = 70
  var PLACE_HOLDER_COLUMN: Int = 71
  var PLACE_HOLDER_VALUE: Int = 72
  final val STOCK_TABLE_MUTABLE: Int = 73
  final val ORDER_LOOKUP_TABLE: Int = 74


}


