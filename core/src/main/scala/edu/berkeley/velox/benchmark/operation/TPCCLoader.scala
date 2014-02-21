package edu.berkeley.velox.benchmark.operation

import edu.berkeley.velox.benchmark.util.RandomGenerator
import edu.berkeley.velox.benchmark.{TPCCItemKey, TPCCConstants}
import edu.berkeley.velox.benchmark.datamodel.Transaction
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.datamodel.{Row, PrimaryKey, Timestamp}
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.cluster.TPCCPartitioner
import edu.berkeley.velox.rpc.InternalRPCService

object TPCCLoader extends Logging {
  val lastNameStrings = Array("BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING")

  private var generator = new RandomGenerator

  private def generateZipCode: String = {
    return generator.nstring(4, 4) + "11111"
  }

  private def generateDataString: String = {
    var data: String = generator.astring(26, 50)
    if (generator.nextDouble < .1) {
      val originalIndex: Int = generator.number(0, data.length - 9)
      data = data.substring(0, originalIndex) + "ORIGINAL" + data.substring(originalIndex + 8)
    }
    return data
  }

  private def generateLastName(customerNo: Int): String = {
    if (customerNo > 999) return Integer.toString(generator.NURand(255, 0, 999))
    else return lastNameStrings(customerNo % 10) + lastNameStrings(customerNo / 10 % 10) + lastNameStrings(customerNo / 100 % 10)
  }

  def doLoad(w_id: Int,
                partitioner: TPCCPartitioner,
                messageService: InternalRPCService,
                storage: StorageEngine) {
    val loadTxn = new Transaction(Timestamp.assignNewTimestamp(), partitioner, storage, messageService)
    val itemTable = loadTxn.table(TPCCConstants.ITEM_TABLE)

    logger.info(s"Creating items...")

    for (i_id <- 1 to 100000) {
     itemTable.put(PrimaryKey.pkey(i_id),
        Row.column(TPCCConstants.I_IM_ID_COL, generator.number(0, 10000))
        .column(TPCCConstants.I_NAME_COL, generator.astring(14, 24))
        .column(TPCCConstants.I_PRICE_COL, generator.fixedPoint(2, 1, 100))
        .column(TPCCConstants.I_DATA_COL, generateDataString))
    }

    logger.info(s"...created!")

    generator = new RandomGenerator

    logger.info(s"Creating warehouse ID $w_id")
    loadTxn.table(TPCCConstants.WAREHOUSE_TABLE).put(PrimaryKey.pkey(w_id),
      Row.column(TPCCConstants.W_NAME_COL, generator.astring(6, 10))
      .column(TPCCConstants.W_STREET_1_COL, generator.astring(6, 10))
      .column(TPCCConstants.W_STREET_2_COL, generator.astring(6, 10))
      .column(TPCCConstants.W_CITY_COL, generator.astring(10, 20))
      .column(TPCCConstants.W_STATE_COL, generator.astring(2, 2))
      .column(TPCCConstants.W_ZIP_COL, generateZipCode)
      .column(TPCCConstants.W_TAX_COL, generator.fixedPoint(4, 0, .2))
      .column(TPCCConstants.W_YTD_COL, 300000.0))

    logger.info(s"Creating stock for warehouses...")
    for (s_i_id <- 1 to 100000) {
      loadTxn.table(TPCCConstants.STOCK_TABLE_IMMUTABLE).put(PrimaryKey.pkey(w_id, s_i_id),
        Row.column(TPCCConstants.formatSDistXX(0), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(1), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(2), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(3), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(4), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(5), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(6), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(7), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(8), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(9), generator.astring(24, 24))
        .column(TPCCConstants.formatSDistXX(10), generator.astring(24, 24))
        .column(TPCCConstants.S_YTD_COL, 0)
        .column(TPCCConstants.S_DATA_COL, generateDataString))
      loadTxn.table(TPCCConstants.STOCK_TABLE_MUTABLE).put(PrimaryKey.pkey(w_id, s_i_id),
        Row.column(TPCCConstants.S_QUANTITY_COL, generator.number(10, 100))
          .column(TPCCConstants.S_ORDER_CNT, 0)
          .column(TPCCConstants.S_REMOTE_CNT, 0))
    }
    logger.info(s"....created stock for warehouses!")


    for (d_id <- 1 to 10) {
      logger.info(s"Creating district ID $d_id (warehouse: $w_id)")
      loadTxn.table(TPCCConstants.DISTRICT_TABLE).put(PrimaryKey.pkey(w_id, d_id),
        Row.column(TPCCConstants.D_NAME_COL, generator.astring(6, 10))
        .column(TPCCConstants.D_STREET_1_COL, generator.astring(10, 20))
        .column(TPCCConstants.D_STREET_2_COL, generator.astring(10, 20))
        .column(TPCCConstants.D_CITY_COL, generator.astring(10, 20))
        .column(TPCCConstants.D_STATE_COL, generator.astring(2, 2))
        .column(TPCCConstants.D_ZIP_COL, generateZipCode)
        .column(TPCCConstants.D_TAX_COL, generator.fixedPoint(4, 0, .2))
        .column(TPCCConstants.D_YTD_COL, 30000.0)
        .column(TPCCConstants.D_NEXT_O_ID, 3001))

      for (c_id <- 1 to 3000) {
        loadTxn.table(TPCCConstants.CUSTOMER_TABLE).put(PrimaryKey.pkey(w_id, d_id, c_id),
          Row.column(TPCCConstants.C_LAST_COL, generateLastName(c_id))
          .column(TPCCConstants.C_MIDDLE_COL, "OE")
          .column(TPCCConstants.C_FIRST_COL, generator.astring(8, 16))
          .column(TPCCConstants.C_STREET_1_COL, generator.astring(6, 10))
          .column(TPCCConstants.C_STREET_2_COL, generator.astring(6, 10))
          .column(TPCCConstants.C_CITY_COL, generator.astring(10, 20))
          .column(TPCCConstants.C_STATE_COL, generator.astring(2, 2))
          .column(TPCCConstants.C_ZIP_COL, generateZipCode)
          .column(TPCCConstants.C_PHONE_COL, generator.nstring(16, 16))
          .column(TPCCConstants.C_SINCE_COL, System.currentTimeMillis().toString)
          .column(TPCCConstants.C_CREDIT_COL, if (generator.nextDouble < .1) "BC" else "GC")
          .column(TPCCConstants.C_CREDIT_LIM_COL, 50000.0)
          .column(TPCCConstants.C_DISCOUNT_COL, generator.fixedPoint(4, 0, .5))
          .column(TPCCConstants.C_BALANCE_COL, -10.0).column(TPCCConstants.C_YTD_PAYMENT_COL, 10.0)
          .column(TPCCConstants.C_PAYMENT_CNT_COL, 1).column(TPCCConstants.C_DELIVERY_CNT_COL, 1)
          .column(TPCCConstants.C_DATA_COL, generator.astring(300, 500)))

        loadTxn.table(TPCCConstants.HISTORY_TABLE).put(PrimaryKey.pkey(w_id, d_id, c_id),
          Row.column(TPCCConstants.H_DATE_COL, System.currentTimeMillis.toString)
          .column(TPCCConstants.H_AMOUNT_COL, 10.0)
          .column(TPCCConstants.H_DATA_COL, generator.astring(12, 24)))
      }

      val c_id_ordered = (1 to 3000).toList
      val c_id_shuffle = scala.util.Random.shuffle(c_id_ordered)

      for (o_id <- 1 to 3000) {
        val orderCount: Int = generator.number(5, 15)
        val deliveryTime = System.currentTimeMillis.toString

        loadTxn.table(TPCCConstants.ORDER_TABLE).put(PrimaryKey.pkey(w_id, d_id, o_id),
          Row.column(TPCCConstants.O_ID, o_id)
          .column(TPCCConstants.O_C_ID_COL, c_id_shuffle(o_id - 1))
          .column(TPCCConstants.O_ENTRY_D, deliveryTime)
          .column(TPCCConstants.O_CARRIER_ID_COL, if (o_id < 2101) generator.number(1, 10) else -1)
          .column(TPCCConstants.O_OL_CNT_COL, orderCount).column(TPCCConstants.O_ALL_LOCAL_COL, 1))


        for (ol_number <- 0 to orderCount) {
          loadTxn.table(TPCCConstants.ORDER_LINE_TABLE)
            .put(PrimaryKey.pkey(w_id, d_id, o_id, ol_number),
            Row.column(TPCCConstants.OL_I_ID_COL, generator.number(1, 100000))
            .column(TPCCConstants.OL_SUPPLY_W_ID_COL, w_id)
            .column(TPCCConstants.OL_DELIVERY_D_COL, if (o_id < 2101) deliveryTime else "")
            .column(TPCCConstants.OL_QUANTITY_COL, 5)
            .column(TPCCConstants.OL_AMOUNT_COL,
              if (o_id < 2101) 0.0 else generator.fixedPoint(2, .01, 9999.99))
            .column(TPCCConstants.OL_DIST_INFO_COL, generator.astring(24, 24)));
          if (o_id > 2101) loadTxn.table(TPCCConstants.NEW_ORDER_TABLE).put(PrimaryKey.pkey(w_id, d_id, o_id), Row.column(-1))
        }
      }

      logger.info("Executing...")
      loadTxn.executeWrite
      logger.info(s"...executed ${storage.numKeys}.")
    }
  }
}
