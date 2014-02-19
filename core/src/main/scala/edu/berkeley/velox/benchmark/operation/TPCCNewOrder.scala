package edu.berkeley.benchmark.tpcc

import edu.berkeley.velox.benchmark.operation.{TPCCNewOrderLineResult, TPCCNewOrderRequest, TPCCNewOrderResponse}
import edu.berkeley.velox.benchmark.datamodel.Transaction
import edu.berkeley.velox.datamodel.Timestamp

import scala.util.control.Breaks._
import edu.berkeley.velox.benchmark.{TPCCItemKey, TPCCConstants}
import edu.berkeley.kaiju.storedproc.datamodel.Row
import java.util
import edu.berkeley.velox.cluster.TPCCPartitioner
import edu.berkeley.velox.rpc.InternalRPCService
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success}
import edu.berkeley.velox.util.NonThreadedExecutionContext._
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.storage.StorageEngine

object TPCCNewOrder extends Logging {
  def execute(request: TPCCNewOrderRequest,
              partitioner: TPCCPartitioner,
              messageService: InternalRPCService,
              storage: StorageEngine): Future[TPCCNewOrderResponse] = {

    val p = Promise[TPCCNewOrderResponse]

    val readTxn = new Transaction(-1, partitioner, storage, messageService)
    val writeTxn = new Transaction(Timestamp.assignNewTimestamp(), partitioner, storage, messageService)
    val shadow_O_ID = Timestamp.assignNewTimestamp.asInstanceOf[Int]
    val W_ID = request.W_ID
    val C_ID = request.C_ID
    val D_ID = request.D_ID
    val OL_I_IDs = request.OL_I_IDs
    val OL_QUANTITIES = request.OL_QUANTITIES
    val OL_SUPPLY_W_IDs = request.OL_SUPPLY_W_IDs
    val OL_CNT = OL_I_IDs.size

    var allLocal = OL_SUPPLY_W_IDs.asScala.filter(i => i != W_ID).isEmpty

    val O_ENTRY_D: String = System.currentTimeMillis.toString

    readTxn.table(TPCCConstants.WAREHOUSE_TABLE).get(Row.pkey(W_ID).column(TPCCConstants.W_TAX_COL))

    readTxn.table(TPCCConstants.DISTRICT_TABLE).get(Row.pkey(W_ID, D_ID)
      .column(TPCCConstants.D_TAX_COL))

    readTxn.table(TPCCConstants.CUSTOMER_TABLE).get(Row.pkey(W_ID, D_ID, C_ID)
      .column(TPCCConstants.C_DISCOUNT_COL)
      .column(TPCCConstants.C_LAST_COL)
      .column(TPCCConstants.C_CREDIT_COL))

    writeTxn.table(TPCCConstants.NEW_ORDER_TABLE).put(Row.pkey(W_ID, D_ID, shadow_O_ID)
      .column(TPCCConstants.PLACE_HOLDER_COLUMN, TPCCConstants.PLACE_HOLDER_VALUE))

    writeTxn.table(TPCCConstants.ORDER_TABLE).put(Row.pkey(W_ID, D_ID, shadow_O_ID, OL_CNT)
      .column(TPCCConstants.O_C_ID_COL, C_ID).column(TPCCConstants.O_ENTRY_D, O_ENTRY_D)
      .column(TPCCConstants.O_CARRIER_ID_COL, null)
      .column(TPCCConstants.O_ALL_LOCAL_COL, if (allLocal) 1 else 0))

    for (ol_cnt <- 0 until OL_CNT) {
      val OL_I_ID: Int = OL_I_IDs.get(ol_cnt)
      val S_W_ID: Int = OL_SUPPLY_W_IDs.get(ol_cnt)

      readTxn.table(TPCCConstants.ITEM_TABLE).get(Row.pkey(OL_I_ID)
        .column(TPCCConstants.I_PRICE_COL)
        .column(TPCCConstants.I_NAME_COL)
        .column(TPCCConstants.I_DATA_COL))

      readTxn.table(TPCCConstants.STOCK_TABLE).get(Row.pkey(S_W_ID, OL_I_ID)
        .column(TPCCConstants.S_DATA_COL).column(TPCCConstants.S_QUANTITY_COL)
        .column(TPCCConstants.formatSDistXX(D_ID)))

      if (W_ID == S_W_ID) {
        readTxn.table(TPCCConstants.STOCK_TABLE).get(Row.pkey(S_W_ID, OL_I_ID).column(TPCCConstants.S_ORDER_CNT))
      }
      else {
        readTxn.table(TPCCConstants.STOCK_TABLE).get(Row.pkey(S_W_ID, OL_I_ID).column(TPCCConstants.S_REMOTE_CNT))
      }
    }

    val readFuture = readTxn.executeRead

    readFuture onComplete {
      case Success(t) => {
        logger.error(s"SUCCESS readfuture!")

        var totalAmount: Double = 0
        var newOrderLines = new util.ArrayList[TPCCNewOrderLineResult]()

        var aborted = false

        for (ol_cnt <- 0 until OL_CNT) {
          val OL_I_ID: Int = OL_I_IDs.get(ol_cnt)
          val OL_QUANTITY: Int = OL_QUANTITIES.get(ol_cnt)
          val S_W_ID: Int = OL_SUPPLY_W_IDs.get(ol_cnt)

          logger.error(s"ol_cnt $ol_cnt!")


          if (readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.ITEM_TABLE, OL_I_ID, TPCCConstants.I_NAME_COL)) == null) {
            logger.error(s"aborting")

            logger.error(s"returning")
            val ret = new TPCCNewOrderResponse(false)

            p.success(ret)
            aborted = true
            break
          } else {

            val I_PRICE = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.ITEM_TABLE, OL_I_ID, TPCCConstants.I_PRICE_COL)).asInstanceOf[Double]
            val I_DATA = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.ITEM_TABLE, OL_I_ID, TPCCConstants.I_DATA_COL)).asInstanceOf[String]
            val I_NAME = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.ITEM_TABLE, OL_I_ID, TPCCConstants.I_NAME_COL)).asInstanceOf[String]
            val S_DATA = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.STOCK_TABLE, S_W_ID, OL_I_ID, TPCCConstants.S_DATA_COL)).asInstanceOf[String]
            var currentStock = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.STOCK_TABLE, S_W_ID, OL_I_ID, TPCCConstants.S_QUANTITY_COL)).asInstanceOf[Integer]

            var brandGeneric: String = "B"
            if (I_DATA.contains("ORIGINAL") && S_DATA.contains("ORIGINAL")) {
              brandGeneric = "G"
            }
            val S_DIST_XX: String = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.STOCK_TABLE, S_W_ID, OL_I_ID, TPCCConstants.formatSDistXX(D_ID))).asInstanceOf[String]
            val OL_AMOUNT: Double = OL_QUANTITY * I_PRICE
            totalAmount += OL_AMOUNT

            newOrderLines.add(new TPCCNewOrderLineResult(S_W_ID, OL_I_ID, I_NAME, OL_QUANTITY, currentStock, brandGeneric, I_PRICE, OL_AMOUNT))

            writeTxn.table(TPCCConstants.ORDER_LINE_TABLE).put(Row.pkey(W_ID, D_ID, shadow_O_ID).column(TPCCConstants.OL_QUANTITY_COL, OL_QUANTITY).column(TPCCConstants.OL_AMOUNT_COL, OL_AMOUNT).column(TPCCConstants.OL_I_ID_COL, OL_I_ID).column(TPCCConstants.OL_SUPPLY_W_ID_COL, S_W_ID).column(TPCCConstants.OL_DELIVERY_D_COL, null).column(TPCCConstants.OL_NUMBER_COL, ol_cnt).column(TPCCConstants.OL_DIST_INFO_COL, S_DIST_XX))
            if (currentStock > OL_QUANTITY + 10) {
              currentStock -= OL_QUANTITY
            }
            else {
              currentStock = (currentStock - OL_QUANTITY + 91)
            }

            writeTxn.table(TPCCConstants.STOCK_TABLE).put(Row.pkey(S_W_ID, OL_I_ID).column(TPCCConstants.S_QUANTITY_COL, currentStock))
            if (W_ID == S_W_ID) {
              val currentOrderCount = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.STOCK_TABLE, S_W_ID, OL_I_ID, TPCCConstants.S_ORDER_CNT)).asInstanceOf[Integer]
              writeTxn.table(TPCCConstants.STOCK_TABLE).put(Row.pkey(S_W_ID, OL_I_ID).column(TPCCConstants.S_ORDER_CNT, currentOrderCount + 1))
            }
            else {
              val currentRemoteCount: Int = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.STOCK_TABLE, S_W_ID, OL_I_ID, TPCCConstants.S_REMOTE_CNT)).asInstanceOf[Integer]
              writeTxn.table(TPCCConstants.STOCK_TABLE).put(Row.pkey(S_W_ID, OL_I_ID).column(TPCCConstants.S_REMOTE_CNT, currentRemoteCount + 1))
            }
          }
        }

        if(!aborted) {
          // TODO! Deferred
          //writeTxn.table(TPCCConstants.ORDER_TABLE).put(Row.pkey(W_ID, D_ID, shadow_O_ID).column(TPCCConstants.O_ID, new DeferredCounter(TPCCConstants.getDistrictNextOID(W_ID, D_ID), 1)))
          writeTxn.table(TPCCConstants.ORDER_TABLE).put(Row.pkey(W_ID, D_ID, shadow_O_ID).column(TPCCConstants.O_ID, 1))

          logger.error(s"writefuture executing!")

          val writeFuture = writeTxn.executeWrite

          writeFuture onComplete {
            case Success(_) => {

              val O_ID = -1
              //TODO! (writeTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.ORDER_TABLE, W_ID, D_ID, shadow_O_ID, TPCCConstants.O_ID)).asInstanceOf[DeferredResult]).getValue.asInstanceOf[Integer]
              val C_DISCOUNT = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.CUSTOMER_TABLE, W_ID, D_ID, C_ID, TPCCConstants.C_DISCOUNT_COL)).asInstanceOf[Double]
              val W_TAX = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.WAREHOUSE_TABLE, W_ID, TPCCConstants.W_TAX_COL)).asInstanceOf[Double]
              val D_TAX = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.DISTRICT_TABLE, W_ID, D_ID, TPCCConstants.D_TAX_COL)).asInstanceOf[Double]
              totalAmount *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)

              val C_LAST: String = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.CUSTOMER_TABLE, W_ID, D_ID, C_ID, TPCCConstants.C_LAST_COL)).asInstanceOf[String]
              val C_CREDIT: String = readTxn.getQueryResult(TPCCItemKey.key(TPCCConstants.CUSTOMER_TABLE, W_ID, D_ID, C_ID, TPCCConstants.C_CREDIT_COL)).asInstanceOf[String]

              p success new TPCCNewOrderResponse(W_ID, D_ID, C_ID, O_ID, OL_CNT, C_LAST, C_CREDIT, C_DISCOUNT, W_TAX, D_TAX, O_ENTRY_D, totalAmount, newOrderLines)
            }
            case Failure(t) => p.failure(t)
          }
        }
      }
      case Failure(t) => p.failure(t)
    }
    p.future
  }
}

