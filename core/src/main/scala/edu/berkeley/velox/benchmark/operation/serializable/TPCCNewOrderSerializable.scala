package edu.berkeley.velox.benchmark.operation.serializable

import edu.berkeley.velox.benchmark.operation.{DeferredIncrement, TPCCNewOrderLineResult, TPCCNewOrderRequest, TPCCNewOrderResponse}
import edu.berkeley.velox.benchmark.datamodel.Transaction
import edu.berkeley.velox.datamodel.{Row, PrimaryKey, Timestamp}

import edu.berkeley.velox.benchmark.TPCCConstants
import java.util
import edu.berkeley.velox.cluster.TPCCPartitioner
import edu.berkeley.velox.rpc.InternalRPCService
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success}
import edu.berkeley.velox.util.NonThreadedExecutionContext._
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.benchmark.datamodel.serializable.{SerializableRowGenerator, LockManager, SerializableRow, SerializableTransaction}

object TPCCNewOrderSerializable extends Logging {
  def execute(lockManager: LockManager,
              request: TPCCNewOrderRequest,
              partitioner: TPCCPartitioner,
              messageService: InternalRPCService,
              storage: StorageEngine): Future[TPCCNewOrderResponse] = {


    val p = Promise[TPCCNewOrderResponse]

    val txn = new SerializableTransaction(lockManager, Timestamp.assignNewTimestamp(), partitioner, storage, messageService)
    val W_ID = request.W_ID
    val C_ID = request.C_ID
    val D_ID = request.D_ID
    val OL_I_IDs = request.OL_I_IDs
    val OL_QUANTITIES = request.OL_QUANTITIES
    val OL_SUPPLY_W_IDs = request.OL_SUPPLY_W_IDs
    val OL_CNT = OL_I_IDs.size

    var sit = OL_SUPPLY_W_IDs.iterator()
    var allLocal = true
    while(sit.hasNext && allLocal) {
      if(sit.next() != W_ID) {
        allLocal = false
      }

    }

    val O_ENTRY_D: String = System.currentTimeMillis.toString

    txn.table(TPCCConstants.WAREHOUSE_TABLE).get(PrimaryKey.pkey(W_ID), SerializableRowGenerator.fetchColumn(TPCCConstants.W_TAX_COL))

    txn.table(TPCCConstants.DISTRICT_TABLE).get(PrimaryKey.pkey(W_ID, D_ID),
      SerializableRowGenerator.fetchColumn(TPCCConstants.D_TAX_COL).columnForUpdate(TPCCConstants.D_NEXT_O_ID))

    txn.table(TPCCConstants.CUSTOMER_TABLE).get(PrimaryKey.pkey(W_ID, D_ID, C_ID),
      SerializableRowGenerator.fetchColumn(TPCCConstants.C_DISCOUNT_COL)
      .column(TPCCConstants.C_LAST_COL)
      .column(TPCCConstants.C_CREDIT_COL))



    var ol_cnt = 0
    while(ol_cnt < OL_CNT) {

      val OL_I_ID: Int = OL_I_IDs.get(ol_cnt)

      val S_W_ID: Int = OL_SUPPLY_W_IDs.get(ol_cnt)

      val t = txn.table(TPCCConstants.ITEM_TABLE).get(PrimaryKey.pkey(OL_I_ID),
        SerializableRowGenerator.fetchColumn(TPCCConstants.I_PRICE_COL)
      .column(TPCCConstants.I_NAME_COL).column(TPCCConstants.I_DATA_COL))


      txn.table(TPCCConstants.STOCK_TABLE_IMMUTABLE).get(PrimaryKey.pkey(S_W_ID, OL_I_ID),
        SerializableRowGenerator.fetchColumn(TPCCConstants.S_DATA_COL)
        .column(TPCCConstants.formatSDistXX(D_ID)))


      txn.table(TPCCConstants.STOCK_TABLE_MUTABLE).get(PrimaryKey.pkey(S_W_ID, OL_I_ID),
        SerializableRowGenerator.columnForUpdate(TPCCConstants.S_ORDER_CNT)
          .column(TPCCConstants.S_REMOTE_CNT)
          .column(TPCCConstants.S_QUANTITY_COL))

      ol_cnt += 1
    }

    val readFuture = txn.executeRead

    readFuture onComplete {
      case Success(t) => {
        var totalAmount: Double = 0
        val newOrderLines = new util.ArrayList[TPCCNewOrderLineResult]()

        var aborted = false
        var ol_cnt = 0


        var O_ID = -1

        while (ol_cnt < OL_CNT && !aborted) {
          val OL_I_ID: Int = OL_I_IDs.get(ol_cnt)
          val OL_QUANTITY: Int = OL_QUANTITIES.get(ol_cnt)
          val S_W_ID: Int = OL_SUPPLY_W_IDs.get(ol_cnt)

          if (txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.ITEM_TABLE, OL_I_ID), TPCCConstants.I_NAME_COL) == null) {
            aborted = true
          } else {

            if(O_ID == -1) {
              O_ID = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.DISTRICT_TABLE, W_ID, D_ID), TPCCConstants.D_NEXT_O_ID).asInstanceOf[Integer]

              txn.table(TPCCConstants.DISTRICT_TABLE).put(PrimaryKey.pkey(W_ID, D_ID), SerializableRowGenerator.column(TPCCConstants.D_NEXT_O_ID, O_ID+1))
            }

            val I_PRICE = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.ITEM_TABLE, OL_I_ID), TPCCConstants.I_PRICE_COL).asInstanceOf[Double]
            val I_DATA = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.ITEM_TABLE, OL_I_ID), TPCCConstants.I_DATA_COL).asInstanceOf[String]
            val I_NAME = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.ITEM_TABLE, OL_I_ID), TPCCConstants.I_NAME_COL).asInstanceOf[String]
            val S_DATA = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_IMMUTABLE, S_W_ID, OL_I_ID), TPCCConstants.S_DATA_COL).asInstanceOf[String]
            var currentStock = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_MUTABLE, S_W_ID, OL_I_ID), TPCCConstants.S_QUANTITY_COL).asInstanceOf[Integer]

            var brandGeneric: String = "B"
            if (I_DATA.contains("ORIGINAL") && S_DATA.contains("ORIGINAL")) {
              brandGeneric = "G"
            }
            val S_DIST_XX: String = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_IMMUTABLE, S_W_ID, OL_I_ID), TPCCConstants.formatSDistXX(D_ID)).asInstanceOf[String]
            val OL_AMOUNT: Double = OL_QUANTITY * I_PRICE
            totalAmount += OL_AMOUNT

            txn.table(TPCCConstants.ORDER_LINE_TABLE)
              .put(PrimaryKey.pkey(W_ID, D_ID, O_ID),
                SerializableRowGenerator.column(TPCCConstants.OL_QUANTITY_COL, OL_QUANTITY)
                  .column(TPCCConstants.OL_AMOUNT_COL, OL_AMOUNT)
                  .column(TPCCConstants.OL_I_ID_COL, OL_I_ID)
                  .column(TPCCConstants.OL_SUPPLY_W_ID_COL, S_W_ID)
                  .column(TPCCConstants.OL_DELIVERY_D_COL, null)
                  .column(TPCCConstants.OL_NUMBER_COL, ol_cnt)
                  .column(TPCCConstants.OL_DIST_INFO_COL, S_DIST_XX))

            if (currentStock > OL_QUANTITY + 10) {
              currentStock -= OL_QUANTITY
            }
            else {
              currentStock = (currentStock - OL_QUANTITY + 91)
            }

            var currentOrderCount = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_MUTABLE, S_W_ID, OL_I_ID), TPCCConstants.S_ORDER_CNT).asInstanceOf[Integer]
            var currentRemoteCount: Int = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_MUTABLE, S_W_ID, OL_I_ID), TPCCConstants.S_REMOTE_CNT).asInstanceOf[Integer]

            newOrderLines.add(new TPCCNewOrderLineResult(S_W_ID, OL_I_ID, I_NAME, OL_QUANTITY, currentStock, brandGeneric, I_PRICE, OL_AMOUNT))

            if (W_ID == S_W_ID) {
              currentOrderCount += 1
            }
            else {
              currentRemoteCount += 1
            }

            txn.table(TPCCConstants.STOCK_TABLE_MUTABLE).put(PrimaryKey.pkey(S_W_ID, OL_I_ID),
              SerializableRowGenerator.column(TPCCConstants.S_ORDER_CNT, currentOrderCount)
              .column(TPCCConstants.S_REMOTE_CNT, currentRemoteCount)
                .column(TPCCConstants.S_QUANTITY_COL, currentStock))

          }


          ol_cnt += 1
        }

        if (aborted) {
          txn.abort()

          p.success(new TPCCNewOrderResponse(false))
        } else {

          txn.table(TPCCConstants.NEW_ORDER_TABLE).put(PrimaryKey.pkey(W_ID, D_ID, O_ID),
            Row.NULL)

          txn.table(TPCCConstants.ORDER_TABLE).put(PrimaryKey.pkey(W_ID, D_ID, O_ID, OL_CNT),
            SerializableRowGenerator.column(TPCCConstants.O_C_ID_COL, C_ID).column(TPCCConstants.O_ENTRY_D, O_ENTRY_D)
            .column(TPCCConstants.O_CARRIER_ID_COL, null)
            .column(TPCCConstants.O_ALL_LOCAL_COL, if (allLocal) 1 else 0))

          txn.table(TPCCConstants.ORDER_TABLE).put(PrimaryKey.pkey(W_ID, D_ID, O_ID), SerializableRowGenerator.column(TPCCConstants.O_ID, O_ID))
          val writeFuture = txn.executeWrite

          writeFuture onComplete {
            case Success(_) => {

              val C_DISCOUNT = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.CUSTOMER_TABLE, W_ID, D_ID, C_ID), TPCCConstants.C_DISCOUNT_COL).asInstanceOf[Double]
              val W_TAX = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.WAREHOUSE_TABLE, W_ID), TPCCConstants.W_TAX_COL).asInstanceOf[Double]
              val D_TAX = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.DISTRICT_TABLE, W_ID, D_ID), TPCCConstants.D_TAX_COL).asInstanceOf[Double]
              totalAmount *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)

              val C_LAST: String = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.CUSTOMER_TABLE, W_ID, D_ID, C_ID), TPCCConstants.C_LAST_COL).asInstanceOf[String]
              val C_CREDIT: String = txn.getQueryResult(PrimaryKey.pkeyWithTable(TPCCConstants.CUSTOMER_TABLE, W_ID, D_ID, C_ID), TPCCConstants.C_CREDIT_COL).asInstanceOf[String]

              txn.commit()

              p success new TPCCNewOrderResponse(W_ID, D_ID, C_ID, O_ID, OL_CNT, C_LAST, C_CREDIT, C_DISCOUNT, W_TAX, D_TAX, O_ENTRY_D, totalAmount, newOrderLines)
            }
            case Failure(t) => {
              txn.abort()
              p.failure(t)
            }
          }
        }
      }
      case Failure(t) => {
                    txn.abort()
                    p.failure(t)
                  }
    }

    p.future
  }
}
