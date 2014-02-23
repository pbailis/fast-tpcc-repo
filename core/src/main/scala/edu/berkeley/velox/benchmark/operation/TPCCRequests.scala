package edu.berkeley.velox.benchmark.operation

import java.util
import edu.berkeley.velox.rpc.Request

class TPCCLoadRequest(val W_ID: Int) extends Request[TPCCLoadResponse]

class TPCCLoadResponse

case class TPCCNewOrderRequest(val W_ID: Int,
                               val D_ID: Int,
                               val C_ID: Int,
                               val OL_I_IDs: util.ArrayList[Int],
                               val OL_SUPPLY_W_IDs: util.ArrayList[Int],
                               val OL_QUANTITIES: util.ArrayList[Int],
                               val serializable: Boolean = false) extends Request[TPCCNewOrderResponse]

class TPCCNewOrderResponse(val W_ID: Int,
                           val D_ID: Int,
                           val C_ID: Int,
                           val O_ID: Int,
                           val O_OL_CNT: Int,
                           val C_LAST: String,
                           val C_CREDIT: String,
                           val C_DISCOUNT: Double,
                           val W_TAX: Double,
                           val D_TAX: Double,
                           val O_ENTRY_D: String,
                           val total_amount: Double,
                           val orderLineList: util.ArrayList[TPCCNewOrderLineResult],
                           var committed: Boolean = true) {

  def this(commit: Boolean) {
    this(-1, -1, -1, -2, -1, "", "", -1, -1, -1, "", -1, null, committed = false)
    assert(!committed)
  }
}

case class TPCCNewOrderLineResult(val OL_SUPPLY_W_ID: Int,
                                  val OL_I_ID: Int,
                                  val I_NAME: String,
                                  val OL_QUANTITY: Int,
                                  val S_QUANTITY: Int,
                                  val brand_generic: String,
                                  val I_PRICE: Double,
                                  val OL_AMOUNT: Double)
