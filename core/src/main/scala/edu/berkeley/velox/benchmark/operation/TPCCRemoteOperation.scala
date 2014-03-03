package edu.berkeley.velox.benchmark.operation

import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.datamodel.{Row, PrimaryKey}
import edu.berkeley.velox.benchmark.TPCCConstants
import edu.berkeley.velox.rpc.Request
import java.util

/**
 * Created by pbailis on 3/2/14.
 */

trait RemoteOperation {
  def getWarehouse(): Int
  def execute(storage: StorageEngine) : RemoteOperationResponse
}

trait RemoteOperationResponse {
  def depositResults(resultsMap: util.Map[PrimaryKey, Row])
}

case class TPCCReadStock(W_ID: Int, D_ID: Int, OL_I_ID: Int) extends RemoteOperation with Request[TPCCReturnStock] {
  def getWarehouse = { W_ID }

  def execute(storage: StorageEngine) = {
    val m_row = storage.get(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_MUTABLE, W_ID, OL_I_ID))
    val i_row = storage.get(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_IMMUTABLE, W_ID, OL_I_ID))

    new TPCCReturnStock(W_ID,
                        D_ID,
      OL_I_ID,
                        m_row.readColumn(TPCCConstants.S_ORDER_CNT).asInstanceOf[Int],
                        m_row.readColumn(TPCCConstants.S_REMOTE_CNT).asInstanceOf[Int],
                        m_row.readColumn(TPCCConstants.S_QUANTITY_COL).asInstanceOf[Int],
                        i_row.readColumn(TPCCConstants.S_DATA_COL).asInstanceOf[String],
                        i_row.readColumn(TPCCConstants.formatSDistXX(D_ID)).asInstanceOf[String])
  }
}

case class TPCCReturnStock(W_ID: Int,
                           D_ID: Int,
                           OL_I_ID: Int,
                           ORDER_CNT: Int,
                           REMOTE_CNT: Int,
                           S_QUANTITY: Int,
                           S_DATA_COL: String,
                           DIST_XX: String) extends RemoteOperationResponse {

  def depositResults(resultsMap: util.Map[PrimaryKey, Row]) {
    resultsMap.put(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_IMMUTABLE, W_ID, OL_I_ID),
      Row.column(TPCCConstants.S_DATA_COL, S_DATA_COL).column(TPCCConstants.formatSDistXX(D_ID), DIST_XX))
    resultsMap.put(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_MUTABLE, W_ID, OL_I_ID),
      Row.column(TPCCConstants.S_ORDER_CNT, ORDER_CNT)
        .column(TPCCConstants.S_REMOTE_CNT, REMOTE_CNT)
        .column(TPCCConstants.S_QUANTITY_COL, S_QUANTITY))
  }

}

case class TPCCUpdateStock(W_ID: Int, I_ID: Int, currentOrderCount: Int, currentRemoteCount: Int, currentStock: Int) extends RemoteOperation with Request[TPCCUpdateStockResponse] {
  def getWarehouse = { W_ID }

  def execute(storage: StorageEngine) = {
     storage.put(PrimaryKey.pkeyWithTable(TPCCConstants.STOCK_TABLE_MUTABLE, W_ID, I_ID),
                 Row.column(TPCCConstants.S_ORDER_CNT, currentOrderCount)
                    .column(TPCCConstants.S_REMOTE_CNT, currentRemoteCount)
                    .column(TPCCConstants.S_QUANTITY_COL, currentStock))
      new TPCCUpdateStockResponse
   }
}

class TPCCUpdateStockResponse extends RemoteOperationResponse {
  def depositResults(resultsMap: util.Map[PrimaryKey, Row]) {}
}

