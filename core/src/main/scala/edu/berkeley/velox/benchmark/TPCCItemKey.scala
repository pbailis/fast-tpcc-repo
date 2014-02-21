package edu.berkeley.velox.benchmark

import edu.berkeley.velox.datamodel.PrimaryKey

class TPCCItemKey extends PrimaryKey {
  def w_id: Int = {
    keyColumns(0)
  }
}

