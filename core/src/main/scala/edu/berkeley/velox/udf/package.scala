package edu.berkeley.velox

import edu.berkeley.velox.storage.StorageManager

package object udf {
  type PerPartitionUDF = ((StorageManager) => Any)
}
