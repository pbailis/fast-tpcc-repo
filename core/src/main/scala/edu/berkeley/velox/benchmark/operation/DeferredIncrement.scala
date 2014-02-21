package edu.berkeley.velox.benchmark.operation

import edu.berkeley.velox.datamodel.PrimaryKey

case class DeferredIncrement(val counterKey: PrimaryKey,
                             val counterColumn: Integer,
                             val destinationKey: PrimaryKey,
                             val destinationColumn: Integer)
