package com.adidas.analytics.util

import org.apache.spark.sql.SaveMode

trait LoadMode {
  def sparkMode: SaveMode
}

object LoadMode {

  case object OverwriteTable extends LoadMode {
    override def sparkMode: SaveMode = SaveMode.Overwrite
  }

  case object OverwritePartitions extends LoadMode {
    override val sparkMode: SaveMode = SaveMode.Overwrite
  }

  case object OverwritePartitionsWithAddedColumns extends LoadMode {
    override val sparkMode: SaveMode = SaveMode.Overwrite
  }

  case object AppendJoinPartitions extends LoadMode {
    override def sparkMode: SaveMode = SaveMode.Append
  }

  case object AppendUnionPartitions extends LoadMode {
    override def sparkMode: SaveMode = SaveMode.Append
  }
}
