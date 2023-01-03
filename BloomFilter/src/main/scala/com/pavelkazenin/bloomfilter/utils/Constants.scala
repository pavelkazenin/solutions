package com.pavelkazenin.bloomfilter.utils

trait Constants {

  val MODE_GENERATE = "generate"
  val MODE_BUILD = "build"
  val MODE_FILTER = "filter"
  val MODE_STREAM = "stream"

  val INIT_DATA_PATH = "init_data_path"
  val INCR_DATA_PATH = "incr_data_path"

  val NUM_PARTITIONS = "partitions"
  val NUM_RECORDS = "records"

  val FILTER_PATH = "filter_path"
  val FILTER_FPP = "filter_fpp"
}
