package com.pavelkazenin.bloomfilter.utils

import scala.collection.mutable

class AppContext extends Serializable {
  // application properties
  val props = mutable.HashMap.empty[String, String]

  def get(key: String) = {
    props.get(key).get
  }

  def put(key: String, value: String) = {
    props.put(key, value)
  }
}
