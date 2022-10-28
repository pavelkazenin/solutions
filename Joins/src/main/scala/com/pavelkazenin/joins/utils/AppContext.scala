package com.pavelkazenin.joins.utils

import scala.collection.{mutable, Map}

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
