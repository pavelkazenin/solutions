package com.pavelkazenin.frequency

import scala.collection.{Map, mutable}

class ThetaDataContext {
    // application properties
    val props = mutable.HashMap.empty[String, String]
    
    def get( key: String ) = { props.get( key ).get }
    def put( key: String, value: String ) = { props.put( key, value ) }
}