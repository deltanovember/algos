package com.alluvia.strategies.quarb

import com.alluvia.strategies.sunset.SunsetEvent
import com.alluvia.markets.ASX
import java.util.Date
import com.alluvia.algo.datasource.{Historical, Iress}

object RunQuarb {
  def main(args: Array[String]) {
    new Historical with Quarb with ASX {
      val startDate: Date = "2011-09-14"
      val endDate: Date = "2011-09-14"    } run

  }

}