package com.alluvia.strategies.solo

import com.alluvia.markets.ASX
import java.util.Date
import com.alluvia.algo.datasource.{Iress, IressReplay, Historical}
import com.alluvia.algo.{OrderRouterClient, FIX}

//
object RunSolo {
  def main(args: Array[String]) {

    new IressReplay with Solo with ASX  {
      val startDate: Date = "2011-01-05"
      val endDate: Date = "2011-12-10"
    } run

  }
}