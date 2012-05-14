package com.alluvia.strategies.irev

import com.alluvia.markets.ASX
import java.util.Date
import com.alluvia.algo.datasource.{Iress, IressReplay, Historical}
import com.alluvia.algo.FIX

object RunIRev {

  def main(args: Array[String]) {

    new IressReplay with ASX with IRev {
      val startDate: Date = "2011-11-28"
      val endDate: Date = "2011-11-28"
    } run

  }

}