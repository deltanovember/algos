package com.alluvia.strategies.dowdog

import java.util.Date
import com.alluvia.strategies.solo.Solo
import com.alluvia.markets.ASX
import com.alluvia.algo.datasource.{Historical, IressReplay}

object RunDowDog {
  def main(args: Array[String]) {
    println(new Date)
    new Historical with DowDog with ASX {
      val startDate: Date = "2011-01-01"
      val endDate: Date = "2011-11-11"    } run

  }
  println(new Date)
}