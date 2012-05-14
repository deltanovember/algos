package com.alluvia.strategies.sunset

/**
 * Sunset runner - historical and live
 *
 * Checklist
 * 1. Make sure all CSV files are closed
 * 2. Make sure command prompt is not "paused"
 */
import java.util.Date
import com.alluvia.markets.{LSE, ASX}
import com.alluvia.algo._
import datasource._

object RunSunsetEvent extends TypeConverter {

  // DN test checkin
// IressReplay
  // Historical
  def main(args: Array[String]) {
    new Historical with SunsetEvent with ASX {
      val startDate: Date = "2011-01-01"
      val endDate: Date = "2011-12-31"
    }.run

  }

}