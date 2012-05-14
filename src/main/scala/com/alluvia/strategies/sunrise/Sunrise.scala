package com.alluvia.strategies.sunrise

import com.alluvia.algo.BackTestingAlgo
import java.util.ArrayList
import java.util.Date
import java.util.LinkedList

import com.alluvialtrading.lib.DateIterator
import com.alluvialtrading.data.{Trade, Regression}

import scala.math.abs

abstract class Sunrise extends BackTestingAlgo {


  private val debug: Boolean = false
    private val tradingDates: LinkedList[String] = new LinkedList[String]
    private[sunrise] val startDate: Date = convertSmartsDateTime("1/3/2010 09:00:00.000")
    private[sunrise] val endDate: Date = convertSmartsDateTime("31/3/2010 16:00:00.000")
    // Opening prices for current day
    private val overnightChanges = new java.util.Hashtable[String, java.lang.Double]
    private final val MAX_OVERNIGHT_MOVE: Double = 0.02


    override def algoStart {
      super.algoStart
    }

    override def algoBody {
      loadStocks()
      loadTradingDates()
      loadOvernightChanges()
      val iterator = new DateIterator(startDate, endDate)
      while (iterator.hasNext) {
        val date: Date = iterator.next
        if (isTradingDate(date)) {

          val allStocks = getAllTradedSecurities(dateToISODateString(date));
           for (stock: String <- allStocks) {
            val todayDate: String = dateToSmartsDateString(date)
            if (null != overnightChanges.get(todayDate + "/" + stock)) {
              val todayChange: Double = overnightChanges.get(todayDate + "/" + stock)
              val todayIndex: Double = overnightChanges.get(todayDate + "/AORD")
              val indexChanges = new ArrayList[java.lang.Double]
              val stockChanges = new ArrayList[java.lang.Double]
              val index: Int = tradingDates.indexOf(todayDate)
              val numDays: Int = 30

              var i = index
              while (i >= index - numDays) {
                val currentDate: String = tradingDates.get(i)
                if (null != overnightChanges.get(currentDate + "/AORD") && null != overnightChanges.get(currentDate + "/" + stock)) {
                  indexChanges.add(overnightChanges.get(currentDate + "/AORD"))
                  stockChanges.add(overnightChanges.get(currentDate + "/" + stock))
                }
                else {
                }
                i -= 1

              }
              if (stockChanges.size > 25) {
                val regression: Regression = getRegression(toDoubleArray(indexChanges), toDoubleArray(stockChanges), todayIndex, 0.9)
                System.out.println(debug)
                if (debug) {
                  System.out.println("stock")
                  for (change <- toDoubleArray(stockChanges)) {
                    System.out.print(change * 100 + ",")
                  }
                  System.out.println("index")
                  for (change <- toDoubleArray(indexChanges)) {
                    System.out.print(change * 100 + ",")
                  }
                }
                val lowerBound: Double = regression.getLowerBound
                val upperBound: Double = regression.getUpperBound
                val tradeInfo: String = todayDate + "," + stock + "," + todayChange + "," + lowerBound + "," + upperBound + "," + todayIndex + "," + regression.getSlope + "," + regression.getrSquared

                val openTrade: Trade = getOpeningTrade(stock, dateToISODateString(date))
                if (null != openTrade) {
                  val price: Double = openTrade.getPrice
                  var volume: Int = (0.15 * openTrade.getVolume).asInstanceOf[Int]
                  val minVolume: Boolean = price * abs(volume) > MIN_TRADE_VALUE
                  val minRSquared: Boolean = regression.getrSquared > 0.3
                  var signal: String = ""
                  val day: String = dateToDay(date)
                  val monday: Boolean = day == "Mon"
                  val excessiveMovement: Boolean = abs(todayChange) > MAX_OVERNIGHT_MOVE
                  val trade: Boolean = minVolume && minRSquared && !monday && !excessiveMovement
                  if (price * abs(volume) > MAX_TRADE_VALUE) {
                    volume = (MAX_TRADE_VALUE / price).asInstanceOf[Int]
                  }
                  if (todayChange < lowerBound && trade) {
                    signal = "BUY"
                    addTradingRecord(signal + "," + tradeInfo)
                  }
                  else if (todayChange > upperBound && trade) {
                    signal = "SELL"
                    addTradingRecord(signal + "," + tradeInfo)
                  }
                  val sign = if ("SELL" == signal) -1 else if ("BUY" == signal) 1 else 0

                  volume = volume * sign
                  if (0 != sign) {
                    profitTrack(dateToISODateTimeString(openTrade.getDate), stock, volume, price, "60", Trade.EXIT_VWAP)
                  }
                }
              }
            }
          }
        }
      }
    }

    override def algoEnd {
      super.algoEnd
    }

    override protected def getCSVHeader: String = {
      "Signal,Date,Stock,Overnight,Lower,Upper,IndexChange,Beta,RSquared"
    }

    override def init {
      super.init

    }

    def isValidTradingDate(date: Date): Boolean = {
      tradingDates.indexOf(dateToSmartsDateString(date)) > 0
    }

    override def loadStocks() {
      val stocks: Array[String] = openFile("data", "allstocks.csv")
      for (stock <- stocks) {
        allStocks.add(stock.trim.split("\\.")(0))
      }
    }

    def loadOvernightChanges() {
      val overnight: Array[String] = openFile("data", "openclose.csv")
      for (price <- overnight) {
        val split: Array[String] = price.split(",")
        if (split(2).length > 0 && split(3).length > 0) {
          val date: String = split(0)
          val security: String = split(1).trim
          val yesterdayClose: Double = split(2).toDouble
          val todayOpen: Double = split(3).toDouble
          val delta: Double = (todayOpen - yesterdayClose) / yesterdayClose
          overnightChanges.put(date + "/" + security, delta)
        }
      }
      val index: Array[String] = openFile("data", "index.csv")
      for (price <- index) {
        val split: Array[String] = price.split(",")
        val date: String = split(0)
        val security: String = "AORD"
        val yesterdayClose: Double = split(1).toDouble
        val todayOpen: Double = split(2).toDouble
        val delta: Double = (todayOpen - yesterdayClose) / yesterdayClose
        overnightChanges.put(date + "/" + security, delta)
      }
      val dummy = 10
    }

    def loadTradingDates() {
      val dates: Array[String] = openFile("data", "index.csv")
      for (date <- dates) {
        tradingDates.add(date.split(",")(0))
      }
    }
  }