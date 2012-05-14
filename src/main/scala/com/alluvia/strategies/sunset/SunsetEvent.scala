package com.alluvia.strategies.sunset

import scala.math.{abs, log}
import com.alluvia.algo.EventAlgo
import java.util.Date
import com.alluvialtrading.fix.OrderSide._
import com.alluvia.types.MagicMap
import collection.mutable.{HashSet, HashMap, ListBuffer}
import com.alluvia.types.market._
import fix._
import com.alluvialtrading.fix.OrderSide

trait SunsetEvent
  extends EventAlgo {

  // User params
  val isExpiry = false // true or false

  override def benchmarkDays = 0
  val blacklist = ListBuffer("TAH", "KCN")

  // Thresholds
  val thresholdTimeBeforeAuction = 65.minutes // minutes
  val thresholdTcountBeforeAuctionMin = 20 // x
  val thresholdValueBeforeAuctionMin = 20000.0 // $
  val thresholdLastSpreadMax = 3.0 // %
  val thresholdDeltaMin = 0.05 // %
  // Min delta for trade used purely for hedging
  val thresholdHedgeDeltaMin = 0.05 // %
  val thresholdDeltaMax = 10.0 // %
  val thresholdDeltaRelativeToLastSpreadMin = 49.0 // %
  val thresholdSmallTradeRelativeToAuction = 0.15 // %
  val thresholdUncrossingValueMin = 30000.0 // $
  var thresholdtradingRangeBeforeAuctionMax = 2.5 // %
  val thresholdWeeklyTradingRange = 10.0 // %
  override def maxOrders = 75
  val thresholdCancellations = maxOrders // natural number
  val thresholdMaxOpenTrades = 10

  // Position limits
  val singlePositionSizeMax = 8000 // $
  val singlePositionSizeMin = 8000 // $
  val singlePositionSizeRelativeToUncrossingValueMax = 10 // %

  // Directional exposure
  val maxDirectionalExposure = 2 // number of positions

  // Recent trades
  val recentTrades = MagicMap[String](new ListBuffer[SunsetTrade])

  case class SunsetTrade(security: String, date: Date, price: Double, volume: Double, value: Double, spreadBeforePercent: Double)

  // Consider security to trade
  val considerSecurity = MagicMap[String](false)
  val staticTestsDone = MagicMap[String](false)
  val maxPriceBeforeAuction = MagicMap[String](Double.NaN)
  val minPriceBeforeAuction = MagicMap[String](Double.NaN)
  val tcountBeforeAuction = MagicMap[String](0)
  val valueBeforeAuction = MagicMap[String](0.0)

  // Open trades
  //val outstandingDeltas = new HashMap[String, Double]
  var lastTradeTime = new Date(0)
  var cancelCount = 0

  // Orders per second
  var ordersInLastTwoSeconds = 0
  var lastSecond = 0
  val maxOrdersPerTwoSeconds = if (isHistorical) 10 else 2

  // Cancellation gaps
  val timeOfCancel = MagicMap[String](new Date(0) - 365.day)
  val minTimeAfterCancel = 2.seconds

  // High low
  case class Range(high: Double, low: Double) {
    def getRange = (high - low) / low * 100
  }

  val highLow = new HashMap[String, Range]
  val excessiveWeeklyRange = new HashMap[String, Boolean]

  // Backtesting hack
  val pendingQuoteMatch = new HashSet[QuoteMatch]

  def getServerRoutingKey = "sunset"

  override def onStart(s: Start) {
    //val order = submitLimitOrder(OrderSide.SELL, "ALL.AX", 20, 20)
  }

  // ================================================================================
  // ON TRADE
  // ================================================================================

  override def onTrade(t: Trade) {

    // Profit tracking - morning exit
    if (performExit &&
      t.date < closeTime &&
      getOpenSecurities.contains(t.security)) {
      val order = getOrderBySymbol(t.security)
      val side = order.getSide match {
        case BUY => SELL
        case SELL => BUY

      }
      println("closing position")
      submitLimitOrder(side, order.getSymbol, t.price, order.getQuantity)

    }

    // if (security == "BHP") println(date, security, price, volume)
    if ((closeTime - t.date) > thresholdTimeBeforeAuction) return

    // Store recent trading information
    recentTrades(t.security).append(SunsetTrade(t.security, t.date, t.price, t.volume, t.value, t.spreadBeforePercent))

    // Consider security
    considerSecurity(t.security) = true

  }

  override def onQuote(q: Quote) {
    if (q.security == "JBH") {
     // printcsv("sunsetquotes.csv", q.date, q.security, q.bidOrAsk, q.bid, q.ask)
    }
  }

  // ================================================================================
  // ON QUOTE MATCH
  // ================================================================================

  var lastCopy = 0L

  override def onQuoteMatch(q: QuoteMatch) {

    // No duplication with Solo
    if (q.price < 0.55) return
    
    printcsv("sunset.csv", q.security, q.date, q.price, q.volume, q.value, q.bidBefore, q.askBefore)

    if (q.date.hours < closeTime.hours) return

        val currentSecond = q.date.seconds
    // Reset every two seconds
    if (currentSecond != lastSecond && currentSecond % 2 == 0) {
      lastSecond = currentSecond
      ordersInLastTwoSeconds = 0
    }

    if (q.security == "WPL" &&
    q.date.toTimeStr > "16:10:02") {
      println(q.date.hours)
      println(closeTime.hours)
      println("debug")
    }
    // Apply static filter rules
    // ------------------------------------------------------------

    // IF not considering current security THEN break
    if (!staticTestsDone(q.security) && considerSecurity(q.security)) {

      // No longer first onQuoteMatch object for current security
      staticTestsDone(q.security) = true

      def blacklist(filter: => Boolean) = () => {

        if (filter) {
          considerSecurity(q.security) = false
          true
        }
        else
          false
      }
      val filters = List(
        // IF intraday auction or bad timestamp THEN break
        blacklist(q.date.before(closeTime)),
        // IF data is undefined THEN break
        blacklist(undefined(q.askBefore) || undefined(q.bidBefore) || undefined(q.price)),
        // IF not shortable THEN break
        blacklist {
          //println("*")
          !isShortable(q.security)
        },
        // IF last spread was too large or invalid THEN break
        blacklist(q.spreadBeforePercent > thresholdLastSpreadMax || q.spreadBeforePercent < 0),
        // IF insufficient trade count before auction THEN break
        blacklist {
          tcountBeforeAuction(q.security) = recentTrades(q.security).size
          tcountBeforeAuction(q.security) < thresholdTcountBeforeAuctionMin
        },
        // IF insufficient value before auction THEN break
        blacklist {
          valueBeforeAuction(q.security) = recentTrades(q.security).map(_.value).sum
          valueBeforeAuction(q.security) < thresholdValueBeforeAuctionMin
        },
        // Range checks
        blacklist {
          maxPriceBeforeAuction(q.security) = recentTrades(q.security).map(_.price).max
          minPriceBeforeAuction(q.security) = recentTrades(q.security).map(_.price).min
          val tradingRangeBeforeAuction = 100 * log(maxPriceBeforeAuction(q.security) / minPriceBeforeAuction(q.security))
          tradingRangeBeforeAuction > thresholdtradingRangeBeforeAuctionMax
        }
      )

      if (filters.exists(x => x())) return

    }


    // Apply dynamic filter rules
    // ------------------------------------------------------------

    if (!considerSecurity(q.security)) return

    // Calculate auction price change
    val auctionDelta = 100 * log(q.price / q.midBefore)

    // Buying with too many buys or selling with too many sells
    def currentOrderImbalance = (numBuys >= numSells && auctionDelta < 0) ||
                                  (numSells >= numBuys && auctionDelta > 0)

    if (hasOpenOrder(q.security) &&
    auctionDelta != getOrderBySymbol(q.security).getScore) {
      println("Updating delta", q.security, auctionDelta)
      getOrderBySymbol(q.security).setScore(auctionDelta)
    }
    def filterRemove(filter: => Boolean) = () => {
      if (filter) {
        //println("Failed filter" + q.date)
        removeOrder(q.security, q)
        true
      }
      else
        false
    }


    val filters = List(
      // IF auction value is insufficient THEN break
      filterRemove(q.value < thresholdUncrossingValueMin),
      // IF auction price change exceeds maximum threshold THEN break
      filterRemove(abs(auctionDelta) > thresholdDeltaMax),
      // IF auction price change does not exceed threshold and would create further imbalance
      filterRemove(abs(auctionDelta) < thresholdDeltaMin && currentOrderImbalance),
      // IF  auction price does not exceed even hedging threshhold break
      filterRemove(abs(auctionDelta) < thresholdHedgeDeltaMin),
      // Blacklist
      filterRemove(blacklist.contains(q.security)),
      // IF auction price change is not unusual relative to the last spread THEN break
      filterRemove(abs(auctionDelta) < (thresholdDeltaRelativeToLastSpreadMin / 100) * q.spreadBeforePercent),
      // IF close price is not unusual relative to pre-auction activity THEN break
      filterRemove((auctionDelta > 0 && q.price < minPriceBeforeAuction(q.security)) ||
        (auctionDelta < 0 && q.price > maxPriceBeforeAuction(q.security))),
      // Minimum close value
      filterRemove {
        val tradeVolume = (q.volume * singlePositionSizeRelativeToUncrossingValueMax / 100).toInt
        tradeVolume * q.price < singlePositionSizeMin
      }
    )

    // Any true then return
    if (filters.exists(x => x())) return

    // ================================================================================
    // SEND ORDERS JUST BEFORE UNCROSSING
    // ================================================================================

    // Send Orders
    // ------------------------------------------------------------

    if (ordersInLastTwoSeconds < maxOrdersPerTwoSeconds &&
      // Time throttling
      q.date - timeOfCancel(q.security) > minTimeAfterCancel &&
      // catch bad data
      q.date > openTime && q.date < uncrossTime &&
      // near end auction
      auctionNearFinishTime - q.date < 0.seconds) {

      lastTradeTime = q.date
      if (currentTradesAttemptedToday > maxOrders) {
        println("Max trades exceeded")
        return
      }


      val orderLimitBreached = getOpenSecurities.size >= thresholdMaxOpenTrades

      // Hedging rules
      val mustCancel = if (numBuys >= numSells + maxDirectionalExposure && auctionDelta < 0) true
      else if (numSells >= numBuys + maxDirectionalExposure && auctionDelta > 0) true
      else if (orderLimitBreached) true
      else false

      val securityOnly = q.security
      val securityWithExchange = securityOnly + "." + getSecurityExchange
      val direction = if (auctionDelta > 0) SELL else BUY

      if (!hasOpenOrder(securityOnly)) {
        var tradePrice = 0.0

        val threshold = if (math.abs(auctionDelta) > thresholdDeltaMin) thresholdDeltaMin else thresholdHedgeDeltaMin

        if (auctionDelta < 0) {
          tradePrice = q.midBefore * (1 - threshold / 100)
          if (tradePrice > q.bidBefore) tradePrice = q.bidBefore
        }
        else {
          tradePrice = q.midBefore * (1 + threshold / 100)
          if (tradePrice < q.askBefore) tradePrice = q.askBefore
        }

        var tradeVolume = (q.volume * singlePositionSizeRelativeToUncrossingValueMax / 100).toInt
        if (tradeVolume * q.price > singlePositionSizeMax) tradeVolume = (singlePositionSizeMax / q.price).toInt

        // Fire FIX orders
        val minTick = getMinTickSize(tradePrice, securityWithExchange)
        if (!defined(minTick)) return

        // Is trade volume small relative to auction
        val smallVolume = tradeVolume < thresholdSmallTradeRelativeToAuction * q.volume


        if (q.security == "BLD" && q.date.toTimeStr > "16:10:08") {
          println("debug", tradeVolume,  thresholdSmallTradeRelativeToAuction * q.volume)
          val temp = 5
        }

        // Price priority optimisation
        tradePrice = {
          if (direction == SELL && smallVolume) forceRoundDown(tradePrice.toString, minTick.toString)
          else if (direction == SELL) forceRoundUp(tradePrice.toString, minTick.toString)
          else if (direction == BUY && smallVolume) forceRoundUp(tradePrice.toString, minTick.toString)
          else forceRoundDown(tradePrice.toString, minTick.toString)
        }

        var doTrade = false

        if (!mustCancel) {
          doTrade = true
        }
        // negative deltas
        else if (auctionDelta < 0) {
          // forced to buy
          val worst = if (numBuys >= numSells + maxDirectionalExposure) getAllOrders.filter(x => x.getScore < 0).maxBy(x => x.getScore)
            // find absolute0c worst
          else getAllOrders.minBy(x => abs(x.getScore))
          if (abs(worst.getScore) < abs(auctionDelta)) {
            // replace
            println("Replacing", worst.getTicker, worst.getScore)
            removeOrder(worst.getTicker, q)
            doTrade = true
          }
          else {
            println("delta too small", auctionDelta, "BUY")
          }
        }
        else if (auctionDelta > 0) {
          val worst =  if (numSells >= numBuys + maxDirectionalExposure) getAllOrders.filter(x => x.getScore > 0).minBy(x => x.getScore)
            // find absoulte worst
          else getAllOrders.minBy(x => abs(x.getScore))

          if (abs(worst.getScore) < abs(auctionDelta)) {
            // replace
            println("Replacing", worst.getTicker, worst.getScore)
            removeOrder(worst.getTicker, q)
            doTrade = true
          }
          else {
            println(numSells, numBuys)
            println("delta too small", auctionDelta, "SELL")
          }
        }

        if (doTrade) {
          println("Trading: " + q.date + " " + direction + " " + securityWithExchange + " " + tradePrice + " " + tradeVolume + " " + auctionDelta + " " + q.bidBefore + " " + q.askBefore + " " + q.price)
          submitLimitOrder(direction, securityWithExchange, tradePrice, tradeVolume, auctionDelta)
          ordersInLastTwoSeconds += 1

        }
        else if (isHistorical) {
          pendingQuoteMatch.add(q)
        }
        printOrders()

        return
      }
      // Do we need to reverse directions?
      else if (hasOpenOrder(securityOnly) && getOrderBySymbol(securityOnly).getSide != direction) {
        removeOrder(securityOnly, q)
      }

    }


  }


  override def onDayEnd(e: DayEnd) {

    // In historical mode sort out missed trades due to bad ordering
    if (isHistorical) {
      //            if (pendingQuoteMatch.contains(q)) {
      //              println("qm found")
      //              pendingQuoteMatch.remove(q)
      //            }
      println("eod", pendingQuoteMatch.size)
      pendingQuoteMatch.foreach(x => onQuoteMatch(x))
    }
    pendingQuoteMatch.clear()
  }
  override def onDayStart(d: DayStart) {
    //  println("starting day" + q.date)
    println("Max orders: " + maxOrders)
    println("Browser print: " + printToBrowser)
    val tooVolatile = highLow.keys.toList.filter(key => highLow(key).getRange < thresholdWeeklyTradingRange)
    tooVolatile.foreach {
      sec => if (isShortable(sec)) println("Too volatile: " + sec, highLow(sec).high, highLow(sec).low)
      excessiveWeeklyRange.put(sec, true)

    }

    // Reset
    recentTrades.clear()
    considerSecurity.clear()
    staticTestsDone.clear()
    maxPriceBeforeAuction.clear()
    minPriceBeforeAuction.clear()
    tcountBeforeAuction.clear()
    valueBeforeAuction.clear()
    cancelCount = 0
    highLow.clear()
    excessiveWeeklyRange.clear()
    timeOfCancel.clear()
  }

  override def onFIXCancel(f: FIXCancel) {
    printcsv("data\\sunsetcancel.csv", f.orderID)
  }
  override def onFIXHeartbeat() {
    //println("Sunset has received FIX heartbeat")
  }

    override def onFIXFill(f: FIXFill) {
    val order = getAnyOrderByID(extractFIX(f.clOrdID))
    appendcsv(getServerRoutingKey + ".temp", (new Date).toIso, f.security, f.price, f.volume, f.orderID, f.clOrdID, order.getSymbol, order.getSide)
  }
  override def onFIXPartialFill(f: FIXPartialFill) {
    val order = getAnyOrderByID(extractFIX(f.clOrdID))
    appendcsv(getServerRoutingKey + ".temp", (new Date).toIso, f.security, f.price, f.volume, f.orderID, f.clOrdID, order.getSymbol, order.getSide)
  }

  override def onFIXReject(f: FIXReject) {
    val order = getAnyOrderByID(extractFIX(f.clOrdID))
    println("sunsetreject.csv", f.rejectReason, f.clOrdID, order.getTicker)
    printcsv("sunsetreject.csv", f.rejectReason, f.clOrdID, order.getTicker)
    blacklist += order.getTicker
  }

  override def onFIXNew(f: FIXNew) {
    //println("FIXNew received", f.security, f.price, f.volume)
    printcsv("data\\sunsetnew.csv", f.orderID, f.security, f.price, f.volume)
  }

  def printOrders() {
    println("Orders: ")
    getAllOrders.sortBy(x => (x.getScore)).foreach(x => println(x.getSymbol, x.getSide, x.getTicker, x.getScore, x.getLimit))
  }

  // use QuoteMatch as clock
  def removeOrder(security: String, q: QuoteMatch) {
    super.removeOrder(security)
    //outstandingDeltas.remove(security)
    timeOfCancel.put(security, q.date)
  }

}

