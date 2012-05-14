package com.alluvia.strategies.solo

import com.alluvia.algo.EventAlgo
import com.alluvialtrading.fix.OrderSide._
import com.alluvialtrading.fix.OrderSide
import com.alluvia.types.MagicMap
import java.util.Date
import com.alluvia.types.market._
import fix.{FIXReject, FIXPartialFill, FIXNew, FIXFill}
import collection.mutable.{ListBuffer, HashSet}

trait Solo extends EventAlgo {

  val openingTrade = MagicMap[String](0.0)
  val orderHistory = MagicMap[String](0)
  val blacklist = ListBuffer("HST", "PMP", "OMH", "ELD", "MMX", "NXS", "KZL", "CVN")

  // If we cancel, ignore next message
  val quotesSinceCancel = MagicMap[String](0)
  val timeOfCancel = MagicMap[String](new Date(0))
  val lastQuotePrice = MagicMap[String](0.0)
  val messageGapAfterCancel = 2
  val minTimeAfterCancel = 3.seconds

  // Position limits
  var singlePositionSizeMax = 8000 // $
  var singlePositionSizeMin = 8000 // $
  var singlePositionSizeRelativeToUncrossingValueMax = 15 // %

  // Thresholds
  val thresholdMaxOpenTrades = 8
  val maxOrdersPerSecond = 2

  // Balance buys and sells?
  val hedge = true
  val maxHedgeImbalance = 4
  var lastTradeTime = new Date(0)

  // Timer variables
  var ordersInLastSecond = 0
  var lastSecond = 0

  def validSecurity(price: Double, security: String) = price < 0.55 && price > 0.1 && isShortable(security)

  def getServerRoutingKey = "solo"
  def removeOrder(q: QuoteMatch) {
    super.removeOrder(q.security)
    quotesSinceCancel.put(q.security, 0)
    timeOfCancel.put(q.security, q.date)
  }
  override def onDayStart(d: DayStart) {
    println("clearing", d.dateTime)
    openingTrade.clear()
    quotesSinceCancel.clear
    timeOfCancel.clear
    lastQuotePrice.clear
    orderHistory.clear
  }

  override def onFIXFill(f: FIXFill) {
    val order = getAnyOrderByID(extractFIX(f.clOrdID))
    appendcsv("Solo.temp", (new Date).toIso, f.security, f.price, f.volume, f.orderID, f.clOrdID, order.getSymbol, order.getSide)
  }
  override def onFIXPartialFill(f: FIXPartialFill) {
    val order = getAnyOrderByID(extractFIX(f.clOrdID))
    appendcsv("Solo.temp", (new Date).toIso, f.security, f.price, f.volume, f.orderID, f.clOrdID, order.getSymbol, order.getSide)
  }
  override def onFIXReject(f: FIXReject) {
    val order = getAnyOrderByID(extractFIX(f.clOrdID))
    println("soloreject.csv", f.rejectReason, f.clOrdID, order.getTicker)
    printcsv("data\\soloreject.csv", f.rejectReason, f.clOrdID, order.getTicker)
    blacklist += order.getTicker
  }
  override def onFIXNew(f: FIXNew) {
    println("FIXNew received", f.security, f.price, f.volume)
  }

  override def onQuoteMatch(q: QuoteMatch) {

    //if (q.date > closeTime && q.price >= 0.1 && q.price < 0.5) printcsv("solo_all.csv", q.date, q.security, q.price, q.volume, q.value)
    if (validSecurity(q.price, q.security) &&
      q.date > closeTime) {
      printcsv("solo.csv", q.date, q.security, q.price, q.volume, q.value)
      lastQuotePrice.put(q.security, q.price)
      quotesSinceCancel.put(q.security, quotesSinceCancel(q.security) + 1)
    }
    if (q.date.hours < closeTime.hours) return

    // Use midBefore to eliminate any possible bias
    def direction = if (q.price >= q.midBefore) SELL else BUY
    def excessBuys = numBuys - maxHedgeImbalance >= numSells && direction == BUY
    def excessSells = numSells - maxHedgeImbalance >= numBuys && direction == SELL
    def timeSinceLastTrade = q.date - lastTradeTime
    def filterRemove(filter: => Boolean) = () => {
      if (filter) {
        //println("Filter failed, removing: " + q.date)
        removeOrder(q)
        true
      }
      else
        false
    }

    val currentSecond = q.date.seconds
    if (currentSecond != lastSecond) {
      lastSecond = currentSecond
      ordersInLastSecond = 0
    }
    val validTime = auctionNearFinishTime - q.date < 15.seconds

    val securityWithExchange = q.security + "." + getSecurityExchange
    var tradeVolume = (q.volume * singlePositionSizeRelativeToUncrossingValueMax / 100).toInt - 1
    if (tradeVolume * q.price > singlePositionSizeMax) tradeVolume = (singlePositionSizeMax / q.price).toInt

    var tradePrice = q.price
    val minTick = getMinTickSize(tradePrice, securityWithExchange)
    if (!defined(minTick)) return

    // Is trade volume small relative to auction
    val smallVolume = tradeVolume < singlePositionSizeRelativeToUncrossingValueMax/100.0 * q.volume

    // Price priority optimisation
    tradePrice = {
      if (direction == SELL && smallVolume) forceRoundDown(tradePrice.toString, minTick.toString)
      else if (direction == SELL) forceRoundUp(tradePrice.toString, minTick.toString)
      else if (direction == BUY && smallVolume) forceRoundUp(tradePrice.toString, minTick.toString)
      else forceRoundDown(tradePrice.toString, minTick.toString)
    }

    // Do we need to reverse directions?
    if (getOpenSecurities.contains(q.security) &&
      validTime) {
      val order = getOrderBySymbol(q.security)
      if (order.getSide != direction) {
        println("Direction change. Removing")
        removeOrder(q)
      }
      else if (order.getLimit != tradePrice) {
        val goodMovement = (order.getSide == BUY && tradePrice < order.getLimit) || (order.getSide == SELL && tradePrice > order.getLimit)
        if (goodMovement) println("Favourable price movement. Unusual", q.security, q.date, order.getLimit, tradePrice)
        else {
          println("Price change. Removing")
          removeOrder(q)
        }

      }

    }
    else if (validSecurity(q.price, q.security) &&
      // near end auction
      validTime &&
      // Time throttling
      q.date - timeOfCancel(q.security) > minTimeAfterCancel &&
      ordersInLastSecond < maxOrdersPerSecond &&
      !getOpenSecurities.contains(q.security)) {

      val filters = List(
        // Minimum close value
        filterRemove {
          val tradeVolume = (q.volume * singlePositionSizeRelativeToUncrossingValueMax / 100).toInt
          tradeVolume * q.price < singlePositionSizeMin
        },
      // Maximum orders per security. If we have not traded
      // in a while, then OK to place multiple orders
        filterRemove(orderHistory(q.security) > 0 && timeSinceLastTrade < 1.second),
      // Do not trade next message after cancel IF price has changed
      filterRemove(quotesSinceCancel(q.security) < messageGapAfterCancel && q.price != lastQuotePrice(q.security)),
      // Blacklist
      filterRemove(blacklist.contains(q.security)),
      // Avoid multi tick spreads with indicative in middle
      filterRemove(q.price < q.askBefore && q.price > q.bidBefore)
      )
      // Any true then return
      if (filters.exists(x => x())) {
        return
      }

      // Fire FIX orders
      if (getOpenSecurities.size >= thresholdMaxOpenTrades) {
        println("NOT Trading outstanding order limit breached: " + q.date + " " + direction + " " + securityWithExchange + " " + tradePrice + " " + tradeVolume)
        return
      }
      else if (hedge &&
        (excessBuys || excessSells)) {
        println("Hedging NOT Trading: " + q.date + " " + direction + " " + securityWithExchange + " " + tradePrice + " " + tradeVolume)
      }
      else {
        println("Trading: " + q.date + " " + direction + " " + securityWithExchange + " " + tradePrice + " " + tradeVolume)
        val order = submitLimitOrder(direction, securityWithExchange, tradePrice, tradeVolume)
        printcsv("data\\solo_sent_orders", order.getSymbol, order.getSecurityID, order.getOriginalID, order.getID)
        // Increment order history
        orderHistory.put(q.security, orderHistory(q.security) + 1)
        println("buys " + numBuys, "sells", numSells)
        getBuys.foreach(x => println(x.getSymbol, x.getSide))
        getSells.foreach(x => println(x.getSymbol, x.getSide))
        lastTradeTime = q.date
        ordersInLastSecond += 1
        return
      }

    }

  }

  override def onStart(s: Start) {

   // val o = submitLimitOrder(OrderSide.BUY, "BHP.AX", 30.5, 20)
   // Thread.sleep(15000)
   // cancelOrder(o)
  }

  override def onTrade(t: Trade) {

    // Open auction (close trade)
    if (validSecurity(t.price, t.security) &&
      !openingTrade.contains(t.security)) {
      openingTrade.put(t.security, t.price)
      if (getOpenSecurities.contains(t.security)) {
        val order = getOrderBySymbol(t.security)
        val side = order.getSide match {
          case BUY => SELL
          case SELL => BUY
        }
        println("closing position")
        submitLimitOrder(side, order.getSymbol(), t.price, order.getQuantity)
      }

    }

   if (t.date > closeTime) printcsv("soloquotes.csv", t.security, t.bidBefore, t.askBefore, t.bidVolBefore, t.askVolBefore)

  }

  case class SoloTrade(side: OrderSide, price: Double)

}