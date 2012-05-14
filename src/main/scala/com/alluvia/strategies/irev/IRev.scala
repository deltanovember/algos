package com.alluvia.strategies.irev

import com.alluvia.algo.EventAlgo
import collection.mutable.{ListBuffer, HashMap}

import scala.math.{abs, log}
import java.util.Date
import com.alluvialtrading.fix.OrderSide._
import com.alluvialtrading.fix.OrderSide
import com.alluvia.types.market._
import com.alluvia.types.{MagicMap, FixedList}
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics

trait IRev
  extends EventAlgo {

  // Params
  // -----------------------------------------------------------------------

  // User params
  val print2file = true // Boolean
  val loud = true // Boolean
  val allSecs = true  // Boolean
  val debugSecurity = "AMP"

  // Anomaly thresholds
  val thresholdErrorMarginMin = 5  // bps
  val thresholdNumStandardDeviations = 6 // x
  val VWAPTime = 30.minutes  // minutes

  // Distribution characteristics
  val thresholdTimeAwayAuctionMin = 10.minutes  // minutes
  val thresholdErrorDistAgeMin = 30.minutes  // minutes
  val thresholdNumEventsMin = 300  // x
  val thresholdNumEventsMax = 10000  // x

  // Brokerage
  val thresholdBrokerageRateMin = 10  // bps
  val thresholdBrokerageFlatMin = 8  // $

  // Position limits
  val thresholdOrderSizeMax = 15000  // $
  val thresholdNumPositionsPerMarketMax = 50  // x
  val thresholdNumPositionBuildsMax = 3  // x
  val thresholdPositionHoldTimeMin = 30.minutes  // minutes
  val thresholdTimeBetweenBuilds = 3.minutes  // minutes
  val thresholdTotalBuildTimeMax = 15.minutes  // minutes
  val thresholdTimeBetweenNewPositions = 30.minutes  // minutes

  // Initialise derived data arrays
  def defaultIntList = new FixedList[Int](thresholdNumEventsMax)
  def defaultDoubleList = new FixedList[Double](thresholdNumEventsMax)
  var errorStats = new DescriptiveStatistics()
  val errorDist = MagicMap[String](defaultDoubleList)
  var VWAPTimeBased = MagicMap[String](0.0)
  var spreadTimeBased = MagicMap[String](0.0)
  var errorEventBased = MagicMap[String](0.0)
  val tradeList = MagicMap[String](new FixedList[Trade](thresholdNumEventsMax))
  val errorDistStartTime = new HashMap[String, Date]

  // Order management
  case class Position2(date: java.util.Date, security: String, insideDelta : Double, volume: Double, value: Double, direction : OrderSide, brokerage : Double)

  //val positionsAll = MagicMap[String](new FixedList[Position](thresholdNumOpenPositionsPerSecurityMax))
  val position = new HashMap[String, Position2]
  val numPositionBuilds = MagicMap[String](0)
  var lastAuctionTime = MagicMap[String](openTime)
  var firstEntryTime = MagicMap[String](openTime)
  var lastEntryTime = MagicMap[String](openTime)
  var lastExitTime = MagicMap[String](openTime)

  val securityList = new HashMap[String, Boolean]
  securityList.put("AMP", true)
  //  securityList.put("CBA", true)
  //  securityList.put("ANZ", true)
  //  securityList.put("NAB", true)
  //  securityList.put("BHP", true)
  //  securityList.put("SDL", true)
  //  securityList.put("AWC", true)
  //  securityList.put("PDN", true)
  //  securityList.put("CQR", true)
  //  securityList.put("ILU", true)
  //  securityList.put("CWN", true)
  //  securityList.put("ORI", true)
  securityList.put("NCM", true)
  //  securityList.put("WTF", true)

  // Print to file
  var fileName = "irev.csv"
  val dir = "C:/Users/mclifton.CM-CRC/Alluvial/Code/alluvia/src/main/scala/com/alluvia/visual/spread2/spreaddata/data/";

  println("\nRunning iRev:")

  override def onDayStart( d: DayStart) {
    lastAuctionTime = MagicMap[String](openTime)
    firstEntryTime = MagicMap[String](openTime)
    lastEntryTime = MagicMap[String](openTime)
    lastExitTime = MagicMap[String](openTime)
  }

  override def onQuoteMatch(q: QuoteMatch) {
    if (!allSecs && !securityList.contains(q.security)) return
    if (position.contains(q.security)) exitSignalGeneratorQuoteMatch(q)
    lastAuctionTime.put(q.security, q.date)
  }

  override def onQuote(q: Quote) {
    if (eventFilter(q.date, q.security, q.spreadBps)) return
    entrySignalGenerator(q)
    exitSignalGenerator(q, false)
  }

  override def onTrade(t: Trade) {
    if (print2Console(t.security)) println(dateToISODateTimeString(t.date) + " " + t.security + " " + transType)
    if (eventFilter(t.date, t.security, t.spreadBeforeBps)) return
    if (position.contains(t.security)) return

    calcRefDataTimeBased(t)
    calcRefDataEventBased(t)

    if (print2file) printcsv(dir + t.date.toDateStr.replace("-","") + "/signals/" + "irev_data.csv",
      "SIGNAL", t.date.toIso + " " + t.date.toTimeStr, t.security,
      VWAPTimeBased(t.security),
      spreadTimeBased(t.security),
      if (errorDist(t.security).size < thresholdNumEventsMin) 9999 else thresholdNumStandardDeviations * errorEventBased(t.security))
  }

  def entrySignalGenerator(q: Quote) {

    // IF variables are undefined THEN break
    if (undefined(q.bidBefore) || undefined(q.askBefore) || undefined(q.bidVolBefore) || undefined(q.askVolBefore) ||
      !VWAPTimeBased.contains(q.security) || !VWAPTimeBased.contains(q.security)) return

    // IF there are insufficient observations to calculate reference data THEN break
    if (errorDist(q.security).size < thresholdNumEventsMin || (q.date - errorDistStartTime(q.security)) < thresholdErrorDistAgeMin) return

    // IF the size of the spread has blown out relative to the reference spread THEN break
    if (q.spreadBps > 3 * spreadTimeBased(q.security)) return

    // ELSE calculate delta
    var direction = BUY
    var insideDelta = 100 * 100 * log(VWAPTimeBased(q.security) / q.ask)
    if (q.bid > VWAPTimeBased(q.security)) {
      direction = SELL
      insideDelta = 100 * 100 * log(q.bid / VWAPTimeBased(q.security))
    }

    // IF variance is not anomalous THEN break
    if (q.ask > (VWAPTimeBased(q.security) - thresholdNumStandardDeviations*errorEventBased(q.security)) && q.bid < (VWAPTimeBased(q.security) + thresholdNumStandardDeviations*errorEventBased(q.security))) return

    // IF position already exists AND new direction is opposite THEN exit
    if (position.contains(q.security) && q.security == "NCM") println(direction + " " + position(q.security).direction)
    if (position.contains(q.security) && direction != position(q.security).direction) {
      exitSignalGenerator(q, true)
      return
    }

    // IF time since last entry trade is too small THEN break
    if (position.contains(q.security) && (q.date - lastEntryTime(q.security)) < thresholdTimeBetweenBuilds) return

    // IF time since last entry trade is too great THEN break
    if (position.contains(q.security) && (q.date - firstEntryTime(q.security)) > thresholdTotalBuildTimeMax) return

    // IF position exists and new insideDelta is smaller than last insideDelta THEN break
    if (position.contains(q.security) && insideDelta <= position(q.security).insideDelta) return

    // IF time since last exit trade is insufficient THEN break
    if ((q.date - lastExitTime(q.security)) < thresholdTimeBetweenNewPositions) return

    // Calculate trade size
    var orderPrice = q.ask
    var orderVolume = q.askVol
    var orderValue = orderPrice * orderVolume
    if (direction == SELL) {
      orderPrice = q.bid
      orderVolume = q.bidVol
      orderValue = orderPrice * orderVolume
    }
    if (orderValue > thresholdOrderSizeMax) {
      orderVolume = (thresholdOrderSizeMax / orderPrice).floor
      orderValue = orderPrice * orderVolume
    }

    // Calculate transactions costs
    val brokerageBps = 100 * 100 * calcBrokerage(orderValue) / orderValue
    val tcosts = brokerageBps + spreadTimeBased(q.security)

    // IF price change is not large relative to transactions costs THEN break
    if (insideDelta < tcosts + thresholdErrorMarginMin) return

    // ELSE generate trade signal
    preTradeChecker(q, direction, q.security, orderPrice, orderVolume, orderValue, insideDelta, VWAPTimeBased(q.security),
      spreadTimeBased(q.security), calcBrokerage(orderPrice * orderVolume))

  }

  def preTradeChecker(q : Quote, direction : OrderSide, security : String, orderPrice : Double, orderVolume : Double,
                      orderValue : Double, insideDelta : Double, refPrice : Double, refSpread : Double, brokerage : Double) {

    // IF number of open positions per market will exceed limit THEN break
    if (position.size > thresholdNumPositionsPerMarketMax) return

    // IF number of open positions per security will exceed limit THEN break
    if (numPositionBuilds(security) > thresholdNumPositionBuildsMax) return

    // ELSE submit order
    //val order = submitLimitOrder(direction, security, orderPrice, orderVolume.toInt)

    // Update positions (assume complete fill)
    val updatedVolume = if (position.contains(security)) position(security).volume + orderVolume else orderVolume
    val updatedValue = if (position.contains(security)) position(security).value + orderValue else orderValue
    val updatedBrokerage = if (position.contains(security)) position(security).brokerage + brokerage else brokerage
    position.put(security, Position2(q.date, security, insideDelta, updatedVolume, updatedValue, direction, updatedBrokerage))
    numPositionBuilds.put(security, numPositionBuilds(security) + 1)
    if (!firstEntryTime.contains(q.security)) firstEntryTime.put(q.security, q.date)
    lastEntryTime(q.security) = q.date

    // Print to file
    if (print2file) printcsv(dir + q.date.toDateStr.replace("-","") + "/signals/" + fileName,
      "SIGNAL", q.date.toIso + " " + q.date.toTimeStr, security,
      "irev", insideDelta, refSpread, refPrice,
      orderPrice, orderVolume, orderValue,
      round2(brokerage),
      if (direction == BUY) 'B' else 'S',
      "open")

  }

  def exitSignalGenerator(q: Quote, signal : Boolean) {

    // IF a position does not already exist THEN break
    if (!position.contains(q.security)) return

    // IF position has been held for less than minimum hold time THEN break
    if (!signal && (q.date - position(q.security).date) < thresholdPositionHoldTimeMin) return

    // Prepare order details
    def direction = if (position(q.security).direction == BUY) SELL else BUY
    var orderPrice = if (position(q.security).direction == BUY) q.bid else q.ask
    var orderVolume = position(q.security).volume
    var orderValue = orderPrice * orderVolume

    if (print2Console(q.security)) println(direction + " " + orderPrice + " " + orderVolume + " " + orderValue + " " + q.bidVol + " " + q.askVol)

    // IF insufficient volume to get out THEN break
    if ((direction == BUY && orderVolume > q.askVol) || (direction == SELL && orderVolume > q.bidVol)) return

    // ELSE exit position
    //val order = submitLimitOrder(direction, q.security, orderPrice, orderVolume.toInt)

    // Calculate brokerage
    val brokerage = calcBrokerage(orderValue)

    // Calculate profit/loss
    if (print2Console(q.security)) println(q.security  + " " + direction + " " + orderValue + " " + position(q.security).value + " " + brokerage)
    val profit = if (direction == SELL) orderValue - position(q.security).value - brokerage - position(q.security).brokerage
    else position(q.security).value - orderValue - brokerage - position(q.security).brokerage

    // Print to file
    if (print2file) printcsv(dir + q.date.toDateStr.replace("-","") + "/signals/" + fileName,
      "SIGNAL", q.date.toIso + " " + q.date.toTimeStr, q.security,
      "irev", "", "", "",
      orderPrice, orderVolume, orderValue,
      round2(brokerage),
      if (direction == BUY) 'B' else 'S',
      "close",
      round2(profit))

    position.remove(q.security)
    numPositionBuilds(q.security) = 0
    lastExitTime(q.security) = q.date
    firstEntryTime.remove(q.security)

  }

  def exitSignalGeneratorQuoteMatch(q: QuoteMatch) {

    // IF a position does not already exist THEN break
    if (!position.contains(q.security)) return

    // Prepare order details
    def direction = if (position(q.security).direction == BUY) SELL else BUY
    var orderPrice = q.price
    var orderVolume = position(q.security).volume
    var orderValue = orderPrice * orderVolume

    if (print2Console(q.security)) println(direction + " " + orderPrice + " " + orderVolume + " " + orderValue + " " + q.volume)

    // IF insufficient volume to get out THEN break
    //if (orderVolume > q.volume) return

    // ELSE exit position
    //val order = submitLimitOrder(direction, q.security, orderPrice, orderVolume.toInt)

    // Calculate brokerage
    val brokerage = calcBrokerage(orderValue)

    // Calculate profit/loss
    if (print2Console(q.security)) println(q.security  + " " + direction + " " + orderValue + " " + position(q.security).value + " " + brokerage)
    val profit = if (direction == SELL) orderValue - position(q.security).value - brokerage - position(q.security).brokerage
    else position(q.security).value - orderValue - brokerage - position(q.security).brokerage

    // Print to file
    if (print2file) printcsv(dir + q.date.toDateStr.replace("-","") + "/signals/" + fileName,
      "SIGNAL", q.date.toIso + " " + q.date.toTimeStr, q.security,
      "irev", "", "", "",
      orderPrice, orderVolume, orderValue,
      round2(brokerage),
      if (direction == BUY) 'B' else 'S',
      "close",
      round2(profit))

    position.remove(q.security)
    numPositionBuilds(q.security) = 0
    lastExitTime(q.security) = q.date

  }

  // Calculate brokerage
  def calcBrokerage(orderValue : Double): Double = {
    var brokerageFee = orderValue * thresholdBrokerageRateMin / 100 / 100
    if (brokerageFee < thresholdBrokerageFlatMin) brokerageFee = thresholdBrokerageFlatMin
    brokerageFee
  }

  // Event filters
  def eventFilter(date : Date, security : String, spreadBps : Double): Boolean = {

    // IF current security is undesired THEN break
    if (!allSecs && !securityList.contains(security)) {
      val filterOut = true
      return filterOut
    }

    // If spread is undefined THEN reset distributions and break
    if (undefined(spreadBps)) {
      val filterOut = true
      return filterOut
    }

    // IF event is near market open THEN break
    if ((date - openTime) < thresholdTimeAwayAuctionMin) {
      val filterOut = true
      return filterOut
    }

    // IF event is near an auction THEN break
    if (!lastAuctionTime.contains(security) || (date - lastAuctionTime(security)) < thresholdTimeAwayAuctionMin) {
      val filterOut = true
      return filterOut
    }

    val filterOut = false
    return filterOut

  }

  def calcRefDataEventBased(t : Trade) {
    if (!errorDistStartTime.contains(t.security)) errorDistStartTime.put(t.security, t.date)
    errorDist -> t.security append abs(t.price - VWAPTimeBased(t.security))
    errorStats = new DescriptiveStatistics();
    for (x <- errorDist(t.security)) errorStats.addValue(x)
    errorEventBased.put(t.security, errorStats.getStandardDeviation)
  }

  def calcRefDataTimeBased(t : Trade) {

    // Recent trades
    tradeList(t.security).append(t)
    val recent = tradeList(t.security).filter(trade => t.date - trade.date < VWAPTime)  // remove stale data

    // Vwap (time limited)
    val value = recent.map(x => x.price * x.volume)
    val volume = recent.map(x => x.volume)
    VWAPTimeBased.put(t.security, value.sum / volume.sum)
    //if (print2Console(t.security)) println("VWAP " + t.date.toTimeStr + " " + errorDist(t.security).size)
    // Spread (time limited)
    val spread = recent.map(x => x.spreadBeforeBps)
    spreadTimeBased.put(t.security, spread.sum / spread.size)

  }

  def print2Console(security: String) = (loud && security == debugSecurity)

}