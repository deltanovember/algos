package com.alluvia.strategies.dowdog

import com.alluvia.algo.EventAlgo
import collection.mutable.HashMap
import com.alluvia.types.MagicMap
import com.alluvialtrading.fix.OrderSide
import com.alluvialtrading.fix.OrderSide._
import com.alluvia.types.market.{DayEnd, DayStart, QuoteMatch, Trade}

/**
 * Dogs of the Dow strategy
 */

trait DowDog extends EventAlgo {

  type Delta = Double
  val openingPrice = new HashMap[String, Double]
  val openingTrade = MagicMap[String](0.0)
  val dailyDeltas = MagicMap[String](0.0)
  val maxOrdersPerSide = 3

  case class Position(security: String, delta: Double)

  override def onDayEnd(d: DayEnd) {
    openingTrade.clear
    openingPrice.clear
    dailyDeltas.clear
  }
  override def onDayStart(d: DayStart) {

  }

  override def onTrade(t: Trade) {
    if (!openingPrice.contains(t.security)) openingPrice.put(t.security, t.price)

    // Open auction (close trade)
    if (!openingTrade.contains(t.security)) {
      openingTrade.put(t.security, t.price)
      val temp = getOpenSecurities
      if (getOpenSecurities.contains(t.security)) {
        val order = getOrderBySymbol(t.security)
        val side = order.getSide match {
          case BUY => SELL
          case SELL => BUY
        }
        submitLimitOrder(side, order.getSymbol(), t.price, order.getQuantity)
      }
      else {
        //println()
      }

    }
  }

  override def onQuoteMatch(q: QuoteMatch) {
    if (!isShortable(q.security)) return
    if (q.date < closeTime) return

    def positiveDeltas = dailyDeltas.filter(x => x._2 > 0)
    def negativeDeltas = dailyDeltas.filter(x => x._2 < 0)
    def max = if (positiveDeltas.size > 0) positiveDeltas.map(x => x._2).max else 0.0
    def min = if (negativeDeltas.size > 0) negativeDeltas.map(x => x._2).min else Double.PositiveInfinity

    if (openingPrice.contains(q.security) &&
      !getOpenSecurities.contains(q.security)) {
      val delta = (q.price - openingPrice(q.security)) / openingPrice(q.security)

      val volume = (10000/q.price).toInt
      // replace and buy
      if (delta > 0 && delta > max) {
        if (numSells >= maxOrdersPerSide) {
          val sells = getSells.map(x => (x.getSymbol, x.getScore))
          val worst = getWorstSell
          removeOrder(worst)
        }
        //println("Trading: " + q.date + " " + SELL + " " + q.security + " " + q.price + " " + volume, delta, max)
        submitLimitOrder(OrderSide.SELL, q.security, q.price, volume, delta)

      }
      else if (delta < 0 && delta < min) {
        if (numBuys >= maxOrdersPerSide) {
          val buys = getBuys.map(x => (x.getSymbol, x.getScore))
          val worst = getWorstBuy
          removeOrder(worst)
        }
        //println("Trading: " + q.date + " " + BUY + " " + q.security + " " + q.price + " " + volume, delta, min)
        submitLimitOrder(OrderSide.BUY, q.security, q.price, volume, math.abs(delta))
      }

      dailyDeltas.put(q.security, delta)
    }
  }

}