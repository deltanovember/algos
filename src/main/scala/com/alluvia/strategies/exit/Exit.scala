package com.alluvia.strategies.exit

import java.util.Date
import com.alluvia.markets.ASX
import com.alluvia.tools.exchange.OrderBook
import com.alluvia.types.market._
import com.alluvialtrading.fix.{OrderType, Order, OrderSide}
import com.alluvialtrading.fix.OrderSide._
import com.alluvia.algo.{OrderRouterClient, EventAlgo}
import com.alluvia.algo.datasource.{Iress, IressReplay}
import fix.FIXNew
import io.Source
import collection.mutable.{HashSet, ListBuffer, HashMap}

object RunExit {
  def main(args: Array[String]) {
    println(new Date)
    new Iress with Exit with ASX with OrderRouterClient {
      val startDate: Date = "2011-12-22"
      val endDate: Date = "2011-12-22"
    }.run

  }

  println(new Date)
}

trait Exit extends EventAlgo {

  val book = new OrderBook
  val sec = "MLB"
  val file = "data\\exit.csv"
  def getServerRoutingKey = "sunset"

  // Exit positions e.g. from overnight
  val positions = new HashMap[String, Order]

  // Last order sent
  val sent = new HashMap[String, Order]
  
  // Ignore QuoteMatch immediately following trade
  val tempIgnore = new HashSet[String]

  /**
   * Generate exit order sans price
   */
  def generateOrder(side: OrderSide, symbol: String, quantity: Int): Order = {
    val order = new Order()
    order.setSide(side)
    order.setSymbol(symbol)
    order.setQuantity(quantity)
    order.setType(OrderType.LIMIT)
    order
  }

  def loadOrders() = {

    val orders = new ListBuffer[Order]
    loadTrades("Solo.history")
    loadTrades("sunset.history")

    def loadTrades(file: String) {
      // Load Solo trades
      for (line <- Source.fromFile(file).getLines) {
        val tokens = line.split(",")
        if (tokens.size > 7) {
          // friday check
          val freshTrade = if (new java.util.Date(tokens(0).getTime).day == 6) tokens(0) == (new Date() - 3.day).toIso
          else tokens(0) == (new Date() - 1.day).toIso
          if (freshTrade) {
            val quantity = tokens(3).toDouble.toInt
            val entryDirection = tokens(7)
            val side = if (entryDirection.equalsIgnoreCase("Buy")) SELL else BUY
            val order = new Order()
            order.setQuantity(quantity)
            order.setSide(side)
            order.setSymbol(tokens(6))
            orders += order
          }

        }
      }
    }


    orders.sortBy(x => x.getSymbol).foreach(x => println(x.getSymbol, x.getSide, x.getQuantity))
    //exit(0)
    orders.toList

  }

  override def onFIXNew(f: FIXNew) {
    printcsv("data\\exitnew.csv", f, f.orderID, f.clOrdID, getAnyOrderByID(extractFIX(f.clOrdID)).getSymbol)
  }

  override def onQuoteFull(q: QuoteFull) {
    if (q.security == sec) printcsv(file, "open", q.securityStatus)
  }

  override def onQuoteMatch(q: QuoteMatch) {

    def isMoreAggressive(currentTradePrice: Double, lastOrder: Order) =
      (lastOrder.getSide == BUY && currentTradePrice > lastOrder.getLimit) ||
        (lastOrder.getSide == SELL && currentTradePrice < lastOrder.getLimit)
    if (tempIgnore.contains(q.security)) {
      tempIgnore.remove(q.security)
      return
    }
    // Initial order
    val securityWithExchange = q.security + "." + getSecurityExchange
    if (positions.contains(securityWithExchange) && !sent.contains(securityWithExchange)) {

      val order = positions(securityWithExchange)
      val submitted = submitLimitOrder(order.getSide, order.getSymbol, q.price, order.getQuantity)
      sent.put(securityWithExchange, submitted)
      return
    }

    // Amend orders - heart of algo
    if (sent.contains(securityWithExchange)) {
      // logging
      printcsv("data\\exitlog.csv", q.date, q.security, q.price, q.surplus, q.volume, q.value)
      val lastOrder = sent(securityWithExchange)
      // Surplus bid and we sell or surplus ask and we buy
      def oppose = (q.surplus > 0 && lastOrder.getSide == SELL) || (q.surplus < 0 && lastOrder.getSide == BUY)
      val minTick = getMinTickSize(q.price, lastOrder.getSymbol)
      // Strategy 1 is to cross
      if (oppose) {
        if (lastOrder.getLimit != q.price) {

          val tradePrice =
          // Be passive
            if (q.price * q.surplus > 250000) {
              if (lastOrder.getSide == SELL) forceRoundUp(q.price.toString, minTick.toString)
              else forceRoundUp(q.price.toString, minTick.toString)
            }
            else {
              q.price
            }
          val newOrder = createOrderWithoutSend(lastOrder.getSide, lastOrder.getSymbol, tradePrice, lastOrder.getQuantity)
          println("Amending to cross", newOrder.getLimit, q.price, q.surplus, q.date)
          if (isMoreAggressive(tradePrice, lastOrder)) {
            amendOrder(lastOrder, newOrder)
            sent.put(securityWithExchange, newOrder)
            tempIgnore.add(q.security)
            Thread.sleep(500)
          }


        }
      }
      // Initially always be aggressive to guarantee fill
      else {

        val aggressivePrice = if (lastOrder.getSide == SELL) forceRoundDown(q.price.toString, minTick.toString)
        else forceRoundUp(q.price.toString, minTick.toString)
        if (lastOrder.getLimit != aggressivePrice) {
          val newOrder = createOrderWithoutSend(lastOrder.getSide, lastOrder.getSymbol, aggressivePrice, lastOrder.getQuantity)
          if (isMoreAggressive(aggressivePrice, lastOrder)) {
            amendOrder(lastOrder, newOrder)
            sent.put(securityWithExchange, newOrder)
            println("aggressive amend", newOrder.getLimit, q.price, q.surplus)
            tempIgnore.add(q.security)
            Thread.sleep(500)
          }

        }
      }

    }
  }
  override def onStart(s: Start) {
    printcsv(file, "Date", "Sec", "Price", "Volume", "Action", "OrderNo", "OrderType", "BidOrAsk")
    loadOrders().foreach {
      x => positions.put(x.getSymbol, x)
      println("Loading", x.getSymbol)
    }

  }
  override def onSingleOrder(o: SingleOrder) {
    if (o.security == sec) {
      printcsv("exit.csv", o.date, o.security, o.price, o.volume, o.action, o.orderNo, o.orderType, o.bidOrAsk)
      if (o.action == 'A') book.addOrder(o)
      else if (o.action == 'D') book.deleteOrder(o)
      else if (o.action == 'M') book.modifyOrder(o)
    }
  }

  var opening = true
  var openTradeTime = new Date

  override def onTrade(t: Trade) {
    if (t.security == sec) {
      if (opening) {
        openTradeTime = t.date
        opening = false
      }
      if (t.date == openTradeTime) {
        println("*****", t.date, t.security, t.price, t.volume)
        book.displayBook()
        println(book.getQuoteMatch.price)
        println(book.getQuoteMatch.volume)
      }
    }

  }
}