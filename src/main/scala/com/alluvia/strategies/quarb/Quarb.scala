package com.alluvia.strategies.quarb

/**
 *
 */
import com.alluvia.algo.EventAlgo
import collection.mutable.HashMap
import java.util.Date
import org.apache.commons.math.stat.descriptive.SummaryStatistics
import scala.math.abs
import com.alluvia.types.MagicMap
import com.alluvia.types.market.{Quote, Trade}

trait Quarb
  extends EventAlgo {

  // Benchmark values
  val values = MagicMap[String](0.0)
  val volumes = MagicMap[String](0.0)
  val openingTrade = new HashMap[String, Date]
  val betaDistributions = MagicMap[String](new SummaryStatistics())
  val benchmarkTime = 15.minutes
  var latestBenchmarkPrice = 0.0
  val benchmarkSecurity = "STW"

  var lastBenchmarkBid = Double.NaN
  var lastBenchmarkAsk = Double.NaN

  override def benchmarkDays = 0

  override def onBenchmarkTrade {

  }

  override def onQuote(q: Quote) {

    // Always straight store benchmark security
    if (q.security == benchmarkSecurity &&
      openingTrade.contains(q.security) &&
      q.date - openingTrade(q.security) > benchmarkTime) {
      lastBenchmarkAsk = q.askBefore
      lastBenchmarkBid = q.bidBefore
    }
    else if (openingTrade.contains(q.security) &&
      q.date - openingTrade(q.security) > benchmarkTime &&
    defined(lastBenchmarkAsk)) {
      // Store
      val mid = (q.bid + q.ask) / 2
      val openPrice = (values(q.security)) / volumes(q.security)
      val delta = (mid - openPrice) / openPrice

      val refMid = (lastBenchmarkAsk + lastBenchmarkBid) / 2
      val refOpenPrice = values(benchmarkSecurity) / volumes(benchmarkSecurity)
      val refDelta = (refMid - refOpenPrice) / refOpenPrice

      val intraDayBeta = delta / refDelta

      if (abs(refDelta) > 0.005 &&
        abs(delta) > 0.005 &&
      isShortable(q.security)) {
        betaDistributions(q.security).addValue(intraDayBeta)
        if (/**security == "PRY" &&*/
          intraDayBeta > betaDistributions(q.security).getMean + 3 * betaDistributions(q.security).getStandardDeviation)
          println(q.security, q.date, intraDayBeta, betaDistributions(q.security).getMean,  betaDistributions(q.security).getStandardDeviation, delta, refDelta)
      }
    }
  }

  override def onTrade(t: Trade) {

    // First trade
    if (!openingTrade.contains(t.security)) {
      openingTrade.put(t.security, t.date)
    }
    // 15 minute VWAPs
    if (t.date - openingTrade(t.security) < benchmarkTime) {
      // Reference price
      values.put(t.security, values(t.security) + t.value)
      volumes.put(t.security, volumes(t.security) + t.volume)
    }
    else {

    }
    
  }

}