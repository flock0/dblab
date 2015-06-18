package ch.epfl.data
package dblab.legobase
package schema

import scala.collection.mutable.ListMap

case class Statistics() {
  private val statsMap = new scala.collection.mutable.HashMap[String, Double]()
  private def format(name: String) = name.replaceAll("Record", "").replaceAll("Type", "").replaceAll("\\(", "_").replaceAll("\\)", "").replaceAll(",", "_").toUpperCase()

  def +=(nameAndValue: (String, Double)) = statsMap += (format(nameAndValue._1) -> nameAndValue._2)
  def +=(name: String, value: Double) = statsMap += (format(name) -> value)
  def mkString(delim: String) = ListMap(statsMap.toSeq.sortBy(_._1): _*).mkString("\n========= STATISTICS =========\n", delim, "\n==============================\n")
  def increase(nameAndValue: (String, Double)) = statsMap += format(nameAndValue._1) -> (statsMap.getOrElse(format(nameAndValue._1), 0.0) + nameAndValue._2)
  def apply(statName: String): Double = statsMap(statName) // TODO-GEN: will die

  def getCardinality(tableName: String) = statsMap.get("CARDINALITY_" + format(tableName)) match {
    case Some(stat) => stat
    case None =>
      // This means that the statistics module has been asked for either a) a table that does not exist
      // in this schema or b) cardinality of an intermediate table that is being scanned over (see TPCH 
      // Q13 for an example about how this may happen). In both cases, we throw a warning message and
      // return the biggest cardinality
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics do not include cardinality information for table " + tableName + ". Returning largest cardinality to compensate. This may lead to degraded performance due to unnecessarily large memory pool allocations.")
      getLargestCardinality()
  }

  def getLargestCardinality() = {
    statsMap.foldLeft(1.0)((max, kv) => {
      if (kv._1.startsWith("CARDINALITY") && kv._2 > max) kv._2
      else max
    })
  }

  // TODO-GEN: The three following functions assume 1-N schemas. We have to make this explicit
  def getJoinOutputEstimation(tableName1: String, tableName2: String): Int = {
    val cardinality1 = getCardinality(tableName1)
    val cardinality2 = getCardinality(tableName2)
    Math.max(cardinality1, cardinality2).toInt
  }
  def getJoinOutputEstimation(intermediateCardinality: Double, tableName2: String): Double = {
    Math.max(intermediateCardinality, getCardinality(tableName2))
  }
  def getJoinOutputEstimation(tableName2: String, intermediateCardinality: Double): Double = {
    Math.max(intermediateCardinality, getCardinality(tableName2))
  }

  def getDistinctAttrValues(attrName: String): Int = statsMap.get("DISTINCT_" + attrName) match {
    case Some(stat) => stat.toInt
    case None =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for DISTINCT_" + attrName + " not found. Returning largest cardinality to compensate. This may lead to degraded performance due to unnecessarily large memory pool allocations.")
      getLargestCardinality().toInt // TODO-GEN: Make this return the cardinality of the corresponding table
  }

  def getEstimatedNumObjectsForType(typeName: String) = statsMap("QS_MEM_" + format(typeName))

  def removeQuerySpecificStats() {
    // QS stands for Query specific
    statsMap.retain((k, v) => k.startsWith("QS") == false)
  }

  def getNumYearsAllDates(): Int = statsMap.get("NUM_YEARS_ALL_DATES") match {
    case Some(stat) => stat.toInt
    case None =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for NUM_YEARS_ALL_DATES not found. Returning 128 years to compensate.")
      128
  }

  def getOutputSizeEstimation(): Int = statsMap.get("QS_OUTPUT_SIZE_ESTIMATION") match {
    case Some(stat) => stat.toInt
    case None =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for QS_OUTPUT_SIZE_ESTIMATION not found. Returning largest cardinality to compensate.")
      getLargestCardinality().toInt // This is more than enough for all practical cases encountered so far
  }
}

// TODO add a type representation for fixed-size stringd-sizz