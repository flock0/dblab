import ch.epfl.data.dblab.legobase.frontend.SQLParser
import org.scalatest._
import Matchers._

object TPCHQueries {
  val q1 = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflzag, l_linestatus ORDER BY l_returnflag, l_linestatus"
  val q2 = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 43 AND p_type LIKE '%TIN' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AFRICA' AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AFRICA') LIMIT 100"
  val q3 = "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'HOUSEHOLD' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-04' AND l_shipdate > DATE '1995-03-04' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"
  val q4 = "SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= DATE '1993-08-01' AND o_orderdate < DATE '1993-11-01' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority"
  val q5 = "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= DATE '1996-01-01' AND o_orderdate < DATE '1997-01-01' GROUP BY n_name ORDER BY revenue DESC"
  val q6 = "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1997-01-01' AND l_discount BETWEEN 0.09 - 0.01 AND 0.09 + 0.01 AND l_quantity < 24"
}

class SQLParserTest extends FlatSpec {

  "SQLParser" should "parse Q1 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q1)
    r should not be None
  }

  "SQLParser" should "parse Q2 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q2)
    r should not be None
  }

  "SQLParser" should "parse Q3 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q3)
    r should not be None
  }

  "SQLParser" should "parse Q4 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q4)
    r should not be None
  }

  "SQLParser" should "parse Q5 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q5)
    r should not be None
  }

  "SQLParser" should "parse Q6 correctly" in {
    val parser = SQLParser
    val r = parser.parse(TPCHQueries.q6)
    r should not be None
  }

  "SQLParser" should "parse simple SELECT/FROM correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT * FROM table t")
    r should not be None
  }

  "SQLParser" should "parse simple SELECT/FROM (case insensitive) correctly" in {
    val parser = SQLParser
    val r = parser.parse("select * FROM table t")
    r should not be None
  }

  "SQLParser" should "parse single expression correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT column1 FROM table")
    r should not be None
  }

  "SQLParser" should "parse explicit columns correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT column1, ident.column2 FROM table")
    r should not be None
  }

  "SQLParser" should "parse aggregate functions correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT SUM(column1), MIN(ident.column2) FROM table")
    r should not be None
  }

  "SQLParser" should "parse aliased expression correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT SUM(column1) AS theSum, MIN(ident.column2) AS minimum FROM table")
    r should not be None
  }

  "SQLParser" should "parse arithmetic operations correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT SUM(column1) / 10, MIN(ident.column2) + 125.50 FROM table")
    r should not be None
  }

  "SQLParser" should "parse GROUP BY with HAVING correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT column1 FROM table GROUP BY column1, table.column2 HAVING SUM(table.column3) > 12345")
    r should not be None
  }

  "SQLParser" should "parse WHERE correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT column1 FROM table WHERE column2 > 2.51 AND 1 != 0 AND table.column3 BETWEEN 5 AND 10")
    r should not be None
  }

  "SQLParser" should "parse dates correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT * FROM lineitem WHERE DATE '1998-09-02'")
    r should not be None
  }

  "SQLParser" should "parse LIMIT correctly" in {
    val parser = SQLParser
    val r = parser.parse("SELECT * FROM table LIMIT 20")
    r should not be None
  }
}
