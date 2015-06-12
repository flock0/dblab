import ch.epfl.data.dblab.legobase.parser._
import org.scalatest._
import Matchers._

object TPCHQueries {
  val q1 = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflzag, l_linestatus ORDER BY l_returnflag, l_linestatus"
  val q2 = "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 43 AND p_type LIKE '%TIN' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AFRICA' AND ps_supplycost = (SELECT MIN(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'AFRICA') LIMIT 100"
  val q3 = "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'HOUSEHOLD' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-04' AND l_shipdate > DATE '1995-03-04' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10"
  val q4 = "SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= DATE '1993-08-01' AND o_orderdate < DATE '1993-11-01' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority"
  val q5 = "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= DATE '1996-01-01' AND o_orderdate < DATE '1997-01-01' GROUP BY n_name ORDER BY revenue DESC"
  val q6 = "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1997-01-01' AND l_discount BETWEEN 0.09 - 0.01 AND 0.09 + 0.01 AND l_quantity < 24"
  val q7 = "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, EXTRACT(YEAR FROM l_shipdate) AS l_year, l_extendedprice * (1 - l_discount) AS volume FROM supplier, lineitem, orders, customer, nation n1, nation n2 WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND ((n1.n_name = 'UNITED STATES' AND n2.n_name = 'INDONESIA') OR (n1.n_name = 'INDONESIA' AND n2.n_name = 'UNITED STATES')) AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS shipping GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year"
  val q8 = "SELECT o_year, SUM(CASE WHEN nation = 'INDONESIA' THEN volume ELSE 0 END) / SUM(volume) AS mkt_share FROM (SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'ASIA' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND p_type = 'MEDIUM ANODIZED NICKEL') AS all_nations GROUP BY o_year ORDER BY o_year"
  val q9 = "SELECT nation, o_year, SUM(amount) AS sum_profit FROM (SELECT n_name AS nation, EXTRACT(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%ghost%') AS profit GROUP BY nation, o_year ORDER BY nation, o_year DESC"
  val q10 = "SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '1994-11-01' AND o_orderdate < DATE '1995-02-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment ORDER BY revenue DESC LIMIT 20"
  val q11 = "SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS \"value\" FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'UNITED KINGDOM' GROUP BY ps_partkey HAVING SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'UNITED KINGDOM') ORDER BY \"value\" DESC"
  val q12 = "SELECT l_shipmode, SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode in ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= date '1994-01-01' AND l_receiptdate < date '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode"
  val q13 = "SELECT c_count, COUNT(*) AS custdist FROM (SELECT c_custkey, count(o_orderkey) c_count FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%unusual%packages%' GROUP BY c_custkey) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC"
  val q14 = "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1994-03-01' AND l_shipdate < DATE '1994-04-01'"
  val q15 = "WITH revenue AS (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= DATE '1993-09-01' AND l_shipdate < DATE '1993-12-01' GROUP BY l_suppkey) SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue WHERE s_suppkey = supplier_no AND total_revenue = (SELECT MAX(total_revenue) FROM revenue) ORDER BY s_suppkey"
  val q16 = "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brAND <> 'Brand#21' AND p_type not LIKE 'PROMO PLATED%' AND p_size in (23, 3, 33, 29, 40, 27, 22, 4) AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"
  val q17 = "SELECT SUM(l_extendedprice) / 7.0 AS avg_yearlyFROM lineitem, part WHERE p_partkey = l_partkey AND p_brAND = 'Brand#15' AND p_container = 'MED BAG' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)"
  val q18 = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) FROM customer, orders, lineitem WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate LIMIT 100"
  val q19 = "SELECT SUM(l_extendedprice* (1 - l_discount)) AS revenue FROM lineitem, part WHERE (p_partkey = l_partkey AND p_brAND = 'Brand#31' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 4 AND l_quantity <= 4 + 10 AND p_size BETWEEN 1 AND 5 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brAND = 'Brand#43' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 15 AND l_quantity <= 15 + 10 AND p_size BETWEEN 1 AND 10 AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON' ) OR (p_partkey = l_partkey AND p_brAND = 'Brand#43' AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 26 AND l_quantity <= 26 + 10 AND p_size BETWEEN 1 AND 15 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')"
  val q20 = "SELECT s_name, s_address FROM supplier, nation WHERE s_suppkey IN (SELECT ps_suppkey FROM partsupp WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'azure%') AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey AND l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1997-01-01')) AND s_nationkey = n_nationkey AND n_name = 'JORDAN' ORDER BY s_name"
  val q21 = "SELECT s_name, COUNT(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey ) AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate) AND s_nationkey = n_nationkey AND n_name = 'MOROCCO' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"
  val q22 = "SELECT cntrycode, count(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT SUBSTRING(c_phone FROM 1 for 2) AS cntrycode, c_acctbal FROM customer WHERE SUBSTRING(c_phone FROM 1 for 2) in ('20', '22', '23', '24', '25', '26', '29') AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone FROM 1 for 2) in ('20', '22', '23', '24', '25', '26', '29')) AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode"
}

class SQLParserTest extends FlatSpec {

  val parser = SQLParser

  "SQLParser" should "parse Q1 correctly" in {
    val r = parser.parse(TPCHQueries.q1)
    r should not be None
  }

  it should "parse Q2 correctly" in {
    val r = parser.parse(TPCHQueries.q2)
    r should not be None
  }

  it should "parse Q3 correctly" in {
    val r = parser.parse(TPCHQueries.q3)
    r should not be None
  }

  it should "parse Q4 correctly" in {
    val r = parser.parse(TPCHQueries.q4)
    r should not be None
  }

  it should "parse Q5 correctly" in {
    val r = parser.parse(TPCHQueries.q5)
    r should not be None
  }

  it should "parse Q6 correctly" in {
    val r = parser.parse(TPCHQueries.q6)
    r should not be None
  }

  it should "parse Q7 correctly" in {
    val r = parser.parse(TPCHQueries.q7)
    r should not be None
  }

  it should "parse Q8 correctly" in {
    val r = parser.parse(TPCHQueries.q8)
    r should not be None
  }

  it should "parse Q9 correctly" in {
    val r = parser.parse(TPCHQueries.q9)
    r should not be None
  }

  it should "parse Q10 correctly" in {
    val r = parser.parse(TPCHQueries.q10)
    r should not be None
  }

  it should "parse Q11 correctly" in {
    val r = parser.parse(TPCHQueries.q11)
    r should not be None
  }

  it should "parse Q12 correctly" in {
    val r = parser.parse(TPCHQueries.q12)
    r should not be None
  }

  it should "parse Q13 correctly" in {
    val r = parser.parse(TPCHQueries.q13)
    r should not be None
  }

  it should "parse Q14 correctly" in {
    val r = parser.parse(TPCHQueries.q14)
    r should not be None
  }

  it should "parse Q15 correctly" in {
    val r = parser.parse(TPCHQueries.q15)
    r should not be None
  }

  it should "parse Q16 correctly" in {
    val r = parser.parse(TPCHQueries.q16)
    r should not be None
  }

  it should "parse Q17 correctly" in {
    val r = parser.parse(TPCHQueries.q17)
    r should not be None
  }

  it should "parse Q18 correctly" in {
    val r = parser.parse(TPCHQueries.q18)
    r should not be None
  }

  it should "parse Q19 correctly" in {
    val r = parser.parse(TPCHQueries.q19)
    r should not be None
  }

  it should "parse Q20 correctly" in {
    val r = parser.parse(TPCHQueries.q20)
    r should not be None
  }

  it should "parse Q21 correctly" in {
    val r = parser.parse(TPCHQueries.q21)
    r should not be None
  }

  it should "parse Q22 correctly" in {
    val r = parser.parse(TPCHQueries.q22)
    r should not be None
  }

  it should "parse simple SELECT/FROM correctly" in {
    val r = parser.parse("SELECT * FROM table t")
    r should not be None
  }

  it should "parse single expression correctly" in {
    val r = parser.parse("SELECT column1 FROM table")
    r should not be None
  }

  it should "parse explicit columns correctly" in {
    val r = parser.parse("SELECT column1, ident.column2 FROM table")
    r should not be None
  }

  it should "parse aggregate functions correctly" in {
    val r = parser.parse("SELECT SUM(column1), MIN(ident.column2) FROM table")
    r should not be None
  }

  it should "parse aliased expression correctly" in {
    val r = parser.parse("SELECT SUM(column1) AS theSum, MIN(ident.column2) AS 'minimum' FROM table")
    r should not be None
  }

  it should "parse arithmetic operations correctly" in {
    val r = parser.parse("SELECT SUM(column1) / 10, MIN(ident.column2) + 125.50 FROM table")
    r should not be None
  }

  it should "parse GROUP BY with HAVING correctly" in {
    val r = parser.parse("SELECT column1 FROM table GROUP BY column1, table.column2 HAVING SUM(table.column3) > 12345")
    r should not be None
  }

  it should "parse WHERE correctly" in {
    val r = parser.parse("SELECT column1 FROM table WHERE column2 > 2.51 AND 1 != 0 AND table.column3 BETWEEN 5 AND 10")
    r should not be None
  }

  it should "parse dates correctly" in {
    val r = parser.parse("SELECT * FROM lineitem WHERE DATE '1998-09-02'")
    r should not be None
  }

  it should "parse LIMIT correctly" in {
    val r = parser.parse("SELECT * FROM table LIMIT 20")
    r should not be None
  }

  it should "parse EXTRACT correctly" in {
    val r1 = parser.parse("SELECT EXTRACT(YEAR FROM some_date) FROM table")
    val r2 = parser.parse("SELECT EXTRACT(MONTH FROM some_date) FROM table")
    val r3 = parser.parse("SELECT EXTRACT(DAY FROM some_date) FROM table")

    r1 should not be None
    r2 should not be None
    r3 should not be None
  }

  it should "parse simple CASE statement correctly" in {
    val r = parser.parse("SELECT CASE attributeOne WHEN 'some_value' THEN this ELSE that END FROM table")
    r should not be None
  }

  it should "parse complex CASE statement correctly" in {
    val r = parser.parse("SELECT CASE WHEN attribute = 'some_value' THEN this ELSE that END FROM table")
    r should not be None
  }

  it should "parse complex CASE statement with multiple cases correctly" in {
    val r = parser.parse("SELECT SUM(CASE WHEN attribute = 'some_value' THEN this WHEN answer = 42 THEN valX ELSE valY END) FROM table")
    r should not be None
  }
}
