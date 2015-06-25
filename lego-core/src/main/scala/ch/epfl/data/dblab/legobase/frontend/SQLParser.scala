package ch.epfl.data
package dblab.legobase
package frontend

import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.combinator.token._
import scala.util.parsing.input.CharArrayReader.EofCh

//TODO : should this be here?
import ch.epfl.data.dblab.legobase.queryengine.GenericEngine

/**
 * A simple SQL parser.
 * Based on: https://github.com/stephentu/scala-sql-parser
 */
object SQLParser extends StandardTokenParsers {

  def parse(statement: String): SelectStatement = {
    phrase(parseSelectStatement)(new lexical.Scanner(statement)) match {
      case Success(r, q) => r
      case _             => throw new Exception("Unable to parse SQL query!")
    }
  }

  def extractRelationsFromJoinTree(joinTree: Relation): Seq[Relation] = {
    joinTree match {
      case Join(left, right, _, _) =>
        extractRelationsFromJoinTree(left) ++ extractRelationsFromJoinTree(right)
      case tbl: SQLTable => Seq(tbl)
      case sq: Subquery => sq.subquery.joinTree match {
        case Some(tr) => extractRelationsFromJoinTree(tr)
        case None     => sq.subquery.relations
      }
    }
  }

  def parseSelectStatement: Parser[SelectStatement] = (
    "SELECT" ~> parseProjections ~ "FROM" ~ parseRelations ~ parseWhere.? ~ parseGroupBy.? ~ parseHaving.? ~ parseOrderBy.? ~ parseLimit.? <~ ";".? ^^ {
      case pro ~ _ ~ tab ~ whe ~ grp ~ hav ~ ord ~ lim => {
        val rel = tab.foldLeft(Seq[Relation]())((list, tb) => list ++ extractRelationsFromJoinTree(tb))
        val aliases = (pro match {
          case ep: ExpressionProjections => ep.lst.zipWithIndex.filter(p => p._1._2.isDefined).map(al => (al._1._1, al._1._2.get, al._2))
          case ac: AllColumns            => Seq()
        }) ++ (grp match {
          case Some(gb) => gb.keys.zipWithIndex.filter(p => p._1._2.isDefined).map(gb => (gb._1._1, gb._1._2.get, gb._2))
          case None     => Seq()
        })
        val hasJoin = tab(0).isInstanceOf[Join]
        //System.out.println(tab)
        hasJoin match {
          case true => tab.size match {
            case 1 => SelectStatement(pro, rel, Some(tab(0)), whe, grp, hav, ord, lim, aliases)
            case _ => throw new Exception("BUG: Invalid number of join trees detected!")
          }
          case false => tab.size match {
            case 1 => SelectStatement(pro, rel, Some(tab(0)), whe, grp, hav, ord, lim, aliases)
            case _ => throw new Exception("Error in query: There are multiple input relations but no join! Cannot process such query statement!")
          }
        }
      }
    })

  def parseProjections: Parser[Projections] = (
    "*" ^^^ AllColumns()
    | rep1sep(parseAliasedExpression, ",") ^^ { case lst => ExpressionProjections(lst) })

  def parseAliasedExpression: Parser[(Expression, Option[String])] = (
    parseExpression ~ ("AS" ~> ident).? ^^ { case expr ~ alias => (expr, alias) })

  def parseExpression: Parser[Expression] = parseCase | parseOr

  def parseCase: Parser[Expression] = "CASE" ~ "WHEN" ~ parseExpression ~ "THEN" ~ parseExpression ~ "ELSE" ~ parseExpression <~ "END" ^^ {
    case _ ~ _ ~ cond ~ _ ~ thenp ~ _ ~ elsep => Case(cond, thenp, elsep)
  }

  def parseOr: Parser[Expression] =
    parseAnd * ("OR" ^^^ { (a: Expression, b: Expression) => Or(a, b) })

  def parseAnd: Parser[Expression] =
    parseSimpleExpression * ("AND" ^^^ { (a: Expression, b: Expression) => And(a, b) })

  def parseSimpleExpression: Parser[Expression] = (
    parseAddition ~ rep(
      ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") ~ parseAddition ^^ {
        case op ~ right => (op, right)
      }
        | "BETWEEN" ~ parseAddition ~ "AND" ~ parseAddition ^^ {
          case op ~ a ~ _ ~ b => (op, a, b)
        }
        | "NOT" ~> "IN" ~ "(" ~ (parseSelectStatement | rep1sep(parseExpression, ",")) ~ ")" ^^ {
          case op ~ _ ~ a ~ _ => (op, a, true)
        }
        | "IN" ~ "(" ~ (parseSelectStatement | rep1sep(parseExpression, ",")) ~ ")" ^^ {
          case op ~ _ ~ a ~ _ => (op, a, false)
        }
        | "NOT" ~> "LIKE" ~ parseAddition ^^ { case op ~ a => (op, a, true) }
        | "LIKE" ~ parseAddition ^^ { case op ~ a => (op, a, false) }) ^^ {
        case left ~ elems =>
          elems.foldLeft(left) {
            case (acc, (("=", right: Expression)))                  => Equals(acc, right)
            case (acc, (("<>", right: Expression)))                 => NotEquals(acc, right)
            case (acc, (("!=", right: Expression)))                 => NotEquals(acc, right)
            case (acc, (("<", right: Expression)))                  => LessThan(acc, right)
            case (acc, (("<=", right: Expression)))                 => LessOrEqual(acc, right)
            case (acc, ((">", right: Expression)))                  => GreaterThan(acc, right)
            case (acc, ((">=", right: Expression)))                 => GreaterOrEqual(acc, right)
            case (acc, (("BETWEEN", l: Expression, r: Expression))) => And(GreaterOrEqual(acc, l), LessOrEqual(acc, r))
            case (acc, (("IN", e: Seq[_], n: Boolean)))             => In(acc, e.asInstanceOf[Seq[Expression]], n)
            case (acc, (("IN", s: SelectStatement, n: Boolean)))    => In(acc, Seq(s), n)
            case (acc, (("LIKE", e: Expression, n: Boolean)))       => Like(acc, e, n)
          }
      } |
      "NOT" ~> parseSimpleExpression ^^ (Not(_)) |
      "EXISTS" ~> "(" ~> parseSelectStatement <~ ")" ^^ { case s => Exists(s) })

  def parseAddition: Parser[Expression] =
    parseMultiplication * (
      "+" ^^^ { (a: Expression, b: Expression) => Add(a, b) } |
      "-" ^^^ { (a: Expression, b: Expression) => Subtract(a, b) })

  def parseMultiplication: Parser[Expression] =
    parsePrimaryExpression * (
      "*" ^^^ { (a: Expression, b: Expression) => Multiply(a, b) } |
      "/" ^^^ { (a: Expression, b: Expression) => Divide(a, b) })

  def parsePrimaryExpression: Parser[Expression] = (
    parseLiteral |
    parseKnownFunction |
    ident ~ opt("." ~> ident | "(" ~> repsep(parseExpression, ",") <~ ")") ^^ {
      case id ~ None           => FieldIdent(None, id)
      case a ~ Some(b: String) => FieldIdent(Some(a), b)
    } |
    "(" ~> (parseExpression | parseSelectStatement) <~ ")"
    | "+" ~> parsePrimaryExpression ^^ (UnaryPlus(_))
    | "-" ~> parsePrimaryExpression ^^ (UnaryMinus(_)))

  def parseKnownFunction: Parser[Expression] = (
    "COUNT" ~> "(" ~> ("*" ^^^ CountAll() | parseExpression ^^ { case expr => CountExpr(expr) }) <~ ")"
    | "MIN" ~> "(" ~> parseExpression <~ ")" ^^ (Min(_))
    | "MAX" ~> "(" ~> parseExpression <~ ")" ^^ (Max(_))
    | "SUM" ~> "(" ~> parseExpression <~ ")" ^^ (Sum(_))
    | "AVG" ~> "(" ~> parseExpression <~ ")" ^^ (Avg(_))
    | "YEAR" ~> "(" ~> parseExpression <~ ")" ^^ (Year(_))
    | "SUBSTRING" ~> "(" ~> parseExpression ~ "," ~ parseExpression ~ "," ~ parseExpression <~ ")" ^^ {
      case str ~ _ ~ idx1 ~ _ ~ idx2 => Substring(str, idx1, idx2)
    })

  def parseLiteral: Parser[Expression] = (
    numericLit ^^ { case i => IntLiteral(i.toInt) }
    | floatLit ^^ { case f => DoubleLiteral(f.toDouble) }
    | stringLit ^^ {
      case s => {
        if (s.length == 1) CharLiteral(s.charAt(0))
        else StringLiteral(GenericEngine.parseString(s))
      }
    }
    | "NULL" ^^ { case _ => NullLiteral() }
    | "DATE" ~> stringLit ^^ { case s => DateLiteral(GenericEngine.parseDate(s)) })

  def parseRelations: Parser[Seq[Relation]] = rep1sep(parseRelation, ",")

  def parseRelation: Parser[Relation] = (
    parseSimpleRelation ~ rep(opt(parseJoinType) ~ "JOIN" ~ parseSimpleRelation ~ "ON" ~ parseExpression ^^ {
      case tpe ~ _ ~ r ~ _ ~ e => (tpe.getOrElse(InnerJoin), r, e)
    }) ^^ {
      case r ~ elems => elems.foldLeft(r) { case (x, r) => Join(x, r._2, r._1, r._3) }
    })

  def parseSimpleRelation: Parser[Relation] = (
    ident ~ ("AS".? ~> ident.?) ^^ {
      case tbl ~ alias => SQLTable(tbl, alias)
    }
    | ("(" ~> parseSelectStatement <~ ")") ~ ("AS".? ~> ident) ^^ {
      case subq ~ alias => Subquery(subq, alias)
    })

  def parseJoinType: Parser[JoinType] = (
    ("LEFT" ~ "OUTER" | "LEFT" ~ "SEMI" | "RIGHT" <~ "OUTER".? | "FULL" ~ "OUTER" | "ANTI") ^^ {
      case "LEFT" ~ "OUTER" => LeftOuterJoin
      case "LEFT" ~ "SEMI"  => LeftSemiJoin
      case "RIGHT"          => RightOuterJoin
      case "FULL" ~ "OUTER" => FullOuterJoin
      case "ANTI"           => AntiJoin
    }
    | "INNER" ^^^ InnerJoin)

  def parseWhere: Parser[Expression] = (
    "WHERE" ~> parseExpression)

  def parseGroupBy: Parser[GroupBy] = (
    "GROUP" ~> "BY" ~> rep1sep(parseAliasedExpression, ",") ^^
    { case exp => GroupBy(exp) })

  def parseHaving: Parser[Having] = ("HAVING" ~> parseExpression ^^
    { case exp => Having(exp) })

  def parseOrderBy: Parser[OrderBy] = (
    "ORDER" ~> "BY" ~> rep1sep(parseOrderKey, ",") ^^ { case keys => OrderBy(keys) })

  def parseOrderKey: Parser[(Expression, OrderType)] = (
    parseExpression ~ ("ASC" | "DESC").? ^^ {
      case v ~ Some("DESC") => (v, DESC)
      case v ~ Some("ASC")  => (v, ASC)
      case v ~ None         => (v, ASC)
    })

  def parseLimit: Parser[Limit] = (
    "LIMIT" ~> numericLit ^^ { case lim => Limit(lim.toInt) })

  class SqlLexical extends StdLexical {
    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }
    override def token: Parser[Token] =
      (identChar ~ rep(identChar | digit) ^^ { case first ~ rest => processIdent(first :: rest mkString "") }
        | rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
          case i ~ None    => NumericLit(i mkString "")
          case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
        }
        | '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") }
        | '\"' ~ rep(chrExcept('\"', '\n', EofCh)) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
        | EofCh ^^^ EOF
        | '\'' ~> failure("unclosed string literal")
        | '\"' ~> failure("unclosed string literal")
        | delim
        | failure("illegal character"))
    def regex(r: Regex): Parser[String] = new Parser[String] {
      def apply(in: Input) = {
        val source = in.source
        val offset = in.offset
        val start = offset // handleWhiteSpace(source, offset)
        (r findPrefixMatchOf (source.subSequence(start, source.length))) match {
          case Some(matched) =>
            Success(source.subSequence(start, start + matched.end).toString,
              in.drop(start + matched.end - offset))
          case None =>
            Success("", in)
        }
      }
    }
  }
  override val lexical = new SqlLexical

  def floatLit: Parser[String] =
    elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  lexical.reserved += (
    "SELECT", "AS", "OR", "AND", "GROUP", "ORDER", "BY", "WHERE",
    "JOIN", "ASC", "DESC", "FROM", "ON", "NOT", "HAVING",
    "EXISTS", "BETWEEN", "LIKE", "IN", "NULL", "LEFT", "RIGHT",
    "FULL", "OUTER", "SEMI", "INNER", "ANTI", "COUNT", "SUM", "AVG", "MIN", "MAX", "YEAR",
    "DATE", "TOP", "LIMIT", "CASE", "WHEN", "THEN", "ELSE", "END", "SUBSTRING")

  lexical.delimiters += (
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";")

}
