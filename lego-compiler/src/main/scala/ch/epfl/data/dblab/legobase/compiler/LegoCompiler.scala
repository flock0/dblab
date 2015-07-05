package ch.epfl.data
package dblab.legobase
package compiler

import Config._
import schema._
import deep._
import utils._
import prettyprinter._
import optimization._
import optimization.c._
import sc.pardis.optimization._
import sc.pardis.ir._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.types._
import sc.pardis.compiler._
import scala.reflect._
import ch.epfl.data.dblab.legobase.frontend.OperatorAST._

/**
 * The class which is responsible for wiring together different parts of the compilation pipeline
 * such as program reification, optimization pipeline, and code generation.
 *
 * @param DSL the polymorphic embedding trait which contains the reified program.
 * This object takes care of online partial evaluation
 * @param number specifies the TPCH query number (TODO should be removed)
 * @param generateCCode specifies the target code language.
 * @param settings the compiler settings provided as command line arguments
 */
object LegoCompiler /*(val DSL: LoweringLegoBase, val number: Int, val generateCCode: CodeGenerationLang, val settings: Settings, val schema: Schema)*/ extends Compiler[LoweringLegoBase] with LegoRunner {
  val DSL: LoweringLegoBase = new LoweringLegoBase {}
  import DSL.unit
  import DSL._
  val generateCCode = Config.codeGenLang
  val outputFile = "generatedCode" // TODO -- GENERALIZE FOR DIFFERENT FILE NAMES
  val queryNumber = 23 // Dummy -- TODO: Will die

  /*def outputFile: String = {
    def queryWithNumber =
      //if (settings.isSynthesized)
      //settings.queryName
      //else
      "Q" + number
    def argsString = settings.args.filter(_.startsWith("+")).map(_.drop(1)).sorted.mkString("_")
    if (settings.nameIsWithFlag)
      argsString + "_" + queryWithNumber
    else
      queryWithNumber
  }
*/
  val reportCompilationTime: Boolean = true

  def compile[T: PardisType](program: => Expression[T]): Unit = compile[T](program, outputFile)

  override def compile[T: PardisType](program: => Expression[T], outputFile: String): Unit = {
    if (reportCompilationTime) {
      val block = utils.Utilities.time(DSL.reifyBlock(program), "Reification")
      val optimizedBlock = utils.Utilities.time(optimize(block), "Optimization")
      val irProgram = irToPorgram.createProgram(optimizedBlock)
      utils.Utilities.time(codeGenerator.generate(irProgram, outputFile), "Code Generation")
    } else {
      super.compile(program, outputFile)
    }
  }
  /*
  override def irToPorgram = if (generateCCode == CCodeGeneration) {
    IRToCProgram(DSL)
  } else {
    IRToProgram(DSL)
  }

*/
  lazy val codeGenerator =
    if (generateCCode == CCodeGeneration) {
      if (settings.noLetBinding)
        new LegoCASTGenerator(DSL, false, outputFile, true)
      else
        new LegoCGenerator(false, outputFile, true)
    } else {
      if (settings.noLetBinding)
        new LegoScalaASTGenerator(DSL, false, outputFile)
      else
        new LegoScalaGenerator(false, outputFile)
    }

  def setupCompilerPipeline(schema: Schema): Unit = {
    /*
    /**
     * If MultiMap is remaining without being converted to something which doesn't have set,
     * the field removal causes the program to be wrong
     */
    //TODO-GEN Remove gen and make string compression transformer dependant on removing unnecessary fields.
    def shouldRemoveUnusedFields = settings.stringCompression || (settings.hashMapPartitioning ||
      (
        settings.hashMapLowering && (settings.setToArray || settings.setToLinkedList))) && !settings.noFieldRemoval

    pipeline += new StatisticsEstimator(DSL, schema)

    pipeline += LBLowering(shouldRemoveUnusedFields)
    pipeline += TreeDumper(false)
    pipeline += ParameterPromotion
    pipeline += DCE
    pipeline += PartiallyEvaluate

    // pipeline += PartiallyEvaluate
    pipeline += HashMapHoist
    if (!settings.noSingletonHashMap)
      pipeline += SingletonHashMapToValueTransformer

    if (settings.hashMapPartitioning) {

      if (queryNumber == 18) {
        pipeline += ConstSizeArrayToLocalVars
        pipeline += DCE
        pipeline += TreeDumper(true)
        pipeline += new HashMapTo1DArray(DSL)
      }
      pipeline += new HashMapPartitioningTransformer(DSL, queryNumber, schema)

      pipeline += ParameterPromotion
      pipeline += PartiallyEvaluate
      pipeline += DCE
    }

    if (settings.stringCompression) pipeline += new StringDictionaryTransformer(DSL, schema)
    // pipeline += TreeDumper(false)

    if (settings.hashMapLowering || settings.hashMapNoCollision) {
      if (settings.hashMapLowering) {
        pipeline += new sc.pardis.deep.scalalib.collection.MultiMapOptimalTransformation(DSL)
        pipeline += new HashMapToSetTransformation(DSL, queryNumber)
      }
      if (settings.hashMapNoCollision) {
        pipeline += new HashMapNoCollisionTransformation(DSL, queryNumber)
        // pipeline += TreeDumper(false)
      }
      // pipeline += PartiallyEvaluate
      pipeline += DCE

      if (settings.setToLinkedList) {
        pipeline += SetLinkedListTransformation
        if (settings.containerFlattenning) {
          pipeline += ContainerFlatTransformer
        }
        pipeline += ContainerLowering
      }

      if (settings.setToArray) {
        pipeline += SetArrayTransformation
      }
      if (settings.setToLinkedList || settings.setToArray || settings.hashMapNoCollision) {
        pipeline += AssertTransformer(TypeAssertion(t => !t.isInstanceOf[DSL.SetType[_]]))
        //pipeline += ParameterPromotion
        pipeline += DCE
        pipeline += PartiallyEvaluate
        pipeline += new OptionToCTransformer(DSL) | new Tuple2ToCTransformer(DSL)
        pipeline += ParameterPromotion
      }

      pipeline += new BlockFlattening(DSL) // should not be needed!
    }

    val partitionedQueries = List(3, 6, 10, 14)
    if (settings.partitioning && partitionedQueries.contains(queryNumber)) {
      pipeline += new WhileToRangeForeachTransformer(DSL)
      pipeline += new ArrayPartitioning(DSL, queryNumber)
      pipeline += DCE
    }

    // pipeline += PartiallyEvaluate
    // pipeline += DCE

    if (settings.constArray) {
      pipeline += ConstSizeArrayToLocalVars
      // pipeline += SingletonArrayToValueTransformer
    }

    if (settings.columnStore) {

      pipeline += new ColumnStoreTransformer(DSL, settings)
      // if (settings.hashMapPartitioning) {
      //   pipeline += new ColumnStore2DTransformer(DSL, number)
      // }
      pipeline += ParameterPromotion
      pipeline += PartiallyEvaluate

      pipeline += DCE
      pipeline += ParameterPromotion
      pipeline += PartiallyEvaluate
      pipeline += DCE
      pipeline += ParameterPromotion
      pipeline += DCE
    }

    if (settings.mallocHoisting) {
      pipeline += new MemoryAllocationHoist(DSL, schema)
    }

    if (settings.stringOptimization) {
      pipeline += new StringOptimization(DSL)
    }

    if (settings.largeOutputHoisting && !settings.onlyLoading) {
      pipeline += new LargeOutputPrintHoister(DSL, schema)
    }
*/
    if (generateCCode == CCodeGeneration) pipeline += new CTransformersPipeline(settings)
    /*
    pipeline += DCECLang //NEVER REMOVE!!!!
*/
  }
  /*
  /*object Q12SynthesizedExtract {
    val Pat = "Q12S(_\\w)?_(\\d*)".r
    def unapply(str: String): Option[(Boolean, Int)] = str match {
      case Pat(target, numFieldsStr) =>
        val isCCode = if (target == null) false else true
        val numFields = numFieldsStr.toInt
        Some(isCCode -> numFields)
      case _ => None
    }
  }*/*/

  var settings: Settings = _

  def main(args: Array[String]) {
    if (args.length < 3) {
      import Settings._
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <data_folder> <scaling_factor_number> <list of queries to run> <copy>? <+optimizations> <-options>")
      System.out.println("     : data_folder_name should contain folders named sf0.1 sf1 sf2 sf4 etc")
      System.out.println("  Available optimizations:")
      System.out.println(Settings.ALL_SETTINGS.collect({ case opt: OptimizationSetting => opt.fullFlagName + ": " + opt.description }).mkString(" " * 6, "\n" + " " * 6, ""))
      System.out.println("  Available options:")
      System.out.println(Settings.ALL_SETTINGS.collect({ case opt: OptionSetting => opt.fullFlagName + ": " + opt.description }).mkString(" " * 6, "\n" + " " * 6, ""))
      // System.out.println("""  Synthesized queries:
      //       Q12S[_C]_N: N is the number of fields of the Lineitem table which should be used.
      // """)
      System.exit(0)
    }
    Config.checkResults = false
    settings = new Settings(args.toList)
    run(args)
  }

  /**
   * Generates a program for the given TPCH query with the given scaling factor.
   *
   * First, the target language and the TPCH query number is extracted from the
   * input string. Second, the corresponding query generator in the polymorphic embedding
   * traits is specified and is put into a thunk. Then, the setting arugments are validated
   * and parsed using [[Settings]] class. Finally, a [[LegoCompiler]] object is created and
   * its `compile` method is invoked by passing an appropriate query generator.
   *
   * @param query the input TPCH query together with the target language
   */
  def executeQuery(operatorTree: OperatorNode, schema: Schema): Unit = {
    settings.validate(Config.codeGenLang, queryNumber)
    setupCompilerPipeline(schema)

    val deepQueryPlan = convertOperator(operatorTree).asInstanceOf[Rep[Operator[Any]]]
    //val compiler = new LegoCompiler(context, queryNumber, Config.codeGenLang, settings, schema)
    compile({
      __newRange(0, unit(Config.numRuns), unit(1)).foreach(__lambda(((i: Rep[Int]) => {
        //GenericEngine.runQuery[Unit]({
        operatorOpen(deepQueryPlan)
        operatorNext(deepQueryPlan)
      })))
      //})
      //Utilities.time({
      //queryPlan.open
      //queryPlan.next
      //}, "Query")
      //unit()
    })
  }

  // Query compilation methods

  // TODO -- Will die once records are generalized
  def getTable[A: ClassTag](tableName: String): Rep[Array[A]] = tableName match {
    case "NATION" => TPCHLoader.loadNation().asInstanceOf[Rep[Array[A]]];
  }

  /*__newPrintOp(aggOp)(__lambda(((kv: this.Rep[ch.epfl.data.dblab.legobase.queryengine.AGGRecord[String]]) => {
          kv.key;
          printf(unit("%.2f|\n"), kv.aggs.apply(unit(0)))
        })), unit(-1));*/

  def convertOperator(node: OperatorNode): Rep[Operator[_]] = node match {
    case ScanOpNode(table, _, _, tp) =>
      __newScanOp(getTable(table.name)(tp))
    //new ScanOp(Loader.loadTable(table)(tp))
    /*case SelectOpNode(parent, cond, _) =>
      createSelectOperator(parent, cond)
    case JoinOpNode(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias) =>
      createJoinOperator(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias)
    case AggOpNode(parent, aggs, gb, aggAlias) =>
      createAggOpOperator(parent, aggs, gb, aggAlias)
    case MapOpNode(parent, indices) =>
      createMapOperator(parent, indices)
    case OrderByNode(parent, orderBy) =>
      createSortOperator(parent, orderBy)*/
    // case PrintOpNode(parent, projNames, limit) =>
    // createPrintOperator(parent, projNames, limit)
    /*case SubqueryNode(parent) => convertOperator(parent)
    case SubquerySingleResultNode(parent) =>
      new SubquerySingleResult(convertOperator(parent))*/
  }
}
