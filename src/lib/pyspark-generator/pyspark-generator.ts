/**
 * A custom generator for PySpark
 * @author Hong Yang
 * @version 1.0
 */

/**
 * @file PySpark code generator class, including helper methods for
 * generating PySpark for blocks.
 */

import { PythonGenerator } from "blockly/python";
import { Order } from "blockly/python";
import type { Block } from "blockly";
import type { Workspace } from "blockly";

export class PySparkGenerator extends PythonGenerator {
  /** @param name Name of the language the generator is for */
  constructor(name = "PySpark") {
    super(name);

    /**
     * Add PySpark-specific reserved words - I don't think this part is necessary for the demo,
     * since we only going to have functional blocks
     */
    this.addReservedWords(
      "SparkSession,SparkContext,RDD,DataFrame,Row,Column,DataFrameWriter," +
        "DataFrameReader,pyspark,sc,spark,udf,lit,col,expr,when,broadcast," +
        "Window,collect_list,collect_set,array,map,struct,explode,flatten," +
        "monotonically_increasing_id,from_unixtime,unix_timestamp,to_timestamp," +
        "to_date,date_format,year,month,dayofmonth,hour,minute,second," +
        "asc,desc,nulls_first,nulls_last,alias,coalesce,otherwise,over,partitionBy," +
        "range,rowsBetween,rangeBetween,unboundedPreceding,unboundedFollowing," +
        "currentRow,lag,lead,rank,dense_rank,percent_rank,ntile,row_number," +
        "first_value,last_value,count,sum,avg,mean,max,min,stddev,variance"
    );
  }

  /**
   * Initialize the database of variable names and add PySpark imports
   *
   * @param workspace Workspace to generate code from
   */
  init(workspace: Workspace) {
    super.init(workspace);

    // Add PySpark imports
    this.definitions_["import_pyspark_core"] =
      "from pyspark.sql import SparkSession";
    this.definitions_["import_pyspark_functions"] =
      "from pyspark.sql.functions import *";
    this.definitions_["import_pyspark_types"] =
      "from pyspark.sql.types import *";

    this.definitions_["spark_session"] =
      "# Create a SparkSession\n" +
      "spark = SparkSession.builder \\\n" +
      '    .appName("PySparkBlocklyApp") \\\n' +
      "    .getOrCreate()";

    // Add SparkContext reference for RDD operations
    this.definitions_["spark_context"] =
      "# Get the SparkContext for RDD operations\nsc = spark.sparkContext";

    this.isInitialized = true;
  }

  /**
   * Prepend the generated code with PySpark-specific imports and setup code.
   *
   * @param code Generated code.
   * @returns Completed code.
   */
  finish(code: string): string {
    return super.finish(code);
  }

  /**
   * PySpark-specific block to code conversion.
   * This is a hook to add PySpark-specific block handlers.
   *
   * @param block The block to convert to code.
   * @param opt_thisOnly True to generate code for only this statement.
   * @returns The generated code, or an array containing the code and operator precedence.
   * @override
   */
  blockToCode(
    block: Block | null,
    opt_thisOnly?: boolean
  ): string | [string, number] {
    if (!block) {
      return "";
    }

    // First check if we have a PySpark-specific block handler
    const blockType = block.type;
    if (blockType in this.forBlock) {
      const result = this.forBlock[blockType].call(this, block, this);
      // Handle null result case
      if (result === null) {
        return "";
      }
      return result;
    }

    // If not, fall back to Python's handling
    const result = super.blockToCode(block, opt_thisOnly);
    // Handle null result case
    if (result === null) {
      return "";
    }
    return result;
  }
}
