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
  
    // Check if we have a handler for the block
    const blockType = block.type;
    if (blockType in this.forBlock) {
      const result = this.forBlock[blockType].call(this, block, this);
      if (result === null) {
        return "";
      }
  
      if (typeof result === "string") {
        return this.scrub_(block, result, opt_thisOnly);
      }
  
      if (!opt_thisOnly && Array.isArray(result) && typeof result[0] === "string") {
        result[0] = this.scrub_(block, result[0], opt_thisOnly);
      }
  
      return result;
    }
  
    // Fall back to parent generator
    const result = super.blockToCode(block, opt_thisOnly);
    if (result === null) {
      return "";
    }
  
    // Also apply scrub_ here if fallback result is a string
    if (typeof result === "string") {
      return this.scrub_(block, result, opt_thisOnly);
    } else {
      return result;
    }
  }
  

  scrub_(block: Block, code: string, opt_thisOnly?: boolean): string {
    // If this is the only block we care about, return just its code.
    if (opt_thisOnly) {
      return code;
    }
  
    // Check if the current block is followed by another block in a statement stack
    const nextBlock = block.getNextBlock();
    const nextCode = nextBlock ? this.blockToCode(nextBlock) : "";
  
    // If blockToCode returns a [string, order] array, unwrap it
    const nextCodeStr = Array.isArray(nextCode) ? nextCode[0] : nextCode;
  
    return code + nextCodeStr;
  }

  
}
