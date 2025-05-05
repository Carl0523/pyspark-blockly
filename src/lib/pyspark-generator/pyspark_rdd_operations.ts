import type { Block } from "blockly";
import type { PySparkGenerator } from "./pyspark-generator";
import { Order } from "blockly/python";

/**
 * Load text file as RDD
 */
export function pyspark_rdd_textfile(
  block: Block,
  generator: PySparkGenerator
): [string, Order] {
  const filePath = generator.valueToCode(block, "FILE_PATH", Order.ATOMIC);

  const code = `sc.textFile(${filePath})`;
  return [code, Order.FUNCTION_CALL];
}

/**
 * Map operation on RDD
 */
export function pyspark_rdd_map(
  block: Block,
  generator: PySparkGenerator
): [string, Order] {
  const rdd =
    generator.valueToCode(block, "RDD", Order.ATOMIC) || "sc.emptyRDD()";
  const func =
    generator.valueToCode(block, "FUNC", Order.NONE) || "lambda x: x";

  const code = `${rdd}.map(${func})`;
  return [code, Order.MEMBER];
}

/**
 * Filter operation on RDD
 */
export function pyspark_rdd_filter(
  block: Block,
  generator: PySparkGenerator
): [string, Order] {
  const rdd =
    generator.valueToCode(block, "RDD", Order.MEMBER) || "sc.emptyRDD()";
  const predicate =
    generator.valueToCode(block, "PREDICATE", Order.NONE) || "lambda x: True";

  const code = `${rdd}.filter(${predicate})`;
  return [code, Order.MEMBER];
}

/**
 * ReduceByKey operation on RDD
 */
export function pyspark_rdd_reducebykey(
  block: Block,
  generator: PySparkGenerator
): [string, Order] {
  const rdd =
    generator.valueToCode(block, "RDD", Order.MEMBER) || "sc.emptyRDD()";
  const func =
    generator.valueToCode(block, "FUNC", Order.NONE) || "lambda a, b: a + b";

  const code = `${rdd}.reduceByKey(${func})`;
  return [code, Order.MEMBER];
}

/**
 * Collect action on RDD
 */
export function pyspark_rdd_collect(
  block: Block,
  generator: PySparkGenerator
): [string, Order] {
    const rdd = generator.valueToCode(block, 'RDD', Order.MEMBER) || 'sc.emptyRDD()';

    const code = `${rdd}.collect()`;
    return [code, Order.FUNCTION_CALL];
}