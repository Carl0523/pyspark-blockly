import type { Block } from "blockly";
import type { PySparkGenerator } from "./pyspark-generator";
import { Order } from "blockly/python";

/**
 * Store a value in a variable
 */
export function pyspark_variable_set(
  block: Block,
  generator: PySparkGenerator
): string {
  const varName = block.getFieldValue("VAR_NAME");
  const value = generator.valueToCode(block, "VALUE", Order.NONE) || "None";

  const code = `${varName} = ${value}\n`;
  return code;
}

/**
 * Get the value of a variable
 */
export function pyspark_variable_get(
  block: Block,
  generator: PySparkGenerator
): [string, Order] {
  const varName = block.getFieldValue("VAR_NAME");

  const code = varName;
  return [code, Order.ATOMIC];
}

/**
 * Print a value
 */
export function pyspark_print(
  block: Block,
  generator: PySparkGenerator
): string {
  const value = generator.valueToCode(block, "VALUE", Order.NONE) || "None";

  const code = `print(${value})\n`;
  return code;
}