// Create a new file: pyspark_lambda_operations.ts
import type { Block } from "blockly";
import type { PySparkGenerator } from "./pyspark-generator";
import { Order } from "blockly/python";

export function pyspark_lambda(
  block: Block,
  generator: PySparkGenerator
): [string, Order] {
  const lambdaText = block.getFieldValue("LAMBDA");
  return [`lambda ${lambdaText}`, Order.ATOMIC];
}