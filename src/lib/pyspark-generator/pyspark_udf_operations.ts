import type { Block } from "blockly";
import type { PySparkGenerator } from "./pyspark-generator";
import { Order } from "blockly/python";

export function pyspark_define_simple_udf(
  block: Block,
  generator: PySparkGenerator
): string {
  const udfName = block.getFieldValue("UDF_NAME");
  const func =
    generator.valueToCode(block, "FUNCTION", Order.NONE) || "lambda x: x";
  const returnType = block.getFieldValue("RETURN_TYPE");

  const code = `${udfName} = udf(${func}, ${returnType})\n`;

  return code;
}

export function pyspark_double_value_udf(
  block: Block,
  generator: PySparkGenerator
): string {
  const udfName = block.getFieldValue("UDF_NAME");

  let code = `def ${udfName}_function(x):\n`;
  code += generator.INDENT + `if x is None:\n`;
  code += generator.INDENT + `try:\n`;
  code += generator.INDENT + generator.INDENT + `return float(x) * 2\n`;
  code += generator.INDENT + `except (ValueError, TypeError):\n`;
  code += generator.INDENT + generator.INDENT + `return None\n\n`;
  code += `${udfName} = udf(${udfName}_function, DoubleType())\n`;

  return code;
}

export function pyspark_string_length_udf(
  block: Block,
  generator: PySparkGenerator
): string {
  const udfName = block.getFieldValue("UDF_NAME");

  let code = `def ${udfName}_function(s):\n`;
  code += generator.INDENT + `if s is None:\n`;
  code += generator.INDENT + generator.INDENT + `return None\n`;
  code += generator.INDENT + `try:\n`;
  code += generator.INDENT + generator.INDENT + `return len(str(s))\n`;
  code += generator.INDENT + `except:\n`;
  code += generator.INDENT + generator.INDENT + `return None\n\n`;
  code += `${udfName} = udf(${udfName}_function, IntegerType())\n`;

  return code;
}

export function pyspark_is_null_udf(
  block: Block,
  generator: PySparkGenerator
): string {
  const udfName = block.getFieldValue("UDF_NAME");

  let code = `def ${udfName}_function(x):\n`;
  code += generator.INDENT + `return x is None\n\n`;
  code += `${udfName} = udf(${udfName}_function, BooleanType())\n`;

  return code;
}

export function pyspark_apply_udf(
  block: Block,
  generator: PySparkGenerator
): [string, Order] {
  const udfName =
    generator.valueToCode(block, "UDF_NAME", Order.ATOMIC) || "my_udf";
  const column =
    generator.valueToCode(block, "COLUMN", Order.ATOMIC) || '"column"';

  const code = `${udfName}(col(${column}))`;
  return [code, Order.FUNCTION_CALL];
}

