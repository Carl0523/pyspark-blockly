import type { Block } from "blockly";
import type { PySparkGenerator } from "./pyspark-generator";
import { Order } from "blockly/python";

export function pyspark_remove_header_udf(
  block: Block,
  generator: PySparkGenerator
): string {
  const udfName = block.getFieldValue("UDF_NAME");

  let code = `def ${udfName}(rdd):\n`;
  code += generator.INDENT + `# Extract the header\n`;
  code += generator.INDENT + `header = rdd.first()\n`;
  code += generator.INDENT + `# Filter out the header row\n`;
  code +=
    generator.INDENT +
    `data_without_header = rdd.filter(lambda line: line != header)\n`;
  code += generator.INDENT + `return data_without_header\n\n`;

  return code;
}
