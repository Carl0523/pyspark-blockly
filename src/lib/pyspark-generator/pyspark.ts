// lib/pyspark.ts - Updated to include text blocks
import * as rddOperations from "./pyspark_rdd_operations";
import * as variableOperations from "./pyspark_variable_operations"; // Add this line
import * as lambdaOperations from "./pyspark_lambda_operations";
import * as udfOperations from "./pyspark_udf_operations";
import { PySparkGenerator } from "./pyspark-generator";
import * as Blockly from "blockly";
import { pythonGenerator } from "blockly/python";


export * from "./pyspark-generator";

/**
 * PySpark code generator instance.
 * @type {!PySparkGenerator}
 */
export const pysparkGenerator = new PySparkGenerator();

pysparkGenerator.addReservedWords(
  "spark, SparkSession, SparkContext, RDD, DataFrame"
);

// Import the standard block handlers from the Python generator
// This is crucial for primitive blocks like "text"
pysparkGenerator.forBlock['text'] = pythonGenerator.forBlock['text'];
pysparkGenerator.forBlock['math_number'] = pythonGenerator.forBlock['math_number'];
pysparkGenerator.forBlock['logic_boolean'] = pythonGenerator.forBlock['logic_boolean'];
pysparkGenerator.forBlock['variables_get'] = pythonGenerator.forBlock['variables_get'];
pysparkGenerator.forBlock['variables_set'] = pythonGenerator.forBlock['variables_set'];

// Add custom PySpark blocks
const generators = { ...rddOperations, ...udfOperations, ...variableOperations, ...lambdaOperations };

Object.entries(generators).forEach(([name, generator]) => {
  pysparkGenerator.forBlock[name as keyof typeof pysparkGenerator.forBlock] =
    generator as any;
});