import * as Blockly from "blockly";

function defineRDDBlocks() {
  Blockly.Blocks["pyspark_rdd_textfile"] = {
    init: function (this: Blockly.Block) {
      this.appendValueInput("FILE_PATH")
        .setCheck("String")
        .appendField("Load text file as RDD");
      this.setOutput(true, "RDD");
      this.setColour(230);
      this.setTooltip("Read a text file from the given path and create an RDD");
      this.setHelpUrl("");
    },
  };

  Blockly.Blocks["pyspark_rdd_map"] = {
    init: function (this: Blockly.Block) {
      this.appendValueInput("RDD").setCheck("RDD").appendField("Map RDD");
      this.appendValueInput("FUNC")
        .setCheck("String")
        .appendField("with function");
      this.setOutput(true, "RDD");
      this.setColour(230);
      this.setTooltip("Apply a function to each element of the RDD");
      this.setHelpUrl("");
    },
  };

  Blockly.Blocks["pyspark_rdd_filter"] = {
    init: function (this: Blockly.Block) {
      this.appendValueInput("RDD").setCheck("RDD").appendField("Filter RDD");
      this.appendValueInput("PREDICATE")
        .setCheck("String")
        .appendField("with condition");
      this.setOutput(true, "RDD");
      this.setColour(230);
      this.setTooltip("Apply a condition to each element of the RDD");
      this.setHelpUrl("");
    },
  };

  Blockly.Blocks["pyspark_rdd_reducebykey"] = {
    init: function (this: Blockly.Block) {
      this.appendValueInput("RDD")
        .setCheck("RDD")
        .appendField("ReduceByKey on RDD");
      this.appendValueInput("FUNC")
        .setCheck("String")
        .appendField("with function");
      this.setOutput(true, "RDD");
      this.setColour(230);
      this.setTooltip(
        "Combine values for each key using the specified function"
      );
      this.setHelpUrl("");
    },
  };

  Blockly.Blocks["pyspark_rdd_collect"] = {
    init: function (this: Blockly.Block) {
      this.appendValueInput("RDD").setCheck("RDD").appendField("Collect RDD");
      this.setOutput(true, null);
      this.setColour(230);
      this.setTooltip("Return all elements of the RDD as a list");
      this.setHelpUrl("");
    },
  };
}

function defineUDFBlocks() {
  Blockly.Blocks["pyspark_define_simple_udf"] = {
    init: function (this: Blockly.Block) {
      this.appendDummyInput()
        .appendField("Define UDF")
        .appendField(new Blockly.FieldTextInput("my_udf"), "UDF_NAME");
      this.appendValueInput("FUNCTION")
        .setCheck("String")
        .appendField("function");
      this.appendDummyInput()
        .appendField("return type")
        .appendField(
          new Blockly.FieldDropdown([
            ["Integer", "IntegerType()"],
            ["String", "StringType()"],
            ["Double", "DoubleType()"],
            ["Boolean", "BooleanType()"],
          ]),
          "RETURN_TYPE"
        );
      this.setPreviousStatement(true, null);
      this.setNextStatement(true, null);
      this.setColour(290);
      this.setTooltip("Define a simple PySpark UDF");
      this.setHelpUrl("");
    },
  };

  Blockly.Blocks["pyspark_double_value_udf"] = {
    init: function (this: Blockly.Block) {
      this.appendDummyInput()
        .appendField("Define Double Value UDF")
        .appendField(new Blockly.FieldTextInput("double_value"), "UDF_NAME");
      this.setPreviousStatement(true, null);
      this.setNextStatement(true, null);
      this.setColour(290);
      this.setTooltip("Define a UDF that doubles numeric values");
      this.setHelpUrl("");
    },
  };

  Blockly.Blocks["pyspark_string_length_udf"] = {
    init: function () {
      this.appendDummyInput()
        .appendField("Define String Length UDF")
        .appendField(new Blockly.FieldTextInput("string_length"), "UDF_NAME");
      this.setPreviousStatement(true, null);
      this.setNextStatement(true, null);
      this.setColour(290);
      this.setTooltip("Define a UDF that returns the length of a string");
      this.setHelpUrl("");
    },
  };

  Blockly.Blocks["pyspark_is_null_udf"] = {
    init: function () {
      this.appendDummyInput()
        .appendField("Define Is Null UDF")
        .appendField(new Blockly.FieldTextInput("is_null"), "UDF_NAME");
      this.setPreviousStatement(true, null);
      this.setNextStatement(true, null);
      this.setColour(290);
      this.setTooltip("Define a UDF that checks if a value is null");
      this.setHelpUrl("");
    },
  };

  Blockly.Blocks["pyspark_apply_udf"] = {
    init: function () {
      this.appendValueInput("UDF_NAME").setCheck(null).appendField("Apply UDF");
      this.appendValueInput("COLUMN")
        .setCheck("String")
        .appendField("to column");
      this.setOutput(true, "Column");
      this.setColour(290);
      this.setTooltip("Apply a UDF to a DataFrame column");
      this.setHelpUrl("");
    },
  };
}

function defineVariableBlocks() {
  // Create a variable definition block
  Blockly.Blocks["pyspark_variable_set"] = {
    init: function (this: Blockly.Block) {
      this.appendValueInput("VALUE")
        .setCheck(null)
        .appendField("Store as")
        .appendField(new Blockly.FieldTextInput("rdd1"), "VAR_NAME");
      this.setPreviousStatement(true, null);
      this.setNextStatement(true, null);
      this.setColour(160);
      this.setTooltip("Store a value in a variable");
      this.setHelpUrl("");
    },
  };

  // Create a variable reference block
  Blockly.Blocks["pyspark_variable_get"] = {
    init: function (this: Blockly.Block) {
      this.appendDummyInput()
        .appendField("Get variable")
        .appendField(new Blockly.FieldTextInput("rdd1"), "VAR_NAME");
      this.setOutput(true, null);
      this.setColour(160);
      this.setTooltip("Get the value of a variable");
      this.setHelpUrl("");
    },
  };

  // Create a print block
  Blockly.Blocks["pyspark_print"] = {
    init: function (this: Blockly.Block) {
      this.appendValueInput("VALUE")
        .setCheck(null)
        .appendField("Print");
      this.setPreviousStatement(true, null);
      this.setNextStatement(true, null);
      this.setColour(160);
      this.setTooltip("Print a value");
      this.setHelpUrl("");
    },
  };
}

export function definePySparkBlocks() {
  defineRDDBlocks();
  defineUDFBlocks();
  defineVariableBlocks();
}

/**
 * Create a toolbox configuration for PySpark blocks.
 *
 * @returns A toolbox configuration object for use with Blockly
 */
export function getPySparkToolbox() {
  return {
    kind: "categoryToolbox",
    contents: [
      {
        kind: "category",
        name: "PySpark Inputs",
        colour: "180",
        contents: [
          {
            kind: "block",
            type: "text",
            fields: {
              TEXT: "/data/file.csv",
            },
          },
          {
            kind: "block",
            type: "text",
            fields: {
              TEXT: "col_name",
            },
          },
        ],
      },
      {
        kind: "category",
        name: "RDD Operations",
        colour: "230",
        contents: [
          { kind: "block", type: "pyspark_rdd_textfile" },
          { kind: "block", type: "pyspark_rdd_map" },
          { kind: "block", type: "pyspark_rdd_filter" },
          { kind: "block", type: "pyspark_rdd_reducebykey" },
          { kind: "block", type: "pyspark_rdd_collect" },
        ],
      },
      {
        kind: "category",
        name: "UDF Operations",
        colour: "290",
        contents: [
          { kind: "block", type: "pyspark_define_simple_udf" },
          { kind: "block", type: "pyspark_double_value_udf" },
          { kind: "block", type: "pyspark_string_length_udf" },
          { kind: "block", type: "pyspark_is_null_udf" },
          { kind: "block", type: "pyspark_apply_udf" },
        ],
      },
      {
        kind: "category",
        name: "Lambda Functions",
        colour: "210",
        contents: [
          {
            kind: "block",
            type: "text",
            fields: {
              TEXT: 'lambda x: x.split(",")',
            },
          },
          {
            kind: "block",
            type: "text",
            fields: {
              TEXT: "lambda x: float(x[1])",
            },
          },
          {
            kind: "block",
            type: "text",
            fields: {
              TEXT: "lambda x: (x[0], x[1])",
            },
          },
          {
            kind: "block",
            type: "text",
            fields: {
              TEXT: "lambda a, b: a + b",
            },
          },
          {
            kind: "block",
            type: "text",
            fields: {
              TEXT: 'lambda x: x.startswith("A")',
            },
          },
        ],
      },
      {
        kind: "category",
        name: "Variables",
        colour: "160",
        contents: [
          { kind: "block", type: "pyspark_variable_set" },
          { kind: "block", type: "pyspark_variable_get" },
          { kind: "block", type: "pyspark_print" },
        ],
      }
    ],
  };
}
