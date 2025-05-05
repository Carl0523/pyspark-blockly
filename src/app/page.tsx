// app/pyspark-editor/page.tsx
"use client";

import React, { useRef, useState, useCallback } from "react";
import { Box, Container, Grid, Typography, Paper } from "@mui/material";
import dynamic from "next/dynamic";
import CodeDisplay from "@/components/CodeDisplay";
import PySparkToolbar from "@/components/PySparkToolbar";
import OutputDisplay from "@/components/OutputDisplay";

// Import Blockly editor with dynamic import to avoid SSR issues
const BlocklyEditor = dynamic(() => import("@/components/BlocklyEditor"), {
  ssr: false,
});

import type { BlocklyEditorHandle } from "@/components/BlocklyEditor";

export default function PySparkEditorPage() {
  const [code, setCode] = useState("// PySpark code will appear here");
  const [output, setOutput] = useState("");
  const [isRunning, setIsRunning] = useState(false);
  const editorRef = useRef<BlocklyEditorHandle>(null);

  // Use useCallback to prevent function recreation on each render
  const handleCodeChange = useCallback((generatedCode: string) => {
    setCode(generatedCode);
  }, []);

  // For saving the Python code
  const handleSave = () => {
    if (!editorRef.current) return;

    // Save the Python code
    const pythonCode = code; // Use the current code state that's already being tracked

    // Create a blob and trigger download
    const blob = new Blob([pythonCode], { type: "text/plain" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = "pyspark_workspace.py";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };
  
  const handleRun = async () => {
    // Show loading state
    setIsRunning(true);
    setOutput("Running PySpark code...");

    try {
      const response = await fetch("/api/pyspark", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ code }),
      });

      const result = await response.json();

      if (result.error) {
        setOutput(`Error: ${result.message}\n\n${result.output || ""}`);
      } else {
        setOutput(
          result.output || "Code executed successfully with no output."
        );
      }
    } catch (error: any) {
      setOutput(`Failed to execute code: ${error.message}`);
    } finally {
      setIsRunning(false);
    }
  };

  const handleHelp = () => {
    alert(
      "PySpark Blockly Editor Help:\n\n" +
        "1. Drag blocks from the toolbox on the left\n" +
        "2. Connect blocks to build your PySpark program\n" +
        "3. The generated code appears in the panel\n" +
        "4. Click Run to execute your code\n" +
        "5. View execution results in the output panel"
    );
  };

  return (
    <Box sx={{ display: "flex", flexDirection: "column", height: "100vh" }}>
      <PySparkToolbar
        onSave={handleSave}
      
        onRun={handleRun}
        onHelp={handleHelp}
      />

      <Container maxWidth="xl" sx={{ flexGrow: 1, py: 2 }}>
        <Typography variant="h4" gutterBottom>
          PySpark Block Editor
        </Typography>

        <Grid container spacing={2} sx={{ height: "calc(100% - 70px)" }}>
          <Grid size={{ xs: 12, md: 8 }} sx={{ height: "100%" }}>
            <Paper
              elevation={3}
              sx={{
                height: "100%",
                p: 2,
                display: "flex",
                flexDirection: "column",
              }}
            >
              <Typography variant="h6" gutterBottom>
                Workspace
              </Typography>
              <Box sx={{ flexGrow: 1 }}>
                <BlocklyEditor
                  ref={editorRef}
                  onCodeChange={handleCodeChange}
                />
              </Box>
            </Paper>
          </Grid>

          <Grid size={{ xs: 12, md: 4 }} sx={{ height: "100%" }}>
            <Paper
              elevation={3}
              sx={{
                height: "100%",
                p: 2,
                display: "flex",
                flexDirection: "column",
              }}
            >
              <CodeDisplay code={code} />
              <OutputDisplay output={output} isRunning={isRunning} />
            </Paper>
          </Grid>
        </Grid>
      </Container>
    </Box>
  );
}
