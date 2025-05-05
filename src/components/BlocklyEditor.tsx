// components/BlocklyEditor.tsx - Fixed version
import React, { useEffect, useRef, useState } from 'react';
import { Box } from '@mui/material';
import * as Blockly from 'blockly';
import { definePySparkBlocks, getPySparkToolbox } from '../lib/pyspark-generator/pyspark_blocks';
import { pysparkGenerator } from '../lib/pyspark-generator/pyspark';

interface BlocklyEditorProps {
  onCodeChange: (code: string) => void;
}

export interface BlocklyEditorHandle {
  generateCode: () => string;
  saveWorkspace: () => string;
  loadWorkspace: (json: string) => void;
}

const BlocklyEditor = React.forwardRef<BlocklyEditorHandle, BlocklyEditorProps>(
  ({ onCodeChange }, ref) => {
    const blocklyDivRef = useRef<HTMLDivElement>(null);
    const workspaceRef = useRef<Blockly.WorkspaceSvg | null>(null);
    const [isInitialized, setIsInitialized] = useState(false);

    // Create the workspace only once
    useEffect(() => {
      // Only initialize once
      if (isInitialized || !blocklyDivRef.current) return;

      // Define PySpark blocks
      definePySparkBlocks();

      // Initialize Blockly
      workspaceRef.current = Blockly.inject(blocklyDivRef.current, {
        toolbox: getPySparkToolbox(),
        grid: {
          spacing: 20,
          length: 3,
          colour: '#ccc',
          snap: true,
        },
        zoom: {
          controls: true,
          wheel: true,
          startScale: 1.0,
          maxScale: 3,
          minScale: 0.3,
          scaleSpeed: 1.2,
        },
        trashcan: true,
      });

      // Initialize generator
      pysparkGenerator.init(workspaceRef.current);

      // Add change listener using a function reference to avoid recreation
      const changeListener = () => {
        if (workspaceRef.current) {
          const code = pysparkGenerator.workspaceToCode(workspaceRef.current);
          onCodeChange(code);
        }
      };
      
      workspaceRef.current.addChangeListener(changeListener);
      setIsInitialized(true);

      // Add resize handler
      const resizeBlockly = () => {
        if (workspaceRef.current) {
          Blockly.svgResize(workspaceRef.current);
        }
      };

      const resizeObserver = new ResizeObserver(resizeBlockly);
      if (blocklyDivRef.current) {
        resizeObserver.observe(blocklyDivRef.current);
      }
      
      // This runs only when component unmounts
      return () => {
        resizeObserver.disconnect();
        if (workspaceRef.current) {
          workspaceRef.current.removeChangeListener(changeListener);
          workspaceRef.current.dispose();
          workspaceRef.current = null;
        }
        setIsInitialized(false);
      };
    }, []); // Empty dependency array - run only once

    // Expose methods to parent via ref
    React.useImperativeHandle(ref, () => ({
      generateCode: () => {
        if (!workspaceRef.current) return '';
        return pysparkGenerator.workspaceToCode(workspaceRef.current);
      },
      saveWorkspace: () => {
        if (!workspaceRef.current) return '';
        const state = Blockly.serialization.workspaces.save(workspaceRef.current);
        return JSON.stringify(state);
      },
      loadWorkspace: (jsonText: string) => {
        if (!workspaceRef.current) return;
        try {
          const state = JSON.parse(jsonText);
          Blockly.serialization.workspaces.load(state, workspaceRef.current);
        } catch (e) {
          console.error('Error loading workspace:', e);
        }
      },
    }), []);

    return (
      <Box
        ref={blocklyDivRef}
        sx={{
          width: '100%',
          height: '100%',
          minHeight: '600px',
          border: '1px solid #e0e0e0',
          borderRadius: 1,
        }}
      />
    );
  }
);

BlocklyEditor.displayName = 'BlocklyEditor';

export default BlocklyEditor;