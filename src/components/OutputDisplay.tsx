// components/OutputDisplay.tsx
'use client';

import React from 'react';
import { Box, Paper, Typography, CircularProgress } from '@mui/material';

interface OutputDisplayProps {
  output: string;
  isRunning: boolean;
}

const OutputDisplay: React.FC<OutputDisplayProps> = ({ output, isRunning }) => {
  return (
    <Box sx={{ mt: 2 }}>
      <Typography variant="h6" gutterBottom display="flex" alignItems="center">
        Execution Output
        {isRunning && (
          <CircularProgress size={20} sx={{ ml: 2 }} />
        )}
      </Typography>
      <Paper
        elevation={1}
        sx={{
          p: 2,
          bgcolor: '#000',
          color: '#0f0',
          fontFamily: 'monospace',
          fontSize: '0.875rem',
          overflow: 'auto',
          maxHeight: '300px',
          minHeight: '150px',
          whiteSpace: 'pre-wrap',
        }}
      >
        {output || 'No output yet. Run your code to see results here.'}
      </Paper>
    </Box>
  );
};

export default OutputDisplay;