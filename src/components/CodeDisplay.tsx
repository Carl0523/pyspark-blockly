// components/CodeDisplay.tsx
import React from 'react';
import { Box, Paper, Typography, IconButton, Tooltip } from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';

interface CodeDisplayProps {
  code: string;
}

const CodeDisplay: React.FC<CodeDisplayProps> = ({ code }) => {
  const copyToClipboard = () => {
    navigator.clipboard.writeText(code);
  };

  return (
    <Box sx={{ position: 'relative' }}>
      <Typography variant="h6" gutterBottom>
        Generated PySpark Code
      </Typography>
      <Paper
        elevation={2}
        sx={{
          p: 2,
          bgcolor: '#272822', // Monokai-like background
          color: '#f8f8f2',
          fontFamily: 'monospace',
          fontSize: '0.875rem',
          overflow: 'auto',
          position: 'relative',
          height: '100%',
          minHeight: '200px',
          maxHeight: '450px',
          '&::-webkit-scrollbar': {
            width: '8px',
            height: '8px',
          },
          '&::-webkit-scrollbar-thumb': {
            backgroundColor: 'rgba(255, 255, 255, 0.2)',
            borderRadius: '4px',
          },
        }}
      >
        <pre style={{ margin: 0 }}>{code || '// No code generated yet'}</pre>
      </Paper>
      <Tooltip title="Copy to clipboard">
        <IconButton
          sx={{
            position: 'absolute',
            top: 40,
            right: 10,
            color: 'white',
            bgcolor: 'rgba(0, 0, 0, 0.3)',
            '&:hover': {
              bgcolor: 'rgba(0, 0, 0, 0.5)',
            },
          }}
          onClick={copyToClipboard}
          size="small"
        >
          <ContentCopyIcon fontSize="small" />
        </IconButton>
      </Tooltip>
    </Box>
  );
};

export default CodeDisplay;