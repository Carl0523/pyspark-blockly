// components/PySparkToolbar.tsx
import React from 'react';
import {
  AppBar,
  Toolbar,
  Button,
  Box,
  Tooltip,
  IconButton,
} from '@mui/material';
import SaveIcon from '@mui/icons-material/Save';
import UploadIcon from '@mui/icons-material/Upload';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';

interface PySparkToolbarProps {
  onSave: () => void;
  onLoad: () => void;
  onRun: () => void;
  onHelp: () => void;
}

const PySparkToolbar: React.FC<PySparkToolbarProps> = ({
  onSave,
  onLoad,
  onRun,
  onHelp,
}) => {
  return (
    <AppBar 
      position="static" 
      color="default" 
      elevation={1}
      sx={{ mb: 2 }}
    >
      <Toolbar variant="dense">
        <Box sx={{ flexGrow: 1, display: 'flex', gap: 1 }}>
          <Tooltip title="Save workspace">
            <Button 
              startIcon={<SaveIcon />} 
              onClick={onSave}
              size="small"
              variant="outlined"
            >
              Save
            </Button>
          </Tooltip>
          
          <Tooltip title="Load workspace">
            <Button
              startIcon={<UploadIcon />}
              onClick={onLoad}
              size="small"
              variant="outlined"
            >
              Load
            </Button>
          </Tooltip>
          
          <Tooltip title="Run PySpark code">
            <Button
              startIcon={<PlayArrowIcon />}
              onClick={onRun}
              color="primary"
              variant="contained"
              size="small"
            >
              Run
            </Button>
          </Tooltip>
        </Box>
        
        <Tooltip title="Help">
          <IconButton
            aria-label="help"
            onClick={onHelp}
            size="small"
          >
            <HelpOutlineIcon />
          </IconButton>
        </Tooltip>
      </Toolbar>
    </AppBar>
  );
};

export default PySparkToolbar;