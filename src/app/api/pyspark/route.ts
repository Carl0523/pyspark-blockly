// app/api/run-pyspark/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { exec } from 'child_process';
import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { promisify } from 'util';

const execPromise = promisify(exec);
let imageChecked = false; // Simple cache to avoid checking on every request

export const dynamic = 'force-dynamic';

// Function to ensure the Docker image exists
async function ensureDockerImage() {
  if (imageChecked) return;
  
  try {
    // Check if image exists locally
    const { stdout } = await execPromise('docker images -q bitnami/spark:latest');
    
    if (!stdout.trim()) {
      console.log('Docker image not found locally, pulling now...');
      await execPromise('docker pull bitnami/spark:latest');
      console.log('Docker image pulled successfully');
    } else {
      console.log('Docker image already exists locally');
    }
    
    imageChecked = true;
  } catch (error) {
    console.error('Error checking or pulling Docker image:', error);
    throw error;
  }
}

export async function POST(request: NextRequest) {
  try {
    // Ensure Docker image exists before proceeding
    await ensureDockerImage();
    
    const { code } = await request.json();
    
    if (!code) {
      return NextResponse.json(
        { error: true, message: 'No code provided' }, 
        { status: 400 }
      );
    }

    // Rest of your code remains the same...
    // Create a unique ID for this run
    const id = uuidv4();
    const tempDir = path.join(process.cwd(), 'temp');
    const scriptPath = path.join(tempDir, `${id}.py`);
    
    // Ensure temp directory exists
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }

    
    fs.writeFileSync(scriptPath, code);
    
    try {
      // Run with Docker using the bitnami/spark image
      const { stdout, stderr } = await execPromise(
        `docker run --rm -v "${tempDir}:/app" -v "${process.cwd()}/public/data:/data" bitnami/spark:latest spark-submit /app/${path.basename(scriptPath)}`
      );
      
      // Clean up
      fs.unlinkSync(scriptPath);
      
      return NextResponse.json({
        success: true,
        output: stdout || "Execution completed with no output"
      });
    } catch (error: any) {
      // Clean up
      if (fs.existsSync(scriptPath)) {
        fs.unlinkSync(scriptPath);
      }
      
      console.error('Docker execution error:', error);
      
      return NextResponse.json({
        error: true,
        message: `Docker execution error: ${error.message}`,
        output: error.stderr
      }, { status: 500 });
    }
  } catch (error: any) {
    console.error('Error executing PySpark code:', error);
    return NextResponse.json({
      error: true,
      message: 'Server error while executing PySpark code'
    }, { status: 500 });
  }
}