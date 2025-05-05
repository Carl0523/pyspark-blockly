// app/api/run-pyspark/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { exec } from 'child_process';
import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { promisify } from 'util';

const execPromise = promisify(exec);

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  try {
    const { code } = await request.json();
    
    if (!code) {
      return NextResponse.json(
        { error: true, message: 'No code provided' }, 
        { status: 400 }
      );
    }

    // Create a unique ID for this run
    const id = uuidv4();
    const tempDir = path.join(process.cwd(), 'temp');
    const scriptPath = path.join(tempDir, `${id}.py`);
    
    // Ensure temp directory exists
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }
    
    // Write code to a file
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