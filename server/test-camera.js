const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const CameraCapture = require('./camera-capture');

// Resolve tmp output directory in workspace root
const TMP_DIR = path.join(__dirname, '..', 'tmp');
if (!fs.existsSync(TMP_DIR)) {
    fs.mkdirSync(TMP_DIR, { recursive: true });
}

// Clear any existing frame files in tmp folder
const files = fs.readdirSync(TMP_DIR);
for (const file of files) {
    if (file.startsWith('frame_') && file.endsWith('.png')) {
        fs.unlinkSync(path.join(TMP_DIR, file));
    }
}

// Config: Default 15 FPS
const FPS = parseInt(process.env.CAMERA_FPS, 10) || 15;
console.log(`Starting camera capture test...`);
console.log(`Configured FPS: ${FPS}`);
console.log(`Saving frames to: ${TMP_DIR}`);

const capture = new CameraCapture({
    fps: FPS,
    quality: 70
});

let totalFrames = 0;
let lastFrameTime = null;
const frameIntervals = [];
const conversionPromises = [];

/**
 * Converts a JPEG buffer to a PNG file using FFmpeg.
 * Reads JPEG from stdin and writes PNG to file.
 */
function saveBufferAsPng(jpegBuffer, outputPath) {
    return new Promise((resolve, reject) => {
        const convertProcess = spawn('ffmpeg', [
            '-y',
            '-i', 'pipe:0', // input from stdin
            '-f', 'image2',
            outputPath
        ]);

        convertProcess.stdin.write(jpegBuffer);
        convertProcess.stdin.end();

        convertProcess.on('close', (code) => {
            if (code === 0) {
                resolve();
            } else {
                reject(new Error(`FFmpeg png conversion failed with code ${code}`));
            }
        });

        convertProcess.on('error', (err) => {
            reject(err);
        });
    });
}

// Listen to capture errors or FFmpeg logs
capture.on('error', (err) => {
    console.error('Capture Error:', err);
});

capture.on('log', (log) => {
    // Only log errors or unexpected warnings from FFmpeg to keep output clean
    if (log.includes('Error') || log.includes('warning') || log.includes('failed')) {
        console.warn(`[FFmpeg Log] ${log.trim()}`);
    }
});

const startTime = process.hrtime.bigint();

// Start camera capture
capture.start((frame) => {
    totalFrames++;
    const currentTime = frame.timestamp;

    if (lastFrameTime !== null) {
        // Calculate interval in milliseconds
        const intervalMs = Number(currentTime - lastFrameTime) / 1e6;
        frameIntervals.push(intervalMs);
    }
    lastFrameTime = currentTime;

    // Pad index for correct sorting (e.g. frame_0001.png)
    const paddedIndex = String(frame.frameIndex).padStart(4, '0');
    const filename = `frame_${paddedIndex}.png`;
    const outputPath = path.join(TMP_DIR, filename);

    // Save frame to disk asynchronously as PNG
    const p = saveBufferAsPng(frame.jpegBuffer, outputPath)
        .catch(err => console.error(`Error saving ${filename}:`, err));
    conversionPromises.push(p);

    if (frame.frameIndex % 15 === 0 || frame.frameIndex === 1) {
        console.log(`Captured frame index: ${frame.frameIndex} (timestamp: ${frame.timestamp})`);
    }
});

// Record for 10 seconds, then stop
const RECORD_DURATION_MS = 10000;
setTimeout(async () => {
    console.log('\nStopping camera capture after 10 seconds...');
    capture.stop();

    const endTime = process.hrtime.bigint();
    const durationSec = Number(endTime - startTime) / 1e9;

    console.log('Waiting for all frame PNG conversions to complete...');
    await Promise.all(conversionPromises);

    // Compute stats
    const avgFps = totalFrames / durationSec;
    const expectedFrames = FPS * (RECORD_DURATION_MS / 1000);
    const frameLoss = Math.max(0, expectedFrames - totalFrames);
    const frameLossPercent = ((frameLoss / expectedFrames) * 100).toFixed(2);

    let avgInterval = 0;
    let maxInterval = 0;
    let minInterval = Infinity;

    if (frameIntervals.length > 0) {
        const sum = frameIntervals.reduce((a, b) => a + b, 0);
        avgInterval = sum / frameIntervals.length;
        maxInterval = Math.max(...frameIntervals);
        minInterval = Math.min(...frameIntervals);
    }

    console.log('\n=== Camera Capture Results ===');
    console.log(`Actual Duration:     ${durationSec.toFixed(2)} seconds`);
    console.log(`Total Frames:        ${totalFrames}`);
    console.log(`Average FPS:         ${avgFps.toFixed(2)} (Target: ${FPS} FPS)`);
    console.log(`Expected Frames:     ${expectedFrames}`);
    console.log(`Frame Loss:          ${frameLoss} (${frameLossPercent}%)`);
    if (frameIntervals.length > 0) {
        console.log(`Avg Inter-frame:     ${avgInterval.toFixed(2)} ms (Ideal: ${(1000 / FPS).toFixed(2)} ms)`);
        console.log(`Min Inter-frame:     ${minInterval.toFixed(2)} ms`);
        console.log(`Max Inter-frame:     ${maxInterval.toFixed(2)} ms`);
    }
    console.log(`Frames directory:    ${TMP_DIR}`);
    console.log('==============================');

    process.exit(0);
}, RECORD_DURATION_MS);
