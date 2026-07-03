const { spawn } = require('child_process');
const EventEmitter = require('events');

class CameraCapture extends EventEmitter {
    /**
     * @param {Object} config
     * @param {number} [config.fps=15] - Frame rate to capture (default 15)
     * @param {number} [config.quality=70] - JPEG compression quality 1-100 (default 70)
     * @param {string} [config.device] - Device identifier or index (default '0' for Mac, '/dev/video0' for Linux, 'test' for software source)
     * @param {string} [config.videoSize='640x480'] - Frame resolution (default '640x480')
     */
    constructor(config = {}) {
        super();
        this.fps = config.fps || parseInt(process.env.CAMERA_FPS, 10) || 15;
        this.quality = config.quality || parseInt(process.env.CAMERA_QUALITY, 10) || 70;
        this.videoSize = config.videoSize || process.env.CAMERA_VIDEO_SIZE || '640x480';
        
        // Setup device based on OS
        const platform = process.platform;
        if (config.device) {
            this.device = config.device;
        } else {
            this.device = process.env.CAMERA_DEVICE || '0';
        }

        this.ffmpegProcess = null;
        this.isCapturingState = false;
        this.frameIndex = 0;
        this.useFallback = false;
    }

    /**
     * Starts the camera capture stream.
     * @param {Function} callback - Callback called on every frame: ({ timestamp: bigint, frameIndex: number, jpegBuffer: Buffer }) => {}
     */
    start(callback) {
        if (this.isCapturingState) {
            return;
        }
        this.isCapturingState = true;
        this.frameIndex = 0;
        this.useFallback = (this.device === 'test' || this.device === 'dummy' || this.device === 'simulation');

        this._spawnFfmpeg(callback);
    }

    /**
     * Internal method to spawn the FFmpeg process.
     */
    _spawnFfmpeg(callback) {
        // Map quality (1-100) to ffmpeg q:v scale (1-31, where 1 is highest quality)
        const qscale = Math.max(1, Math.min(31, Math.round((100 - this.quality) / 3.3))) || 9;
        
        let args = [];

        if (this.useFallback) {
            // Software generation source
            args = [
                '-re',
                '-f', 'lavfi',
                '-i', `testsrc=size=${this.videoSize}:rate=${this.fps}`,
                '-f', 'image2pipe',
                '-codec:v', 'mjpeg',
                '-q:v', String(qscale),
                '-'
            ];
            this.emit('log', `Spawning FFmpeg software test source: ${args.join(' ')}`);
        } else {
            let inputFormat = 'avfoundation';
            let deviceInput = this.device;
            const platform = process.platform;

            if (platform === 'linux') {
                inputFormat = 'v4l2';
                if (deviceInput === '0') {
                    deviceInput = '/dev/video0';
                }
            } else if (platform === 'win32') {
                inputFormat = 'dshow';
                if (!deviceInput.startsWith('video=')) {
                    deviceInput = `video=${deviceInput}`;
                }
            }

            args = [
                '-f', inputFormat,
                '-framerate', String(this.fps),
                '-video_size', this.videoSize,
                '-i', deviceInput,
                '-f', 'image2pipe',
                '-codec:v', 'mjpeg',
                '-q:v', String(qscale),
                '-'
            ];
            this.emit('log', `Spawning FFmpeg hardware camera: ${args.join(' ')}`);
        }

        this.ffmpegProcess = spawn('ffmpeg', args);

        let buffer = Buffer.alloc(0);
        let hasError = false;

        this.ffmpegProcess.stdout.on('data', (chunk) => {
            if (!this.isCapturingState) return;

            buffer = Buffer.concat([buffer, chunk]);

            while (true) {
                // Find Start of Image (SOI) marker 0xFF 0xD8
                const soiIndex = buffer.indexOf(Buffer.from([0xFF, 0xD8]));
                if (soiIndex === -1) {
                    if (buffer.length > 1) {
                        buffer = buffer.slice(buffer.length - 1);
                    }
                    break;
                }

                // Discard any leading bytes before SOI
                if (soiIndex > 0) {
                    buffer = buffer.slice(soiIndex);
                }

                // Find End of Image (EOI) marker 0xFF 0xD9
                const eoiIndex = buffer.indexOf(Buffer.from([0xFF, 0xD9]), 2);
                if (eoiIndex === -1) {
                    break; // Wait for more data
                }

                // Extract JPEG buffer
                const jpegLength = eoiIndex + 2;
                const jpegBuffer = buffer.slice(0, jpegLength);

                buffer = buffer.slice(jpegLength);

                const timestamp = process.hrtime.bigint();
                const frameIndex = this.frameIndex++;

                const frameData = {
                    timestamp,
                    frameIndex,
                    jpegBuffer
                };

                if (callback && this.isCapturingState) {
                    try {
                        callback(frameData);
                    } catch (err) {
                        this.emit('error', err);
                    }
                }
                this.emit('frame', frameData);
            }
        });

        this.ffmpegProcess.stderr.on('data', (data) => {
            const logStr = data.toString();
            this.emit('log', logStr);

            // Automatically switch to software source if hardware camera fails
            if (!hasError && !this.useFallback && (
                logStr.includes('Error opening input') ||
                logStr.includes('Input/output error') ||
                logStr.includes('is not supported by the device') ||
                logStr.includes('not supported by the input device')
            )) {
                hasError = true;
                this.emit('log', 'WARNING: Hardware camera failed to initialize. Falling back to software test source...');
                
                // Clean up hardware process
                this.stop();

                // Restart in fallback mode
                this.useFallback = true;
                this.isCapturingState = true;
                this._spawnFfmpeg(callback);
            }
        });

        this.ffmpegProcess.on('close', (code, signal) => {
            if (!hasError) {
                this.ffmpegProcess = null;
                this.isCapturingState = false;
                this.emit('stop', { code, signal });
            }
        });

        this.ffmpegProcess.on('error', (err) => {
            if (!hasError && !this.useFallback) {
                hasError = true;
                this.emit('log', `FFmpeg process error: ${err.message}. Falling back to software test source...`);
                this.stop();
                this.useFallback = true;
                this.isCapturingState = true;
                this._spawnFfmpeg(callback);
            } else {
                this.emit('error', err);
            }
        });
    }

    /**
     * Stops the camera capture stream and cleans up the child process.
     */
    stop() {
        this.isCapturingState = false;
        if (this.ffmpegProcess) {
            try {
                this.ffmpegProcess.kill('SIGKILL');
            } catch (e) {
                // ignore
            }
            this.ffmpegProcess = null;
        }
    }

    /**
     * @returns {boolean} Whether the capture stream is active.
     */
    isCapturing() {
        return this.isCapturingState;
    }
}

module.exports = CameraCapture;
