const EventEmitter = require('events');

class SyntheticSensors extends EventEmitter {
    /**
     * @param {Object} [config] - Configuration options
     * @param {number} [config.frequency=100] - Generation frequency in Hz (default 100)
     * @param {Object} [config.channels] - Channel overrides for bounds and noise standard deviations
     */
    constructor(config = {}) {
        super();
        
        this.frequency = config.frequency || parseInt(process.env.SENSOR_FREQUENCY, 10) || 100;
        this.intervalMs = Math.max(1, Math.round(1000 / this.frequency));
        
        // Define default states, bounds, and noise standard deviations for all 7 channels
        const defaultChannels = {
            accel_x: { initial: 0.0, min: -20.0, max: 20.0, std: 0.1 },
            accel_y: { initial: 0.0, min: -20.0, max: 20.0, std: 0.1 },
            accel_z: { initial: 9.81, min: -20.0, max: 20.0, std: 0.1 }, // z-axis starts with gravity
            lat: { initial: 41.0082, min: 41.0000, max: 41.0200, std: 0.00005 },
            lon: { initial: 28.9784, min: 28.9700, max: 28.9900, std: 0.00005 },
            alt: { initial: 50.0, min: 10.0, max: 100.0, std: 0.2 },
            temperature: { initial: 20.0, min: 15.0, max: 35.0, std: 0.05 }
        };

        // Merge custom channel configuration overrides
        this.channels = {};
        for (const [key, defaults] of Object.entries(defaultChannels)) {
            const override = (config.channels && config.channels[key]) || {};
            this.channels[key] = {
                current: override.initial !== undefined ? override.initial : defaults.initial,
                min: override.min !== undefined ? override.min : defaults.min,
                max: override.max !== undefined ? override.max : defaults.max,
                std: override.std !== undefined ? override.std : defaults.std
            };
        }

        this.timer = null;
        this.isGeneratingState = false;
    }

    /**
     * Generates a normally distributed (Gaussian) random number using the Box-Muller transform.
     * @param {number} mean - Mean of the distribution (0)
     * @param {number} std - Standard deviation of the distribution
     * @returns {number} Gaussian random number
     */
    _gaussianRandom(mean, std) {
        let u = 0;
        let v = 0;
        // Avoid 0 to prevent Infinity or NaN issues with Math.log
        while (u === 0) u = Math.random();
        while (v === 0) v = Math.random();
        
        const z = Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
        return z * std + mean;
    }

    /**
     * Starts the 100Hz generation loop.
     * @param {Function} callback - Callback triggered for every generated reading: ({ timestamp: bigint, channel: string, value: number }) => {}
     */
    start(callback) {
        if (this.isGeneratingState) {
            return;
        }
        this.isGeneratingState = true;

        this.timer = setInterval(() => {
            const timestamp = process.hrtime.bigint();

            for (const [name, config] of Object.entries(this.channels)) {
                // Compute random walk: val = prev + noise
                const noise = this._gaussianRandom(0, config.std);
                let nextValue = config.current + noise;

                // Clamp to realistic bounds
                if (nextValue < config.min) {
                    nextValue = config.min;
                } else if (nextValue > config.max) {
                    nextValue = config.max;
                }

                config.current = nextValue;

                const reading = {
                    timestamp,
                    channel: name,
                    value: nextValue
                };

                if (callback) {
                    try {
                        callback(reading);
                    } catch (err) {
                        this.emit('error', err);
                    }
                }
                this.emit('data', reading);
            }
        }, this.intervalMs);
    }

    /**
     * Stops the generation loop.
     */
    stop() {
        if (!this.isGeneratingState) {
            return;
        }
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
        this.isGeneratingState = false;
    }

    /**
     * @returns {boolean} Whether the generator is active.
     */
    isGenerating() {
        return this.isGeneratingState;
    }
}

module.exports = SyntheticSensors;
