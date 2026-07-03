const SyntheticSensors = require('./synthetic-sensors');

console.log('Initializing Synthetic Sensors Generator...');
console.log('Frequency: 100Hz (1 tick / 10ms)');
console.log('Run duration: 5 seconds\n');

const sensors = new SyntheticSensors({
    frequency: 100
});

// Accumulator for stats
const stats = {
    totalTicks: 0,
    history: {}
};

// Initialize history tracking for each channel to analyze at the end
for (const name of Object.keys(sensors.channels)) {
    stats.history[name] = {
        initial: sensors.channels[name].current,
        min: sensors.channels[name].current,
        max: sensors.channels[name].current,
        final: sensors.channels[name].current,
        values: []
    };
}

// Print header for real-time visualization
console.log(
    '  Timestamp (ns)   | Temp (°C) |    Latitude    |   Longitude    |  Accel X  |  Accel Y  |  Accel Z  '
);
console.log(
    '-------------------|-----------|----------------|----------------|-----------|-----------|-----------'
);

let tickCount = 0;
const currentValues = {};

// Start generator
sensors.start((reading) => {
    const { timestamp, channel, value } = reading;

    // Track stats
    stats.history[channel].values.push(value);
    if (value < stats.history[channel].min) stats.history[channel].min = value;
    if (value > stats.history[channel].max) stats.history[channel].max = value;
    stats.history[channel].final = value;

    // Cache current values to print summary row
    currentValues[channel] = value;

    // Since we generate at 100Hz, we trigger a consolidated console log at 10Hz (every 10 ticks)
    // to keep the console highly readable and avoid performance bottlenecks.
    if (channel === 'temperature') { // We tick through all channels once per interval, temperature is the last channel
        tickCount++;
        stats.totalTicks++;

        if (tickCount % 10 === 0) {
            const timeStr = String(timestamp).slice(-19); // Slice to keep it a reasonable length
            const tempStr = currentValues.temperature.toFixed(2).padStart(8);
            const latStr = currentValues.lat.toFixed(5).padStart(13);
            const lonStr = currentValues.lon.toFixed(5).padStart(13);
            const axStr = currentValues.accel_x.toFixed(2).padStart(8);
            const ayStr = currentValues.accel_y.toFixed(2).padStart(8);
            const azStr = currentValues.accel_z.toFixed(2).padStart(8);

            console.log(
                `${timeStr} | ${tempStr} | ${latStr} | ${lonStr} | ${axStr} | ${ayStr} | ${azStr}`
            );
        }
    }
});

// Run for 5 seconds (5000ms)
const RUN_DURATION_MS = 5000;
setTimeout(() => {
    sensors.stop();
    console.log('\nStopped sensor generation.');

    console.log('\n=== Simulation Statistics ===');
    console.log(`Total Intervals Ticked:  ${stats.totalTicks}`);
    console.log(`Expected Ticks:          ${(RUN_DURATION_MS / 10).toFixed(0)}`);
    console.log(`Actual Average Frequency: ${(stats.totalTicks / (RUN_DURATION_MS / 1000)).toFixed(2)} Hz`);
    console.log('\n=== Channel Random Walk Analysis ===');
    console.log(
        '  Channel    |  Initial  |    Min    |    Max    |   Final   | Net Change'
    );
    console.log(
        '-------------|-----------|-----------|-----------|-----------|-----------'
    );

    for (const [name, h] of Object.entries(stats.history)) {
        const cName = name.padEnd(12);
        const initVal = h.initial.toFixed(4).padStart(9);
        const minVal = h.min.toFixed(4).padStart(9);
        const maxVal = h.max.toFixed(4).padStart(9);
        const finalVal = h.final.toFixed(4).padStart(9);
        const change = (h.final - h.initial).toFixed(4).padStart(9);

        console.log(
            `${cName} | ${initVal} | ${minVal} | ${maxVal} | ${finalVal} | ${change}`
        );
    }
    console.log('=====================================\n');
    process.exit(0);
}, RUN_DURATION_MS);
