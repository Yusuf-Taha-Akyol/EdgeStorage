const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const DATA_DIR = path.join(__dirname, '..', 'data');

// Clean up old storage folders for a clean demo start (optional, but good)
console.log('Initializing EdgeStorage Demo Runner...');
console.log('Cleaning up old directories for a fresh start...');
['drone_01', 'drone_02', 'drone_03', 'cloud'].forEach(dir => {
    const fullPath = path.join(DATA_DIR, dir);
    if (fs.existsSync(fullPath)) {
        try {
            fs.rmSync(fullPath, { recursive: true, force: true });
        } catch (e) {
            console.warn(`Could not clear directory ${dir}: ${e.message}`);
        }
    }
});

const processes = [];

function startProcess(name, script, env = {}, colorCode = '37') {
    const proc = spawn(process.execPath, [script], {
        cwd: __dirname,
        env: {
            ...process.env,
            ...env
        }
    });

    proc.stdout.on('data', (data) => {
        const text = data.toString().trim();
        if (text) {
            console.log(`\x1b[${colorCode}m[${name}]\x1b[0m ${text}`);
        }
    });

    proc.stderr.on('data', (data) => {
        const text = data.toString().trim();
        if (text) {
            console.error(`\x1b[31m[${name} ERR]\x1b[0m ${text}`);
        }
    });

    processes.push({ name, proc });
    return proc;
}

// 1. Start Cloud Server on port 4000
console.log('Starting Cloud Server on Port 4000...');
startProcess('CLOUD', 'cloud.js', { PORT: '4000' }, '32'); // Green prefix

// Wait 1.5 seconds for Cloud to boot up
setTimeout(() => {
    // 2. Start Drone 01 on port 3001
    console.log('Starting Drone-01 Edge Node on Port 3001...');
    startProcess('DRONE-01', 'server.js', {
        PORT: '3001',
        STORAGE_PATH: path.join(DATA_DIR, 'drone_01'),
        DEVICE_ID: 'drone-01',
        API_KEY: 'api-key-drone-01',
        CLOUD_URL: 'http://localhost:4000'
    }, '34'); // Blue prefix

    // 3. Start Drone 02 on port 3002
    console.log('Starting Drone-02 Edge Node on Port 3002...');
    startProcess('DRONE-02', 'server.js', {
        PORT: '3002',
        STORAGE_PATH: path.join(DATA_DIR, 'drone_02'),
        DEVICE_ID: 'drone-02',
        API_KEY: 'api-key-drone-02',
        CLOUD_URL: 'http://localhost:4000'
    }, '35'); // Magenta prefix

    // 4. Start Drone 03 on port 3003
    console.log('Starting Drone-03 Edge Node on Port 3003...');
    startProcess('DRONE-03', 'server.js', {
        PORT: '3003',
        STORAGE_PATH: path.join(DATA_DIR, 'drone_03'),
        DEVICE_ID: 'drone-03',
        API_KEY: 'api-key-drone-03',
        CLOUD_URL: 'http://localhost:4000'
    }, '36'); // Cyan prefix

    console.log('\n=============================================================');
    console.log('EdgeStorage Demo System is fully running!');
    console.log('👉 Open your browser at: http://localhost:4000');
    console.log('=============================================================\n');
}, 1500);

// Handle clean shutdown on Ctrl+C
process.on('SIGINT', () => {
    console.log('\nShutting down all processes...');
    processes.forEach(({ name, proc }) => {
        console.log(`Killing ${name}...`);
        proc.kill();
    });
    process.exit(0);
});
