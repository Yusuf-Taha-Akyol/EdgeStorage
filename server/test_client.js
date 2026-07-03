const http = require('http');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const PORT = 3000;
const BASE_URL = `http://localhost:${PORT}`;

// Helper function to make HTTP requests
function request(method, urlPath, body = null) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'localhost',
            port: PORT,
            path: urlPath,
            method: method,
            headers: {
                'Content-Type': 'application/json'
            }
        };

        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                let parsed = data;
                if (res.headers['content-type'] && res.headers['content-type'].includes('application/json')) {
                    try {
                        parsed = JSON.parse(data);
                    } catch (e) {}
                }
                resolve({
                    status: res.statusCode,
                    headers: res.headers,
                    body: parsed
                });
            });
        });

        req.on('error', (err) => reject(err));

        if (body) {
            req.write(JSON.stringify(body));
        }
        req.end();
    });
}

// Sleep utility
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTests() {
    console.log('--- STARTING EDGE STORAGE SERVER INTEGRATION TESTS ---');

    // 1. Start Server
    console.log('Spawning Node.js server...');
    const serverProcess = spawn('node', ['server.js'], {
        cwd: __dirname,
        env: { ...process.env, PORT: String(PORT) }
    });

    serverProcess.stdout.on('data', (data) => {
        console.log(`[Server] ${data.toString().trim()}`);
    });

    serverProcess.stderr.on('data', (data) => {
        console.error(`[Server Error] ${data.toString().trim()}`);
    });

    // Wait for server to start
    await sleep(2000);

    try {
        // Clear all previous data first
        console.log('\nWiping database to start fresh...');
        const wipeRes = await request('DELETE', '/telemetry');
        console.log('Wipe response:', wipeRes.status, wipeRes.body);

        // 2. Health check
        console.log('\nChecking server health...');
        const health1 = await request('GET', '/health');
        console.log('Health:', health1.status, health1.body);
        if (health1.status !== 200 || health1.body.device_count !== 0) {
            throw new Error('Initial health check failed');
        }

        // 3. Create a device (thermostat)
        console.log('\nCreating device: thermostat...');
        const deviceSchema = {
            name: 'thermostat',
            schema_version: 1,
            record_types: [
                {
                    id: 1,
                    name: 'temperature_humidity',
                    fields: [
                        { id: 1, name: 'temp', type: 'f32', compression: 'none' },
                        { id: 2, name: 'humidity', type: 'f32', compression: 'none' },
                        { id: 3, name: 'active', type: 'bool', compression: 'none' }
                    ]
                }
            ]
        };

        const createRes = await request('POST', '/devices', deviceSchema);
        console.log('Create device response:', createRes.status, createRes.body);
        if (createRes.status !== 201 || createRes.body.stream_id !== 1) {
            throw new Error('Device creation failed');
        }

        // 4. Write telemetry data
        console.log('\nWriting telemetry data points...');
        const now = Date.now() * 1000000; // ns

        const p1 = {
            device_name: 'thermostat',
            record_type_id: 1,
            timestamp_ns: String(now),
            payload: { temp: 22.5, humidity: 45.2, active: true }
        };

        const p2 = {
            device_name: 'thermostat',
            record_type_id: 1,
            timestamp_ns: String(now + 100000000), // +100ms
            payload: { temp: 23.0, humidity: 44.8, active: true }
        };

        const p3 = {
            device_name: 'thermostat',
            record_type_id: 1,
            timestamp_ns: String(now + 200000000), // +200ms
            payload: { temp: 22.8, humidity: 45.0, active: false }
        };

        const write1 = await request('POST', '/telemetry', p1);
        console.log('Write 1:', write1.status, write1.body);
        const write2 = await request('POST', '/telemetry', p2);
        console.log('Write 2:', write2.status, write2.body);
        const write3 = await request('POST', '/telemetry', p3);
        console.log('Write 3:', write3.status, write3.body);

        if (write1.status !== 200 || write2.status !== 200 || write3.status !== 200) {
            throw new Error('Telemetry write failed');
        }

        // 5. Query telemetry
        console.log('\nQuerying telemetry data...');
        const queryRes = await request('GET', `/telemetry?device_name=thermostat&start_ts_ns=${now}&end_ts_ns=${now + 300000000}`);
        console.log('Query response status:', queryRes.status);
        console.log('Query results count:', queryRes.body.length);
        console.log('Query results:', JSON.stringify(queryRes.body, null, 2));

        if (queryRes.status !== 200 || queryRes.body.length !== 3) {
            throw new Error('Telemetry query did not return 3 records');
        }

        // Validate values
        const r1 = queryRes.body[0];
        if (Math.abs(r1.payload.temp - 22.5) > 0.001 || r1.payload.active !== true) {
            throw new Error('Record 1 values mismatch');
        }
        const r3 = queryRes.body[2];
        if (Math.abs(r3.payload.humidity - 45.0) > 0.001 || r3.payload.active !== false) {
            throw new Error('Record 3 values mismatch');
        }

        console.log('\nSUCCESS: Write & read verified successfully!');

        // 6. Test Persistence reload
        console.log('\nTesting persistence reload. Stopping server process...');
        serverProcess.kill('SIGINT');
        await sleep(2000);

        console.log('Restarting server process...');
        const serverProcess2 = spawn('node', ['server.js'], {
            cwd: __dirname,
            env: { ...process.env, PORT: String(PORT) }
        });

        serverProcess2.stdout.on('data', (data) => {
            console.log(`[Server V2] ${data.toString().trim()}`);
        });

        await sleep(2000);

        console.log('Querying thermostat telemetry from restarted server...');
        const queryRes2 = await request('GET', `/telemetry?device_name=thermostat&start_ts_ns=${now}&end_ts_ns=${now + 300000000}`);
        console.log('Query V2 results count:', queryRes2.body.length);
        
        if (queryRes2.status !== 200 || queryRes2.body.length !== 3) {
            serverProcess2.kill();
            throw new Error('Persistence reload failed: query did not return records after restart');
        }
        console.log('SUCCESS: Persistence reload verified successfully!');

        // 7. Cleanup & Stop
        console.log('\nStopping server process...');
        serverProcess2.kill();
        await sleep(1000);
        console.log('--- ALL INTEGRATION TESTS PASSED SUCCESSFULLY! ---');
        process.exit(0);

    } catch (err) {
        console.error('\n--- TEST RUN FAILED ---');
        console.error(err);
        serverProcess.kill();
        process.exit(1);
    }
}

runTests();
