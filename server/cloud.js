const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');

const app = express();
const PORT = 4000;
const CLOUD_DIR = path.join(__dirname, '..', 'data', 'cloud');
const TELEMETRY_FILE = path.join(CLOUD_DIR, 'cloud_telemetry.json');

// Ensure directory exists
if (!fs.existsSync(CLOUD_DIR)) {
    fs.mkdirSync(CLOUD_DIR, { recursive: true });
}

// Credentials Registry
const DEVICES_REGISTRY = {
    'drone-01': { apiKey: 'api-key-drone-01', name: 'Drone-01' },
    'drone-02': { apiKey: 'api-key-drone-02', name: 'Drone-02' },
    'drone-03': { apiKey: 'api-key-drone-03', name: 'Drone-03' }
};

// Memory store
let cloudTelemetry = [];
if (fs.existsSync(TELEMETRY_FILE)) {
    try {
        cloudTelemetry = JSON.parse(fs.readFileSync(TELEMETRY_FILE, 'utf8'));
        console.log(`Loaded ${cloudTelemetry.length} telemetry records from cloud storage.`);
    } catch (e) {
        console.error('Error loading telemetry database:', e);
    }
}

// Device Status map (updated on heartbeat / sync)
let deviceStatus = {
    'drone-01': { name: 'Drone-01', lastSync: null, syncCount: 0 },
    'drone-02': { name: 'Drone-02', lastSync: null, syncCount: 0 },
    'drone-03': { name: 'Drone-03', lastSync: null, syncCount: 0 }
};

// Initialize counters based on loaded data
for (const rec of cloudTelemetry) {
    if (deviceStatus[rec.device_id]) {
        deviceStatus[rec.device_id].syncCount++;
        // Keep track of the most recent sync time
        const date = new Date(Number(BigInt(rec.timestamp_ns) / 1000000n));
        deviceStatus[rec.device_id].lastSync = date.toLocaleTimeString();
    }
}

// Static files middleware
app.use(express.static(path.join(__dirname, 'public')));

// Auth check helper
function getDevice(req) {
    const deviceId = req.headers['x-device-id'];
    const apiKey = req.headers['x-api-key'];
    if (!deviceId || !apiKey) return null;
    const registered = DEVICES_REGISTRY[deviceId];
    if (registered && registered.apiKey === apiKey) {
        return { id: deviceId, ...registered };
    }
    return null;
}

// 1. Ingest Batch Sync Telemetry (POST /api/cloud/sync)
app.post('/api/cloud/sync', (req, res) => {
    const device = getDevice(req);
    if (!device) {
        return res.status(401).json({ error: 'Unauthorized device credentials' });
    }

    let data = [];
    req.on('data', chunk => data.push(chunk));
    req.on('end', () => {
        let payloadBuf = Buffer.concat(data);
        if (req.headers['content-encoding'] === 'gzip') {
            try {
                payloadBuf = zlib.gunzipSync(payloadBuf);
            } catch (e) {
                console.error(`[Cloud] Decompression failed for ${device.id}:`, e.message);
                return res.status(400).json({ error: 'Gzip decompression failed' });
            }
        }

        let payload;
        try {
            payload = JSON.parse(payloadBuf.toString('utf8'));
        } catch (e) {
            console.error(`[Cloud] JSON parse failed for ${device.id}:`, e.message);
            return res.status(400).json({ error: 'Invalid JSON payload' });
        }

        const { records } = payload;
        if (!records || !Array.isArray(records) || records.length === 0) {
            return res.status(400).json({ error: 'Missing or empty records list' });
        }

        console.log(`[Cloud] Received ${records.length} records from device '${device.id}'`);

        // Add device metadata to records and append
        const processedRecords = records.map(rec => ({
            device_id: device.id,
            ...rec
        }));

        cloudTelemetry.push(...processedRecords);

        // Save database file
        try {
            fs.writeFileSync(TELEMETRY_FILE, JSON.stringify(cloudTelemetry, null, 2), 'utf8');
        } catch (e) {
            console.error('[Cloud] Error saving telemetry database:', e);
        }

        // Update status
        deviceStatus[device.id].lastSync = new Date().toLocaleTimeString();
        deviceStatus[device.id].syncCount += records.length;

        // ACK with the timestamp of the last record in the batch
        const lastRecord = records[records.length - 1];
        res.json({
            status: 'success',
            last_record_ts: lastRecord.timestamp_ns
        });
    });
});

// JSON parser for other endpoints
app.use(bodyParser.json());

// 2. Health Ping (GET /api/cloud/ping)
app.get('/api/cloud/ping', (req, res) => {
    res.status(200).json({ status: 'ok' });
});

// 3. Get Device Sync Registry Status (GET /api/cloud/devices)
app.get('/api/cloud/devices', (req, res) => {
    res.json(deviceStatus);
});

// 4. Get Synced Telemetry Data (GET /api/cloud/telemetry)
app.get('/api/cloud/telemetry', (req, res) => {
    const limit = parseInt(req.query.limit, 10) || 100;
    const sorted = [...cloudTelemetry].sort((a, b) => {
        const diff = BigInt(b.timestamp_ns) - BigInt(a.timestamp_ns);
        return diff > 0n ? 1 : (diff < 0n ? -1 : 0);
    });
    res.json(sorted.slice(0, limit));
});

// 5. Clear Database (DELETE /api/cloud/telemetry)
app.delete('/api/cloud/telemetry', (req, res) => {
    cloudTelemetry = [];
    try {
        fs.writeFileSync(TELEMETRY_FILE, JSON.stringify(cloudTelemetry, null, 2), 'utf8');
        console.log('[Cloud] Synced database cleared.');
    } catch (e) {
        console.error('[Cloud] Error clearing telemetry file:', e);
    }

    // Reset status counters
    for (const id in deviceStatus) {
        deviceStatus[id].lastSync = null;
        deviceStatus[id].syncCount = 0;
    }

    res.json({ success: true, message: 'Cloud database cleared successfully.' });
});

// Start Cloud Server
app.listen(PORT, () => {
    console.log(`Cloud central server listening on port ${PORT}`);
});
