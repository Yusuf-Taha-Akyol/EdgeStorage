const express = require('express');
const bodyParser = require('body-parser');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 3000;
const STORAGE_PATH = process.env.STORAGE_PATH || path.join(__dirname, '..', 'data');
const BACKEND_BIN = path.join(__dirname, '..', 'build', 'edgestorage_server_backend');

// Ensure storage directory exists
if (!fs.existsSync(STORAGE_PATH)) {
    fs.mkdirSync(STORAGE_PATH, { recursive: true });
}

const DEVICES_FILE = path.join(STORAGE_PATH, 'devices.json');

// Memory cache of devices: { name: { stream_id, schema } }
let devices = {};
if (fs.existsSync(DEVICES_FILE)) {
    try {
        devices = JSON.parse(fs.readFileSync(DEVICES_FILE, 'utf8'));
        console.log(`Loaded ${Object.keys(devices).length} devices from storage.`);
    } catch (e) {
        console.error('Error reading devices file:', e);
    }
}

// C Engine Enums
const ES_TYPES = {
    U8: 1, U16: 2, U32: 3, U64: 4, I32: 5, I64: 6, F32: 7, F64: 8, BOOL: 9
};

const ES_COMP = {
    NONE: 0, DELTA: 1, DELTA_DELTA: 2, XOR: 3, FOR: 4
};

function mapTypeToEnum(typeStr) {
    if (typeof typeStr === 'number') return typeStr;
    const key = String(typeStr).toUpperCase();
    if (ES_TYPES[key] !== undefined) return ES_TYPES[key];
    if (key === 'FLOAT' || key === 'FLOAT32' || key === 'F32') return ES_TYPES.F32;
    if (key === 'DOUBLE' || key === 'FLOAT64' || key === 'F64') return ES_TYPES.F64;
    if (key === 'INT' || key === 'INT32' || key === 'I32') return ES_TYPES.I32;
    if (key === 'BOOLEAN') return ES_TYPES.BOOL;
    throw new Error(`Unsupported field type: ${typeStr}`);
}

function mapCompToEnum(compStr) {
    if (typeof compStr === 'number') return compStr;
    if (!compStr) return ES_COMP.NONE;
    const key = String(compStr).toUpperCase();
    if (ES_COMP[key] !== undefined) return ES_COMP[key];
    return ES_COMP.NONE;
}

function getFieldSize(typeEnum) {
    switch (typeEnum) {
        case ES_TYPES.U8:
        case ES_TYPES.BOOL:
            return 1;
        case ES_TYPES.U16:
            return 2;
        case ES_TYPES.U32:
        case ES_TYPES.I32:
        case ES_TYPES.F32:
            return 4;
        case ES_TYPES.U64:
        case ES_TYPES.I64:
        case ES_TYPES.F64:
            return 8;
        default:
            return 4;
    }
}

function packField(buffer, offset, typeEnum, val) {
    switch (typeEnum) {
        case ES_TYPES.U8:
            buffer.writeUInt8(Number(val), offset);
            break;
        case ES_TYPES.U16:
            buffer.writeUInt16LE(Number(val), offset);
            break;
        case ES_TYPES.U32:
            buffer.writeUInt32LE(Number(val), offset);
            break;
        case ES_TYPES.U64:
            buffer.writeBigUInt64LE(BigInt(val), offset);
            break;
        case ES_TYPES.I32:
            buffer.writeInt32LE(Number(val), offset);
            break;
        case ES_TYPES.I64:
            buffer.writeBigInt64LE(BigInt(val), offset);
            break;
        case ES_TYPES.F32:
            buffer.writeFloatLE(Number(val), offset);
            break;
        case ES_TYPES.F64:
            buffer.writeDoubleLE(Number(val), offset);
            break;
        case ES_TYPES.BOOL:
            buffer.writeUInt8(val ? 1 : 0, offset);
            break;
        default:
            throw new Error(`Cannot pack unknown type enum: ${typeEnum}`);
    }
}

function unpackField(buffer, offset, typeEnum) {
    switch (typeEnum) {
        case ES_TYPES.U8:
            return buffer.readUInt8(offset);
        case ES_TYPES.U16:
            return buffer.readUInt16LE(offset);
        case ES_TYPES.U32:
            return buffer.readUInt32LE(offset);
        case ES_TYPES.U64:
            return buffer.readBigUInt64LE(offset).toString();
        case ES_TYPES.I32:
            return buffer.readInt32LE(offset);
        case ES_TYPES.I64:
            return buffer.readBigInt64LE(offset).toString();
        case ES_TYPES.F32:
            return buffer.readFloatLE(offset);
        case ES_TYPES.F64:
            return buffer.readDoubleLE(offset);
        case ES_TYPES.BOOL:
            return buffer.readUInt8(offset) !== 0;
        default:
            throw new Error(`Cannot unpack unknown type enum: ${typeEnum}`);
    }
}

function packPayload(fields, payloadObj) {
    let size = 0;
    const fieldsWithEnums = fields.map(f => {
        const typeEnum = mapTypeToEnum(f.type);
        const fSize = getFieldSize(typeEnum);
        size += fSize;
        return { ...f, typeEnum, size: fSize };
    });

    const buffer = Buffer.alloc(size);
    let offset = 0;
    for (const f of fieldsWithEnums) {
        const val = payloadObj[f.name];
        if (val === undefined) {
            throw new Error(`Payload missing field: ${f.name}`);
        }
        packField(buffer, offset, f.typeEnum, val);
        offset += f.size;
    }
    return buffer;
}

function unpackPayload(fields, buffer) {
    const payloadObj = {};
    let offset = 0;
    for (const f of fields) {
        const typeEnum = mapTypeToEnum(f.type);
        const fSize = getFieldSize(typeEnum);
        if (offset + fSize > buffer.length) {
            throw new Error(`Buffer overrun while unpacking field ${f.name}`);
        }
        payloadObj[f.name] = unpackField(buffer, offset, typeEnum);
        offset += fSize;
    }
    return payloadObj;
}

// Subprocess and Command Queue Management
let backend = null;
let queue = [];
let stdoutBuffer = '';

function startBackend() {
    console.log(`Starting C backend: ${BACKEND_BIN}`);
    backend = spawn(BACKEND_BIN);

    backend.stdout.on('data', (data) => {
        stdoutBuffer += data.toString();
        let lines = stdoutBuffer.split('\n');
        stdoutBuffer = lines.pop(); // Keep partial line

        for (let line of lines) {
            if (line.trim() === '') continue;
            handleBackendResponse(line);
        }
    });

    backend.stderr.on('data', (data) => {
        console.error(`[C Backend Error] ${data.toString().trim()}`);
    });

    backend.on('close', (code) => {
        console.log(`C backend exited with code ${code}`);
        // Reject all outstanding requests in the queue
        const currentQueue = queue;
        queue = [];
        for (const item of currentQueue) {
            item.reject(new Error('C backend crashed or exited unexpectedly'));
        }

        // Restart backend if not intentionally closed
        if (code !== 0) {
            console.log('Attempting to restart C backend in 1 second...');
            setTimeout(initializeSystem, 1000);
        }
    });
}

function handleBackendResponse(line) {
    if (queue.length === 0) {
        console.warn(`Unsolicited backend response: ${line}`);
        return;
    }

    const current = queue[0];
    current.responses.push(line);

    if (current.responses.length === 1) {
        if (current.cmdType === 'QUERY_RANGE') {
            const parts = line.split(' ');
            if (parts[0] === 'STATUS' && parts[1] === 'OK' && parts[2] !== undefined) {
                const count = parseInt(parts[2], 10);
                current.responseCount = 1 + count;
            }
        }
    }

    if (current.responses.length >= current.responseCount) {
        queue.shift();
        current.resolve(current.responses);
    }
}

function sendCommand(cmdStr) {
    return new Promise((resolve, reject) => {
        if (!backend) {
            return reject(new Error('C backend not running'));
        }
        const cmdType = cmdStr.trim().split(/\s+/)[0];
        queue.push({
            resolve,
            reject,
            responseCount: 1,
            responses: [],
            cmdType: cmdType
        });
        backend.stdin.write(cmdStr + '\n');
    });
}

async function initializeSystem() {
    startBackend();

    // 1. Open the engine
    const segmentSize = 10 * 1024 * 1024; // 10MB
    const writeBufSize = 64 * 1024; // 64KB
    const compressionEnabled = 1;

    try {
        console.log(`Opening EdgeStorage engine at ${STORAGE_PATH}...`);
        const responses = await sendCommand(`OPEN ${STORAGE_PATH} ${segmentSize} ${writeBufSize} ${compressionEnabled}`);
        if (!responses[0].startsWith('STATUS OK')) {
            throw new Error(`Failed to open engine: ${responses[0]}`);
        }
        console.log('EdgeStorage engine opened successfully.');

        // 2. Re-register all persistent devices
        const deviceNames = Object.keys(devices);
        if (deviceNames.length > 0) {
            console.log(`Re-registering ${deviceNames.length} devices in the C engine...`);
            for (const name of deviceNames) {
                const device = devices[name];
                const stream_id = await registerDeviceInEngine(device);
                device.stream_id = stream_id; // Update stream_id
                console.log(`Device '${name}' re-registered with stream_id: ${stream_id}`);
            }
            fs.writeFileSync(DEVICES_FILE, JSON.stringify(devices, null, 2), 'utf8');
        }
    } catch (e) {
        console.error('Failed to initialize EdgeStorage system:', e);
    }
}

async function registerDeviceInEngine(device) {
    let cmd = `REGISTER_STREAM ${device.name} ${device.schema_version} ${device.record_types.length}\n`;
    for (const rt of device.record_types) {
        let payloadSize = 0;
        for (const f of rt.fields) {
            payloadSize += getFieldSize(mapTypeToEnum(f.type));
        }
        cmd += `RECORD_TYPE ${rt.id} ${rt.name} ${rt.fields.length} ${payloadSize}\n`;
        for (const f of rt.fields) {
            const typeEnum = mapTypeToEnum(f.type);
            const compEnum = mapCompToEnum(f.compression);
            cmd += `FIELD ${f.id} ${f.name} ${typeEnum} ${compEnum}\n`;
        }
    }

    const responses = await sendCommand(cmd.trim());
    const firstLine = responses[0];
    const parts = firstLine.split(' ');
    if (parts[0] !== 'STATUS' || parts[1] !== 'OK') {
        throw new Error(`Failed to register device in engine: ${firstLine}`);
    }
    return parseInt(parts[2], 10);
}

// REST Endpoints

// 1. Device Creation (POST /devices)
app.post('/devices', async (req, res) => {
    try {
        const { name, schema_version, record_types } = req.body;
        if (!name || !schema_version || !record_types || !Array.isArray(record_types)) {
            return res.status(400).json({ error: 'Missing name, schema_version, or record_types (array)' });
        }

        if (devices[name]) {
            return res.status(409).json({ error: `Device '${name}' already exists` });
        }

        // Validate and map record types & fields
        for (const rt of record_types) {
            if (!rt.id || !rt.name || !rt.fields || !Array.isArray(rt.fields)) {
                return res.status(400).json({ error: 'Invalid record type definition' });
            }
            for (const f of rt.fields) {
                if (!f.id || !f.name || !f.type) {
                    return res.status(400).json({ error: 'Invalid field definition' });
                }
            }
        }

        const device = { name, schema_version, record_types };
        const stream_id = await registerDeviceInEngine(device);
        device.stream_id = stream_id;

        // Save to persistent storage
        devices[name] = device;
        fs.writeFileSync(DEVICES_FILE, JSON.stringify(devices, null, 2), 'utf8');

        res.status(201).json({
            message: `Device '${name}' created successfully.`,
            stream_id: stream_id,
            device: device
        });
    } catch (e) {
        console.error('Error creating device:', e);
        res.status(500).json({ error: e.message });
    }
});

// 2. Write Telemetry (POST /telemetry)
app.post('/telemetry', async (req, res) => {
    try {
        const { device_name, record_type_id, timestamp_ns, payload } = req.body;

        if (!device_name || !record_type_id || payload === undefined) {
            return res.status(400).json({ error: 'Missing device_name, record_type_id, or payload' });
        }

        const device = devices[device_name];
        if (!device) {
            return res.status(404).json({ error: `Device '${device_name}' not found` });
        }

        const recordType = device.record_types.find(rt => rt.id === record_type_id);
        if (!recordType) {
            return res.status(404).json({ error: `Record type ID ${record_type_id} not found on device '${device_name}'` });
        }

        // Pack payload to binary buffer based on schema fields
        let packedBuf = Buffer.alloc(0);
        if (recordType.fields.length > 0) {
            try {
                packedBuf = packPayload(recordType.fields, payload);
            } catch (err) {
                return res.status(400).json({ error: `Payload packing error: ${err.message}` });
            }
        }

        const ts = timestamp_ns || String(Date.now() * 1000000); // Default to current time in ns
        const flags = 0;
        const payloadHex = packedBuf.length > 0 ? packedBuf.toString('hex') : '00';
        const payloadSize = packedBuf.length;

        const cmd = `WRITE_RECORD ${device.stream_id} ${ts} ${record_type_id} ${flags} ${payloadSize} ${payloadHex}`;
        const responses = await sendCommand(cmd);

        if (!responses[0].startsWith('STATUS OK')) {
            return res.status(500).json({ error: `Failed to write telemetry: ${responses[0]}` });
        }

        res.status(200).json({ message: 'Telemetry written successfully.' });
    } catch (e) {
        console.error('Error writing telemetry:', e);
        res.status(500).json({ error: e.message });
    }
});

// 3. Query Telemetry (GET /telemetry)
app.get('/telemetry', async (req, res) => {
    try {
        const { device_name, start_ts_ns, end_ts_ns, record_type_id, limit } = req.query;

        if (!device_name) {
            return res.status(400).json({ error: 'Missing device_name parameter' });
        }

        const device = devices[device_name];
        if (!device) {
            return res.status(404).json({ error: `Device '${device_name}' not found` });
        }

        const start = start_ts_ns || '0';
        const end = end_ts_ns || '18446744073709551615'; // Max uint64
        const recType = record_type_id || '0'; // 0 means all types
        const lim = limit || '100';

        const cmd = `QUERY_RANGE ${device.stream_id} ${start} ${end} ${recType} ${lim}`;
        const responses = await sendCommand(cmd);

        // Parse responses
        // First line: "STATUS OK <count>"
        const firstLine = responses[0];
        if (!firstLine.startsWith('STATUS OK')) {
            return res.status(500).json({ error: `Failed to query: ${firstLine}` });
        }

        const records = [];
        for (let i = 1; i < responses.length; i++) {
            const parts = responses[i].split(' ');
            if (parts[0] !== 'RECORD') continue;

            const ts = parts[1];
            const rtId = parseInt(parts[2], 10);
            const flags = parseInt(parts[3], 10);
            const pSize = parseInt(parts[4], 10);
            const pHex = parts[5];

            // Decode payload using schema fields
            let payloadDecoded = null;
            const recordType = device.record_types.find(rt => rt.id === rtId);
            if (recordType && pSize > 0 && pHex) {
                try {
                    const buf = Buffer.from(pHex, 'hex');
                    payloadDecoded = unpackPayload(recordType.fields, buf);
                } catch (err) {
                    payloadDecoded = { error: `Decoding error: ${err.message}`, raw_hex: pHex };
                }
            }

            records.push({
                timestamp_ns: ts,
                record_type_id: rtId,
                flags: flags,
                payload: payloadDecoded
            });
        }

        res.status(200).json(records);
    } catch (e) {
        console.error('Error querying telemetry:', e);
        res.status(500).json({ error: e.message });
    }
});

// 4. Delete Telemetry & Devices (DELETE /telemetry)
app.delete('/telemetry', async (req, res) => {
    try {
        const { device_name } = req.query;

        console.log('Closing C engine for wipe...');
        await sendCommand('CLOSE');
        backend.removeAllListeners();
        backend.kill();

        if (device_name) {
            const device = devices[device_name];
            if (!device) {
                return res.status(404).json({ error: `Device '${device_name}' not found` });
            }

            // Remove stream directory
            const streamDir = path.join(STORAGE_PATH, `stream_${device.stream_id}`);
            if (fs.existsSync(streamDir)) {
                fs.rmSync(streamDir, { recursive: true, force: true });
            }

            delete devices[device_name];
            fs.writeFileSync(DEVICES_FILE, JSON.stringify(devices, null, 2), 'utf8');

            console.log(`Device '${device_name}' telemetry files deleted.`);
        } else {
            // Delete everything in storage directory except devices.json (optionally, let's keep devices list but clear all directories)
            const files = fs.readdirSync(STORAGE_PATH);
            for (const file of files) {
                if (file !== 'devices.json') {
                    const fullPath = path.join(STORAGE_PATH, file);
                    fs.rmSync(fullPath, { recursive: true, force: true });
                }
            }
            // Clear devices memory map and save empty devices
            devices = {};
            fs.writeFileSync(DEVICES_FILE, JSON.stringify(devices, null, 2), 'utf8');
            console.log('All telemetry data and device definitions deleted.');
        }

        // Restart C backend
        console.log('Re-initializing system after wipe...');
        await initializeSystem();

        res.status(200).json({ message: 'Telemetry data deleted successfully.' });
    } catch (e) {
        console.error('Error deleting telemetry:', e);
        res.status(500).json({ error: e.message });
    }
});

// 5. Health Check (GET /health)
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'healthy',
        engine_state: backend ? 'running' : 'stopped',
        storage_path: STORAGE_PATH,
        device_count: Object.keys(devices).length,
        devices: Object.keys(devices).map(name => ({
            name,
            stream_id: devices[name].stream_id,
            record_types: devices[name].record_types.map(rt => rt.name)
        }))
    });
});

// Start initialization and listen
app.listen(PORT, async () => {
    console.log(`EdgeStorage HTTP server listening on port ${PORT}`);
    await initializeSystem();
});
