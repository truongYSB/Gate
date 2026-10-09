const mqtt = require('mqtt');
const { SerialPort } = require('serialport');
const express = require("express");

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const BROKER_URL = 'ws://localhost:9001';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM8';

//Lưu đệm trạng thái M để không phải xuống PLC đọc lại mỗi lần nhấn nút
let cachedMState = 0;
const TARGET_D_REGISTERS = [100, 102];
const port = new SerialPort({
    path: PLC_PORT_NAME,
    baudRate: 9600,
    dataBits: 7,
    parity: 'even',
    stopBits: 1,
    autoOpen: false
});

const app = express();
let responseHandler = null;
let timeoutHandle = null;
let rxBuffer = Buffer.alloc(0);
let heartbeatTimer = null;
const HEARTBEAT_TIMEOUT = 5000;

port.on('open', () => {
    console.log(`✅ Đã mở và chiếm dụng cổng ${PLC_PORT_NAME}`);
    mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'GATEWAY_READY');
});

port.on('close', () => console.log(`🔒 Đã nhả cổng ${PLC_PORT_NAME}`));
port.on('error', (err) => console.log(`❌ Lỗi cổng COM:`, err.message));

function processBuffer() {
    if (rxBuffer.length === 0) return;

    let validIndex = -1;
    for (let i = 0; i < rxBuffer.length; i++) {
        if (rxBuffer[i] === 0x02 || rxBuffer[i] === 0x06 || rxBuffer[i] === 0x15) {
            validIndex = i; break;
        }
    }
    if (validIndex > 0) {
        rxBuffer = rxBuffer.slice(validIndex);
    } else if (validIndex === -1) {
        rxBuffer = Buffer.alloc(0); return;
    }

    let currentHandler = responseHandler;
    responseHandler = null;
    if (timeoutHandle) {
        clearTimeout(timeoutHandle);
        timeoutHandle = null;
    }

    let frameData = Buffer.from(rxBuffer);
    rxBuffer = Buffer.alloc(0);

    if (currentHandler) {
        currentHandler(frameData);
    }
}

port.on('data', (data) => {
    rxBuffer = Buffer.concat([rxBuffer, data]);
    let isComplete = false;

    if (rxBuffer.includes(0x06) || rxBuffer.includes(0x15)) {
        isComplete = true;
    } else if (rxBuffer.includes(0x02) && rxBuffer.includes(0x03)) {
        let etxIndex = rxBuffer.indexOf(0x03);
        if (rxBuffer.length >= etxIndex + 3) isComplete = true;
    }

    if (isComplete) processBuffer();
});

function sendFrame(frame) {
    return new Promise((resolve) => {
        if (timeoutHandle) clearTimeout(timeoutHandle);
        responseHandler = null;
        let isResolved = false;

        const safeResolve = (result) => {
            if (!isResolved) {
                isResolved = true;
                if (timeoutHandle) { clearTimeout(timeoutHandle); timeoutHandle = null; }
                responseHandler = null;
                resolve(result);
            }
        };

        responseHandler = (data) => { safeResolve(data); };

        timeoutHandle = setTimeout(() => {
            if (!isResolved) {
                rxBuffer = Buffer.alloc(0);
                safeResolve(null);
            }
        }, 1500);

        port.write(frame, (err) => {
            if (err) { safeResolve(null); }
        });
    });
}

function calculateChecksum(payloadStr) {
    let sum = 0;
    for (let i = 0; i < payloadStr.length; i++) sum += payloadStr.charCodeAt(i);
    return sum.toString(16).slice(-2).toUpperCase();
}

async function executeWithRetry(frame, validateFunc, retryName) {
    let maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        let res = await sendFrame(frame);
        if (validateFunc(res)) return res;
        if (attempt < maxRetries) await delay(300);
    }
    return null;
}

function buildReadFrame(addressHex, countHex) {
    let payload = '0' + addressHex + countHex + '\x03';
    let checksum = calculateChecksum(payload);
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

function buildWriteByteFrame(addressHex, countHex, valueInt) {
    let hexData = valueInt.toString(16).padStart(2, '0').toUpperCase();
    let payload = '1' + addressHex + countHex + hexData + '\x03';
    let checksum = calculateChecksum(payload);
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

function validateResponseChecksum(resBuffer) {
    if (!resBuffer || resBuffer.length < 3) return false;
    let etxIndex = resBuffer.indexOf(0x03);
    if (etxIndex === -1 || resBuffer.length < etxIndex + 3) return false;
    let payloadToCheck = resBuffer.toString('ascii', 1, etxIndex + 1);
    let receivedChecksum = resBuffer.toString('ascii', etxIndex + 1, etxIndex + 3);
    return receivedChecksum === calculateChecksum(payloadToCheck);
}

// ==========================================
// TÍNH NĂNG MỚI 1: BỘ TÍNH TOÁN ĐỊA CHỈ THANH GHI ĐẶC BIỆT
// ==========================================
function getDAddress(dNum) {
    // D8000+ có Base Address là 0x0E00 trong MC Protocol
    if (dNum >= 8000) return 0x0E00 + (dNum - 8000) * 2;
    return 0x1000 + (dNum * 2);
}

function getMByteAddress(mNum) {
    // M8000+ có Base Address là 0x01E0 trong MC Protocol
    if (mNum >= 8000) return 0x01E0 + Math.floor((mNum - 8000) / 8);
    return 0x0100 + Math.floor(mNum / 8);
}

// ==========================================
// TÍNH NĂNG MỚI 2: HÀM ĐỌC 32-BIT & ĐỌC M ĐẶC BIỆT
// ==========================================
async function readDRegister32(dNumber) {
    let address = getDAddress(dNumber);
    let addressHex = address.toString(16).toUpperCase().padStart(4, '0');
    // Đọc 4 bytes (tương đương 2 thanh ghi 16-bit liền kề)
    let frame = buildReadFrame(addressHex, '04');

    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, `ĐỌC 32-BIT D${dNumber}`);

    if (res) {
        let hexStr = res.toString('ascii', 1, 9); 
        
        // Tách dữ liệu Low Word (D8360)
        let d1Low = hexStr.substring(0, 2), d1High = hexStr.substring(2, 4);
        let valLowWord = parseInt(d1High + d1Low, 16);
        
        // Tách dữ liệu High Word (D8361)
        let d2Low = hexStr.substring(4, 6), d2High = hexStr.substring(6, 8);
        let valHighWord = parseInt(d2High + d2Low, 16);

        // Gộp thành số nguyên 32-bit có dấu (Sử dụng phép dịch bit)
        let value32 = (valHighWord << 16) | valLowWord;
        return value32;
    }
    return null;
}

async function readSpecialMBit(mNumber) {
    let address = getMByteAddress(mNumber);
    let addressHex = address.toString(16).toUpperCase().padStart(4, '0');
    let frame = buildReadFrame(addressHex, '01');

    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, `ĐỌC M${mNumber}`);

    if (res) {
        let hexStr = res.toString('ascii', 1, 3);
        let byteVal = parseInt(hexStr, 16);
        let bitIndex = mNumber % 8;
        return (byteVal & (1 << bitIndex)) !== 0 ? 1 : 0;
    }
    return null;
}

async function writeMByte(valueInt) {
    let frame = buildWriteByteFrame('0100', '01', valueInt);
    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x06);
    }, "GHI LẠI BATCH");
    return res !== null;
}

async function readYState() {
    let frame = buildReadFrame('00A0', '01');
    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, "ĐỌC LẠI Y");
    return res ? parseInt(res.toString('ascii', 1, 3), 16) : -1;
}

async function readMState() {
    let frame = buildReadFrame('0100', '01');
    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, "ĐỌC LẠI M");
    return res ? parseInt(res.toString('ascii', 1, 3), 16) : -1;
}

async function readDRegister(dNumber) {
    let address = getDAddress(dNumber);
    let addressHex = address.toString(16).toUpperCase().padStart(4, '0');
    let frame = buildReadFrame(addressHex, '02');

    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, `ĐỌC THANH GHI D${dNumber}`);

    if (res) {
        let hexStr = res.toString('ascii', 1, 5);
        let lowByte = hexStr.substring(0, 2);
        let highByte = hexStr.substring(2, 4);
        let value = parseInt(highByte + lowByte, 16);
        if (value >= 0x8000) value = value - 0x10000;
        return value;
    }
    return null;
}

// ==========================================
// HỆ THỐNG QUEUE & MQTT
// ==========================================
let actionQueue = [];
let isProcessing = false;

async function processActionQueue() {
    if (isProcessing) return;
    isProcessing = true;

    while (actionQueue.length > 0) {
        let action = actionQueue.shift();
        try {
            await action();
        } catch (e) {
            console.error("❌ Lỗi ngoại lệ trong Action Queue:", e);
        }
    }
    isProcessing = false;
}

mqttClient.on('connect', () => {
    console.log("✅ Gateway đã kết nối với MQTT Broker!");
    mqttClient.subscribe('iot/lab602/dieu_khien_plc/control');
});

mqttClient.on('message', (topic, message) => {
    let msg = message.toString().replace(/\0/g, '').trim();

    if (msg === "HEARTBEAT") {
        clearTimeout(heartbeatTimer);
        heartbeatTimer = setTimeout(() => {
            if (port.isOpen) {
                console.log("🥀 Mất kết nối WebGL. Đang đóng cổng COM...");
                port.close();
            }
        }, HEARTBEAT_TIMEOUT);

        if (isProcessing) return;
        if (!port.isOpen) port.open((err) => { if (err) console.error("❌ Lỗi mở cổng COM:", err.message); });
        return;
    }

    if (msg === "CLEAR_QUEUE") {
        actionQueue = [];
        return;
    }

    if (msg === "SYNC_STATE") {
        actionQueue.push(async () => {
            if (!port.isOpen) return;
            let mVal = await readMState();
            await delay(30);
            let yVal = await readYState();
            await delay(30);

            let dValues = [];
            for (let dNum of TARGET_D_REGISTERS) {
                let dVal = await readDRegister(dNum);
                if (dVal !== null) dValues.push(`D${dNum}:${dVal}`);
                await delay(30); 
            }

            if (mVal !== -1 && yVal !== -1) {
                cachedMState = mVal;
                let payload = [];
                for (let i = 0; i <= 7; i++) payload.push(`M${i}_${(mVal & (1 << i)) ? 'ON' : 'OFF'}`);
                for (let i = 0; i <= 7; i++) payload.push(`Y${i}_${(yVal & (1 << i)) ? 'ON' : 'OFF'}`);
                if (dValues.length > 0) payload.push(...dValues);
                payload.push('SYNC_DONE');

                mqttClient.publish('iot/lab602/dieu_khien_plc/status', payload.join(','));
            }
        });
        processActionQueue();
        return;
    }

    if (msg.includes('_ON') || msg.includes('_OFF')) {
        actionQueue.push(async () => {
            if (!port.isOpen) return;
            let commands = msg.split(',');
            let payload = [];
            let hasError = false;
            let newMVal = cachedMState;

            for (let cmd of commands) {
                let cleanCmd = cmd.trim();
                if (!cleanCmd) continue;
                let [mName, state] = cleanCmd.split('_');
                let mNumber = parseInt(mName.replace('M', ''));
                if (mNumber >= 0 && mNumber <= 7) {
                    if (state === "ON") newMVal |= (1 << mNumber);
                    else newMVal &= ~(1 << mNumber);
                }
            }

            if (newMVal !== cachedMState) {
                let success = await writeMByte(newMVal);
                if (!success) hasError = true;
                else cachedMState = newMVal;
                await delay(30);
            }

            let yVal = await readYState();
            if (yVal === -1 || hasError) {
                payload.push('ACTION_FAILED');
            } else {
                for (let cmd of commands) {
                    let cleanCmd = cmd.trim();
                    if (!cleanCmd) continue;
                    let [mName, _] = cleanCmd.split('_');
                    let mNumber = parseInt(mName.replace('M', ''));
                    if (mNumber >= 0 && mNumber <= 7) {
                        let isON = (cachedMState & (1 << mNumber)) !== 0;
                        payload.push(`M${mNumber}_${isON ? 'ON' : 'OFF'}`);
                    }
                }
                for (let i = 0; i <= 7; i++) {
                    let isON = (yVal & (1 << i)) !== 0;
                    payload.push(`Y${i}_${isON ? 'ON' : 'OFF'}`);
                }
                payload.push('ACTION_DONE');
            }
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', payload.join(','));
        });
        processActionQueue();
    }
});

// ==========================================
// TÍNH NĂNG MỚI 3: TELEMETRY LOOP (VÒNG LẶP ĐO LƯỜNG TỰ ĐỘNG)
// ==========================================
setInterval(() => {
    // Chỉ đẩy lệnh đo lường vào hàng đợi khi cổng COM đã mở và hàng đợi không bị nghẽn
    if (!port.isOpen || actionQueue.length > 3) return; 

    actionQueue.push(async () => {
        // Đọc tổng số xung 32-bit từ D8360 và D8361
        let d8360Val = await readDRegister32(8360);
        await delay(20);
        
        // Đọc cờ trạng thái bận từ M8360
        let m8360Val = await readSpecialMBit(8360);
        
        // Bắn dữ liệu telemetry về Unity qua MQTT
        if (d8360Val !== null && m8360Val !== null) {
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', `D8360:${d8360Val},M8360:${m8360Val}`);
        }
    });
    
    // Kích hoạt xử lý hàng đợi
    processActionQueue();
}, 250); // Tần số đo lường: 250ms/lần (Tránh spam nghẽn cáp Serial)

app.get("/health", (req, res) => {
    res.json({ service: "PLC Gateway", status: "running", mqtt: mqttClient.connected, com: port.isOpen });
});

app.listen(5003, () => { console.log("🚀 Gateway HTTP API đang chạy ở cổng 5003"); });

function gracefulShutdown() {
    console.log("\n🛑 Đang tắt Gateway và giải phóng tài nguyên...");
    if (port.isOpen) {
        port.close(() => { console.log("✅ Đã đóng cổng COM an toàn."); process.exit(0); });
    } else {
        process.exit(0);
    }
}
process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);