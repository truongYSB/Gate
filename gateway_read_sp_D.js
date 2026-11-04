const mqtt = require('mqtt');
const { SerialPort } = require('serialport');
const express = require("express");

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const BROKER_URL = 'ws://localhost:9001';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM4';

let cachedMState = 0;
let cachedM100State = false;
const TARGET_D_REGISTERS = [100, 102];

// [SỬA ĐỔI]: autoOpen: true để COM luôn mở ngay khi bật Server
const port = new SerialPort({
    path: PLC_PORT_NAME,
    baudRate: 9600,
    dataBits: 7,
    parity: 'even',
    stopBits: 1,
    autoOpen: true
});

const app = express();
let responseHandler = null;
let timeoutHandle = null;
let rxBuffer = Buffer.alloc(0);

// [XÓA]: Đã xóa toàn bộ biến heartbeatTimer và HEARTBEAT_TIMEOUT

port.on('open', () => {
    console.log(`✅ Đã mở và chiếm dụng vĩnh viễn cổng ${PLC_PORT_NAME}`);
    mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'GATEWAY_READY');
});

port.on('close', () => console.log(`🔒 Cổng ${PLC_PORT_NAME} bị đóng (Ngoại lệ)`));
port.on('error', (err) => console.error(`❌ Lỗi cổng COM:`, err.message));

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
                console.log("⚠️ PLC Timeout không phản hồi (Giải cứu hàng đợi)!");
                rxBuffer = Buffer.alloc(0);
                safeResolve(null);
            }
        }, 1500);

        port.write(frame, (err) => {
            if (err) { console.error("❌ Lỗi port.write:", err.message); safeResolve(null); }
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

        if (attempt < maxRetries) {
            console.log(`♻️ Đang xả cáp và thử ${retryName} (Lần ${attempt})...`);
            await delay(300);
        }
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

// [THÊM MỚI]: Hàm ghi Word để cài đặt Motor
async function writeDRegister(dNumber, value) {
    let address = 0x1000 + (dNumber * 2);
    let addressHex = address.toString(16).toUpperCase().padStart(4, '0');

    let hexValue = value.toString(16).toUpperCase().padStart(4, '0');
    let highByte = hexValue.substring(0, 2);
    let lowByte = hexValue.substring(2, 4);
    let payloadData = lowByte + highByte;

    let payload = '1' + addressHex + '02' + payloadData + '\x03';
    let checksum = calculateChecksum(payload);
    let frame = Buffer.from('\x02' + payload + checksum, 'ascii');

    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x06);
    }, `GHI D${dNumber}`);
    return res !== null;
}

async function writeMByte(valueInt) {
    let frame = buildWriteByteFrame('0100', '01', valueInt);
    let res = await executeWithRetry(frame, (data) => {
        if (data && data[0] === 0x15) console.log(`⚠️ PLC NAK (0x15) - Lệnh ghi bị từ chối!`);
        return (data && data[0] === 0x06);
    }, "GHI M BATCH");
    return res !== null;
}

function validateResponseChecksum(resBuffer) {
    if (!resBuffer || resBuffer.length < 3) return false;
    let etxIndex = resBuffer.indexOf(0x03);
    if (etxIndex === -1 || resBuffer.length < etxIndex + 3) return false;

    let payloadToCheck = resBuffer.toString('ascii', 1, etxIndex + 1);
    let receivedChecksum = resBuffer.toString('ascii', etxIndex + 1, etxIndex + 3);
    return receivedChecksum === calculateChecksum(payloadToCheck);
}

async function readYState() {
    let frame = buildReadFrame('00A0', '01');
    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, "ĐỌC Y");
    return res ? parseInt(res.toString('ascii', 1, 3), 16) : -1;
}

async function readMState() {
    let frame = buildReadFrame('0100', '01');
    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, "ĐỌC M");
    return res ? parseInt(res.toString('ascii', 1, 3), 16) : -1;
}

// HÀM MỚI: Đọc riêng biệt trạng thái của M100
async function readM100State() {
    let frame = buildReadFrame('010C', '01');

    console.log('[M100] TX:', frame.toString('hex').toUpperCase());

    let res = await executeWithRetry(frame, (data) => {
        console.log(
            '[M100] RX:',
            data ? data.toString('hex').toUpperCase() : 'NULL'
        );

        return (
            data &&
            data[0] === 0x02 &&
            validateResponseChecksum(data)
        );
    }, "ĐỌC M100");

    if (res) {
        let byteVal = parseInt(res.toString('ascii', 1, 3), 16);
        console.log('[M100] VALUE:', byteVal);

        return (byteVal & (1 << 4)) !== 0;
    }

    return null;
}

async function readDRegister(dNumber) {
    let address = 0x1000 + (dNumber * 2);
    let addressHex = address.toString(16).toUpperCase().padStart(4, '0');
    let frame = buildReadFrame(addressHex, '02');

    let res = await executeWithRetry(frame, (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, `ĐỌC D${dNumber}`);

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
        try { await action(); } catch (e) { console.error("❌ Lỗi Queue:", e); }
    }
    isProcessing = false;
}

mqttClient.on('connect', () => {
    console.log("✅ Gateway đã kết nối với MQTT Broker!");
    mqttClient.subscribe('iot/lab602/dieu_khien_plc/control');
});

mqttClient.on('message', (topic, message) => {
    let msg = message.toString().replace(/\0/g, '').trim();
    if (msg !== "SYNC_STATE") console.log(`\n📥 [MQTT IN]: ${msg}`);

    // [THÊM MỚI]: Bắt sự kiện Khởi tạo hoặc Đóng Tab (Báo Tử)
    if (msg === "INIT_PLC" || msg === "WEBGL_CLOSED") {
        actionQueue.push(async () => {
            if (!port.isOpen) return;
            console.log(`🚨 [COLD START / SHUTDOWN] Nhận lệnh ${msg}. Ép xả trạng thái...`);
            await writeMByte(0);
            await writeDRegister(100, 0);
            await writeDRegister(102, 0);
            cachedMState = 0;
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'INIT_DONE');
        });
        processActionQueue();
        return;
    }

    if (msg === "CLEAR_QUEUE") {
        actionQueue = [];
        console.log("🧹 [DỌN DẸP] Đã xóa hàng đợi lệnh!");
        return;
    }

    // [THÊM MỚI]: Nhận thông số từ Unity
    if (msg.startsWith("SET_PARAM")) {
        let parts = msg.split(',');
        if (parts.length === 3) {
            let tPulse = parseInt(parts[1]);
            let tFreq = parseInt(parts[2]);
            actionQueue.push(async () => {
                if (!port.isOpen) return;
                await writeDRegister(100, tPulse);
                await writeDRegister(102, tFreq);
                mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'PARAM_SET_DONE');
            });
            processActionQueue();
        }
        return;
    }

    if (msg === "SYNC_STATE") {
        actionQueue.push(async () => {
            if (!port.isOpen) return;

            let mVal = await readMState();
            await delay(50);
            let m100Val = await readM100State(); // <-- ĐỌC THÊM M100
            await delay(50);
            let yVal = await readYState();
            await delay(50);

            let dValues = [];
            for (let dNum of TARGET_D_REGISTERS) {
                let dVal = await readDRegister(dNum);
                if (dVal !== null) dValues.push(`D${dNum}:${dVal}`);
                await delay(50);
            }

            if (mVal !== -1 && yVal !== -1) {
                cachedMState = mVal;
                cachedM100State = m100Val;
                let payload = [];
                for (let i = 0; i <= 7; i++) payload.push(`M${i}_${(mVal & (1 << i)) ? 'ON' : 'OFF'}`);
                payload.push(`M100_${m100Val ? 'ON' : 'OFF'}`); // <-- BƠM VÀO PAYLOAD
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
            if (!port.isOpen) {
                mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'ACTION_FAILED');
                return;
            }

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
            } else {
                await delay(20);
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

app.get("/health", (req, res) => {
    res.json({ service: "PLC Gateway", status: "running", mqtt: mqttClient.connected, com: port.isOpen });
});

app.listen(5003, () => { console.log("🚀 Gateway HTTP API đang chạy ở cổng 5003"); });

// ==========================================
// HỆ THỐNG GIÁM SÁT CỜ M100 (AUTO-POLLING TỐI ƯU)
// ==========================================
let isPollingM100 = false; // Cờ kiểm soát vòng lặp

async function pollM100() {
    // Nếu hệ thống đang bận xử lý các lệnh từ người dùng (như nhấn nút, Sync), tạm thời bỏ qua nhịp đọc này.
    if (isProcessing) {
        setTimeout(pollM100, 300);
        return;
    }

    isPollingM100 = true;

    // Đẩy lệnh đọc M100 vào Queue, nhưng với cơ chế kiểm soát
    actionQueue.push(async () => {
        if (!port.isOpen) return;

        let m100On = await readM100State();

        if (m100On !== null && m100On !== cachedM100State) {
            cachedM100State = m100On;
            let stateStr = m100On ? 'ON' : 'OFF';

            mqttClient.publish('iot/lab602/dieu_khien_plc/status', `M100_${stateStr}`);
            console.log(`\n🔄 [AUTO UPDATE] Trạng thái Motor: M100_${stateStr}`);
        }
    });

    await processActionQueue(); // Đợi hàm xử lý Queue hoàn thành trước khi đi tiếp
    isPollingM100 = false;

    // Chỉ kích hoạt lần đọc tiếp theo sau khi lần này ĐÃ KẾT THÚC (cộng thêm 300ms nghỉ)
    setTimeout(pollM100, 300);
}

// Khởi động vòng lặp giám sát
pollM100();

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