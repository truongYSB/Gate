const mqtt = require('mqtt');
const { SerialPort } = require('serialport');
const express = require("express");

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const BROKER_URL = 'ws://localhost:9001';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM8';

// BIẾN QUAN TRỌNG: Lưu đệm trạng thái M để không phải xuống PLC đọc lại mỗi lần nhấn nút
let cachedMState = 0; 

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

    console.log(`[RX HEX HOÀN CHỈNH]: ${rxBuffer.toString('hex').toUpperCase()}`);

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
        console.log(`[TX HEX]: ${frame.toString('hex').toUpperCase()}`);

        // Đã tăng Watchdog lên 1500ms
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

// ==========================================
// KIẾN TRÚC MỚI: HÀM BỌC WRAPPER SIÊU GỌN
// ==========================================
async function executeWithRetry(frame, validateFunc, retryName) {
    let maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        let res = await sendFrame(frame);
        
        if (validateFunc(res)) return res;

        if (attempt < maxRetries) {
            console.log(`♻️ Đang xả cáp và thử ${retryName} (Lần ${attempt})...`);
            await delay(300); // Đã tăng thời gian chờ đường truyền sạch lên 300ms
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

async function writeMByte(valueInt) {
    let frame = buildWriteByteFrame('0100', '01', valueInt);
    let res = await executeWithRetry(frame, (data) => {
        if (data && data[0] === 0x15) console.log(`⚠️ PLC NAK (0x15) - Lệnh ghi bị từ chối!`);
        return (data && data[0] === 0x06);
    }, "GHI LẠI BATCH");
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
    console.log(`\n📥 [MQTT ĐÃ NHẬN] Lệnh: ${msg}`);

    if (msg === "HEARTBEAT") {
        clearTimeout(heartbeatTimer);
        heartbeatTimer = setTimeout(() => {
            if (port.isOpen) {
                console.log("🥀 Mất kết nối WebGL (Heartbeat timeout). Đang đóng cổng COM...");
                port.close();
            }
        }, HEARTBEAT_TIMEOUT);

        if (isProcessing) return;
        if (!port.isOpen) port.open((err) => { if (err) console.error("❌ Lỗi mở cổng COM:", err.message); });
        return;
    }

    if (msg === "CLEAR_QUEUE") {
        actionQueue = [];
        console.log("🧹 [DỌN DẸP] Đã xóa sạch hàng đợi lệnh cũ!");
        return;
    }

    if (msg === "SYNC_STATE") {
        actionQueue.push(async () => {
            if (!port.isOpen) return;
            console.log("🔄 [ĐỒNG BỘ] Đang quét toàn bộ trạng thái PLC cho WebGL...");

            let mVal = await readMState();
            await delay(50);
            let yVal = await readYState();

            if (mVal !== -1 && yVal !== -1) {
                cachedMState = mVal; // CẬP NHẬT CACHE M CHO LẦN KHỞI ĐỘNG ĐẦU TIÊN
                
                let payload = [];
                for (let i = 0; i <= 7; i++) payload.push(`M${i}_${(mVal & (1 << i)) ? 'ON' : 'OFF'}`);
                for (let i = 0; i <= 7; i++) payload.push(`Y${i}_${(yVal & (1 << i)) ? 'ON' : 'OFF'}`);
                payload.push('SYNC_DONE');

                mqttClient.publish('iot/lab602/dieu_khien_plc/status', payload.join(','));
                console.log(`📤 [ĐỒNG BỘ HOÀN TẤT] Gửi về Unity: [${payload.join(',')}]`);
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

            // DÙNG BIẾN CACHE: Không cần hỏi lại phần cứng M đang là bao nhiêu nữa!
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

            // Ghi trạng thái M mới xuống
            if (newMVal !== cachedMState) {
                console.log(`⚙️ [XỬ LÝ BATCH] Ghi Cụm M Hex [${newMVal.toString(16).toUpperCase().padStart(2, '0')}]`);
                let success = await writeMByte(newMVal);
                if (!success) {
                    hasError = true;
                } else {
                    cachedMState = newMVal; // Ghi thành công thì cập nhật lại Cache
                }
                await delay(30);
            } else {
                console.log("⏩ Trạng thái M không đổi, bỏ qua bước ghi.");
                await delay(20);
            }

            // Chỉ cần Đọc Y để lấy output
            let yVal = await readYState();

            if (yVal === -1 || hasError) {
                payload.push('ACTION_FAILED');
                console.log("⚠️ Cảnh báo: Lỗi kết nối phần cứng. Đánh rớt lệnh!");
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

            let finalMessage = payload.join(',');
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', finalMessage);
            console.log(`📤 [HOÀN TẤT] Trạng thái gửi về Unity: [${finalMessage}]`);
        });

        processActionQueue();
    }
});

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