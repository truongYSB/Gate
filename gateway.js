const mqtt = require('mqtt');
const { SerialPort } = require('serialport');
const express = require("express");

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const BROKER_URL = 'ws://localhost:9001';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM8';

// ==========================================
// PHẦN 1: KHỞI TẠO & HỆ THỐNG GIAO TIẾP SERIAL
// ==========================================
const port = new SerialPort({
    path: PLC_PORT_NAME,
    baudRate: 9600,
    dataBits: 7,
    parity: 'even',
    stopBits: 1,
    autoOpen: false
});

const app = express();

// Quản lý trạng thái Promise độc lập, chống ghi đè và double resolve
let responseHandler = null;
let timeoutHandle = null;

let rxBuffer = Buffer.alloc(0);

let heartbeatTimer = null;
const HEARTBEAT_TIMEOUT = 5000;

port.on('open', () => console.log(`✅ Đã mở và chiếm dụng cổng ${PLC_PORT_NAME}`));
port.on('close', () => console.log(`🔒 Đã nhả cổng ${PLC_PORT_NAME}`));
port.on('error', (err) => console.log(`❌ Lỗi cổng COM:`, err.message));

// Hàm xử lý và dọn rác Buffer
function processBuffer() {
    if (rxBuffer.length === 0) return;

    // 1. --- CƠ CHẾ LỌC RÁC TỰ ĐỘNG ---
    // Tìm vị trí của ký tự bắt đầu hợp lệ (STX: 0x02, ACK: 0x06, NAK: 0x15)
    let validIndex = -1;
    for (let i = 0; i < rxBuffer.length; i++) {
        if (rxBuffer[i] === 0x02 || rxBuffer[i] === 0x06 || rxBuffer[i] === 0x15) {
            validIndex = i;
            break;
        }
    }

    // Nếu có byte rác (ví dụ byte '37' bị kẹt) nằm trước ký tự hợp lệ, cắt bỏ chúng
    if (validIndex > 0) {
        rxBuffer = rxBuffer.slice(validIndex);
    } else if (validIndex === -1) {
        // Toàn rác, không có tín hiệu hợp lệ -> Xóa sạch
        rxBuffer = Buffer.alloc(0);
        return;
    }
    // ---------------------------------

    console.log(`[RX HEX HOÀN CHỈNH]: ${rxBuffer.toString('hex').toUpperCase()}`);

    let currentHandler = responseHandler;
    responseHandler = null;
    if (timeoutHandle) {
        clearTimeout(timeoutHandle);
        timeoutHandle = null;
    }

    let frameData = Buffer.from(rxBuffer); // Copy dữ liệu an toàn
    rxBuffer = Buffer.alloc(0); // Reset buffer ngay lập tức

    if (currentHandler) {
        currentHandler(frameData);
    }
}

// 2. --- BỘ LỌC DỮ LIỆU THÔNG MINH (Strict Structural Framing) ---
port.on('data', (data) => {
    rxBuffer = Buffer.concat([rxBuffer, data]);

    let isComplete = false;

    // Kiểm tra xem đã nhận được gói lệnh hoàn chỉnh chưa
    if (rxBuffer.includes(0x06) || rxBuffer.includes(0x15)) {
        isComplete = true; // Đã nhận đủ ACK hoặc NAK
    } else if (rxBuffer.includes(0x02) && rxBuffer.includes(0x03)) {
        let etxIndex = rxBuffer.indexOf(0x03);
        // Khung chuẩn: Phải có STX, ETX và đúng 2 byte Checksum theo sau ETX
        if (rxBuffer.length >= etxIndex + 3) {
            isComplete = true;
        }
    }

    // Chỉ xử lý khi khung dữ liệu đã thực sự hoàn chỉnh, không dùng timeout ép buộc
    if (isComplete) {
        processBuffer();
    }
});

// Hàm sendFrame mới: Cập nhật Watchdog độc lập chống kẹt hàng đợi
function sendFrame(frame) {
    return new Promise((resolve) => {
        // Hủy bỏ mọi tiến trình chờ trước đó nếu có
        if (timeoutHandle) clearTimeout(timeoutHandle);
        responseHandler = null;

        let isResolved = false;

        const safeResolve = (result) => {
            if (!isResolved) {
                isResolved = true;
                if (timeoutHandle) {
                    clearTimeout(timeoutHandle);
                    timeoutHandle = null;
                }
                responseHandler = null;
                resolve(result);
            }
        };

        responseHandler = (data) => {
            safeResolve(data);
        };

        console.log(`[TX HEX]: ${frame.toString('hex').toUpperCase()}`);

        // [SỬA LỖI QUAN TRỌNG]: Đặt Watchdog (Timeout) ở ngoài cùng!
        // Bất chấp driver USB có bị treo hay không, đúng 800ms là phải hủy Promise để cứu hàng đợi.
        timeoutHandle = setTimeout(() => {
            if (!isResolved) {
                console.log("⚠️ PLC Timeout không phản hồi (Giải cứu hàng đợi)!");
                rxBuffer = Buffer.alloc(0); // Dọn sạch rác để không kẹt lệnh sau
                safeResolve(null);
            }
        }, 800);

        // Ghi dữ liệu xuống cổng COM
        port.write(frame, (err) => {
            if (err) {
                console.error("❌ Lỗi port.write:", err.message);
                safeResolve(null);
            }
        });
    });
}

// ==========================================
// PHẦN 2: FRAME PARSER & CÁC HÀM ĐỌC/GHI PLC
// ==========================================

function calculateChecksum(payloadStr) {
    let sum = 0;
    for (let i = 0; i < payloadStr.length; i++) {
        sum += payloadStr.charCodeAt(i);
    }
    return sum.toString(16).slice(-2).toUpperCase();
}

function buildReadFrame(addressHex, countHex) {
    let payload = '0' + addressHex + countHex + '\x03';
    let checksum = calculateChecksum(payload);
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

// HÀM MỚI: Build mã lệnh Ghi 1 Byte (8 Bits M0-M7) cùng lúc
function buildWriteByteFrame(addressHex, countHex, valueInt) {
    // Chuyển số nguyên thành chuỗi Hex 2 ký tự (VD: 5 -> "05")
    let hexData = valueInt.toString(16).padStart(2, '0').toUpperCase();

    // Mã lệnh '1' là Write Device Memory
    let payload = '1' + addressHex + countHex + hexData + '\x03';
    let checksum = calculateChecksum(payload);
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

// HÀM MỚI: Ghi toàn bộ trạng thái M0-M7 xuống PLC (Có Auto-Retry)
async function writeMByte(valueInt) {
    let maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        // Địa chỉ 0100 là dải 8 bit từ M0 đến M7, count là 01 (1 byte)
        let frame = buildWriteByteFrame('0100', '01', valueInt);
        let res = await sendFrame(frame);

        if (res && res.length > 0) {
            if (res[0] === 0x15) {
                console.log(`⚠️ PLC NAK (0x15) - Lệnh ghi Cụm (Batch) bị từ chối!`);
                return false;
            }
            if (res[0] === 0x06) return true; // ACK (0x06) - Thành công
        }

        if (attempt < maxRetries) {
            console.log(`♻️ Đang xả cáp và thử GHI LẠI BATCH (Lần ${attempt})...`);
            await delay(100);
        }
    }
    return false;
}

function validateResponseChecksum(resBuffer) {
    if (!resBuffer || resBuffer.length < 3) return false;
    let etxIndex = resBuffer.indexOf(0x03);
    if (etxIndex === -1 || resBuffer.length < etxIndex + 3) return false;

    let payloadToCheck = resBuffer.toString('ascii', 1, etxIndex + 1);
    let receivedChecksum = resBuffer.toString('ascii', etxIndex + 1, etxIndex + 3);
    let calculatedChecksum = calculateChecksum(payloadToCheck);

    return receivedChecksum === calculatedChecksum;
}

// BỔ SUNG AUTO-RETRY CHO LỆNH ĐỌC Y
async function readYState() {
    let maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        let frame = buildReadFrame('00A0', '01');
        let res = await sendFrame(frame);

        if (res && res[0] === 0x02) {
            if (validateResponseChecksum(res)) {
                let hexData = res.toString('ascii', 1, 3);
                return parseInt(hexData, 16);
            } else {
                console.log(`⚠️ Cảnh báo: Checksum Y không khớp ở lần thử ${attempt}!`);
            }
        }

        if (attempt < maxRetries) {
            console.log(`♻️ Đang xả cáp và thử ĐỌC LẠI Y (Lần ${attempt})...`);
            await delay(100);
        }
    }
    return -1;
}

// BỔ SUNG AUTO-RETRY CHO LỆNH ĐỌC M
async function readMState() {
    let maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        let frame = buildReadFrame('0100', '01');
        let res = await sendFrame(frame);

        if (res && res[0] === 0x02) {
            if (validateResponseChecksum(res)) {
                let hexData = res.toString('ascii', 1, 3);
                return parseInt(hexData, 16);
            } else {
                console.log(`⚠️ Cảnh báo: Checksum M không khớp ở lần thử ${attempt}!`);
            }
        }

        if (attempt < maxRetries) {
            console.log(`♻️ Đang xả cáp và thử ĐỌC LẠI M (Lần ${attempt})...`);
            await delay(100);
        }
    }
    return -1;
}

// ==========================================
// PHẦN 3: QUEUE, MQTT, HEARTBEAT & EXPRESS
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

    // Xử lý Heartbeat từ WebGL
    if (msg === "HEARTBEAT") {
        clearTimeout(heartbeatTimer);
        heartbeatTimer = setTimeout(() => {
            if (port.isOpen) {
                console.log("🥀 Mất kết nối WebGL (Heartbeat timeout). Đang đóng cổng COM...");
                port.close();
            }
        }, HEARTBEAT_TIMEOUT);

        if (isProcessing) return;

        if (!port.isOpen) {
            port.open((err) => { if (err) console.error("❌ Lỗi mở cổng COM:", err.message); });
        }
        return;
    }

    if (msg === "CLEAR_QUEUE") {
        actionQueue = [];
        console.log("🧹 [DỌN DẸP] Đã xóa sạch hàng đợi lệnh cũ!");
        return;
    }

    // =======================================================
    // TÍNH NĂNG MỚI: ĐỒNG BỘ TOÀN BỘ TRẠNG THÁI LẦN ĐẦU
    // =======================================================
    if (msg === "SYNC_STATE") {
        actionQueue.push(async () => {
            if (!port.isOpen) return;
            console.log("🔄 [ĐỒNG BỘ] Đang quét toàn bộ trạng thái PLC cho WebGL...");

            let mVal = await readMState();
            await delay(50);
            let yVal = await readYState();

            if (mVal !== -1 && yVal !== -1) {
                let payload = [];
                // Gửi toàn bộ 8 bit M (M0 -> M7)
                for (let i = 0; i <= 7; i++) {
                    payload.push(`M${i}_${(mVal & (1 << i)) ? 'ON' : 'OFF'}`);
                }
                // Gửi toàn bộ 2 bit Y (Y1 -> Y2)
                for (let i = 1; i <= 2; i++) {
                    payload.push(`Y${i}_${(yVal & (1 << i)) ? 'ON' : 'OFF'}`);
                }
                payload.push('ACTION_DONE');

                let finalMessage = payload.join(',');
                mqttClient.publish('iot/lab602/dieu_khien_plc/status', finalMessage);
                console.log(`📤 [ĐỒNG BỘ HOÀN TẤT] Gửi về Unity: [${finalMessage}]`);
            }
        });
        processActionQueue();
        return;
    }

    // =======================================================
    // XỬ LÝ LỆNH ĐIỀU KHIỂN ON/OFF (TỐI ƯU PAYLOAD)
    // =======================================================
    if (msg.includes('_ON') || msg.includes('_OFF')) {
        actionQueue.push(async () => {
            if (!port.isOpen) {
                console.log("⚠️ Bỏ qua lệnh do cổng COM chưa mở!");
                mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'ACTION_FAILED');
                return;
            }

            let commands = msg.split(',');
            let payload = [];
            let hasError = false;

            // 1. Đọc M
            let mVal = await readMState();
            if (mVal === -1) {
                console.log("⚠️ Cảnh báo: Lỗi đọc M ban đầu. Đánh rớt lệnh!");
                mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'ACTION_FAILED');
                return;
            }

            await delay(40);
            let newMVal = mVal;

            // 2. Tính toán RAM
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

            // 3. Ghi Cụm
            if (newMVal !== mVal) {
                console.log(`⚙️ [XỬ LÝ BATCH] Ghi Cụm M Hex [${newMVal.toString(16).toUpperCase().padStart(2, '0')}]`);
                let success = await writeMByte(newMVal);
                if (!success) hasError = true;

                mVal = newMVal;
                await delay(30);
            } else {
                console.log("⏩ Trạng thái M không đổi, bỏ qua bước ghi.");
                await delay(20);
            }

            // 4. Đọc Y
            let yVal = await readYState();

            // 5. ĐÓNG GÓI PAYLOAD TỐI ƯU (INCREMENTAL UPDATE)
            if (yVal === -1 || hasError) {
                payload.push('ACTION_FAILED');
                console.log("⚠️ Cảnh báo: Lỗi kết nối phần cứng. Đánh rớt lệnh!");
            } else {
                // CHỈ lọc ra các nút M mà Unity vừa gửi lệnh để trả về kết quả
                for (let cmd of commands) {
                    let cleanCmd = cmd.trim();
                    if (!cleanCmd) continue;

                    let [mName, _] = cleanCmd.split('_');
                    let mNumber = parseInt(mName.replace('M', ''));

                    if (mNumber >= 0 && mNumber <= 7) {
                        let isON = (mVal & (1 << mNumber)) !== 0;
                        payload.push(`M${mNumber}_${isON ? 'ON' : 'OFF'}`);
                    }
                }

                // LUÔN gửi kèm trạng thái Y vì mạch logic PLC có thể tự bật/tắt Y
                for (let i = 1; i <= 2; i++) {
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

// Health check endpoint
app.get("/health", (req, res) => {
    res.json({
        service: "PLC Gateway",
        status: "running",
        mqtt: mqttClient.connected,
        com: port.isOpen
    });
});

app.listen(5003, () => {
    console.log("🚀 Gateway HTTP API đang chạy ở cổng 5003");
});

// Dọn dẹp tài nguyên khi đóng chương trình (Graceful Shutdown)
function gracefulShutdown() {
    console.log("\n🛑 Đang tắt Gateway và giải phóng tài nguyên...");
    if (port.isOpen) {
        port.close(() => {
            console.log("✅ Đã đóng cổng COM an toàn.");
            process.exit(0);
        });
    } else {
        process.exit(0);
    }
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);