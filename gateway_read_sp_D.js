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

// Cờ điều khiển chỉ quét M100 khi Motor được kích hoạt (Giúp dùng chung Gateway cho bài Đèn)
let shouldPollM100 = false;

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
let expectedType = null; // 'ACK' (chờ 0x06/0x15) hoặc 'DATA' (chờ 0x02...0x03)

port.on('open', () => {
    console.log(`✅ Đã mở và chiếm dụng vĩnh viễn cổng ${PLC_PORT_NAME}`);
    mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'GATEWAY_READY');
});

port.on('close', () => console.log(`🔒 Cổng ${PLC_PORT_NAME} bị đóng (Ngoại lệ)`));
port.on('error', (err) => console.error(`❌ Lỗi cổng COM:`, err.message));

function completeResponse(frameData) {
    let currentHandler = responseHandler;
    responseHandler = null;
    expectedType = null;
    if (timeoutHandle) {
        clearTimeout(timeoutHandle);
        timeoutHandle = null;
    }
    rxBuffer = Buffer.alloc(0);
    if (currentHandler) {
        currentHandler(frameData);
    }
}

port.on('data', (data) => {
    if (!responseHandler) {
        // Nếu không có lệnh nào đang chờ mà có byte rác bay về -> Xóa bỏ ngay
        rxBuffer = Buffer.alloc(0);
        return;
    }

    rxBuffer = Buffer.concat([rxBuffer, data]);

    // 1. Nếu đang chờ phản hồi lệnh GHI (Chỉ nhận 0x06 hoặc 0x15)
    if (expectedType === 'ACK') {
        if (rxBuffer.includes(0x06)) {
            completeResponse(Buffer.from([0x06]));
        } else if (rxBuffer.includes(0x15)) {
            completeResponse(Buffer.from([0x15]));
        }
        return;
    }

    // 2. Nếu đang chờ phản hồi lệnh ĐỌC (Chỉ nhận khung STX 0x02 ... ETX 0x03 + 2 byte Checksum)
    if (expectedType === 'DATA') {
        let stxIndex = rxBuffer.indexOf(0x02);
        if (stxIndex === -1) {
            // Chưa có 0x02 (có thể dính byte 0x06 trễ từ trước) -> Chờ tiếp
            return;
        }
        if (stxIndex > 0) {
            // Cắt bỏ mọi byte rác (như 0x06) nằm trước 0x02
            rxBuffer = rxBuffer.slice(stxIndex);
        }

        let etxIndex = rxBuffer.indexOf(0x03);
        if (etxIndex !== -1 && rxBuffer.length >= etxIndex + 3) {
            let validFrame = rxBuffer.slice(0, etxIndex + 3);
            completeResponse(validFrame);
        }
    }
});

function sendFrame(frame, type = 'DATA') {
    return new Promise((resolve) => {
        if (timeoutHandle) clearTimeout(timeoutHandle);
        responseHandler = null;
        rxBuffer = Buffer.alloc(0);
        expectedType = type;
        let isResolved = false;

        const safeResolve = (result) => {
            if (!isResolved) {
                isResolved = true;
                if (timeoutHandle) { clearTimeout(timeoutHandle); timeoutHandle = null; }
                responseHandler = null;
                expectedType = null;
                resolve(result);
            }
        };

        responseHandler = (data) => { safeResolve(data); };

        // Watchdog 800ms (Đủ cho cổng phụ RS232, không làm treo hàng đợi tới 1.5s)
        timeoutHandle = setTimeout(() => {
            if (!isResolved) {
                console.log("⚠️ PLC Timeout không phản hồi (Giải cứu hàng đợi)!");
                rxBuffer = Buffer.alloc(0);
                safeResolve(null);
            }
        }, 800);

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

async function executeWithRetry(frame, type, validateFunc, retryName) {
    let maxRetries = 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        let res = await sendFrame(frame, type);
        if (validateFunc(res)) return res;

        if (attempt < maxRetries) {
            console.log(`♻️ Đang xả cáp và thử ${retryName} (Lần ${attempt})...`);
            await delay(150);
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

async function writeDRegister(dNumber, value) {
    let address = 0x1000 + (dNumber * 2);
    let addressHex = address.toString(16).toUpperCase().padStart(4, '0');

    let unsignedVal = value < 0 ? (0x10000 + value) : value;
    let hexValue = unsignedVal.toString(16).toUpperCase().padStart(4, '0');
    let highByte = hexValue.substring(0, 2);
    let lowByte = hexValue.substring(2, 4);
    let payloadData = lowByte + highByte;

    let payload = '1' + addressHex + '02' + payloadData + '\x03';
    let checksum = calculateChecksum(payload);
    let frame = Buffer.from('\x02' + payload + checksum, 'ascii');

    let res = await executeWithRetry(frame, 'ACK', (data) => {
        return (data && data[0] === 0x06);
    }, `GHI D${dNumber}`);
    return res !== null;
}

async function writeMByte(valueInt) {
    let frame = buildWriteByteFrame('0100', '01', valueInt);
    let res = await executeWithRetry(frame, 'ACK', (data) => {
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
    let res = await executeWithRetry(frame, 'DATA', (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, "ĐỌC Y");
    return res ? parseInt(res.toString('ascii', 1, 3), 16) : -1;
}

async function readMState() {
    let frame = buildReadFrame('0100', '01');
    let res = await executeWithRetry(frame, 'DATA', (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, "ĐỌC M");
    return res ? parseInt(res.toString('ascii', 1, 3), 16) : -1;
}

async function readM100State() {
    let frame = buildReadFrame('010C', '01');
    let res = await executeWithRetry(frame, 'DATA', (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
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

    let res = await executeWithRetry(frame, 'DATA', (data) => {
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
// HỆ THỐNG QUEUE & MQTT (CÓ KHOẢNG NGHỈ CHỐNG NGHẼN CÁP)
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
            // BẮT BUỘC: Nghỉ 80ms giữa 2 phiên truyền Serial để cổng phụ RS232 của PLC kịp đóng khung!
            await delay(80);
        } catch (e) {
            console.error("❌ Lỗi Queue:", e);
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
    if (msg !== "SYNC_STATE") console.log(`\n📥 [MQTT IN]: ${msg}`);

    if (msg === "INIT_PLC" || msg === "WEBGL_CLOSED") {
        shouldPollM100 = false;
        actionQueue.push(async () => {
            if (!port.isOpen) return;
            console.log(`🚨 [COLD START / SHUTDOWN] Nhận lệnh ${msg}. Ép xả trạng thái...`);
            await writeMByte(0);
            await delay(50);
            await writeDRegister(100, 0);
            await delay(50);
            await writeDRegister(102, 0);
            cachedMState = 0;
            cachedM100State = false;
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'INIT_DONE');
        });
        processActionQueue();
        return;
    }

    if (msg === "CLEAR_QUEUE") {
        actionQueue = [];
        shouldPollM100 = false;
        console.log("🧹 [DỌN DẸP] Đã xóa hàng đợi lệnh!");
        return;
    }

    if (msg.startsWith("SET_PARAM")) {
        let parts = msg.split(',');
        if (parts.length === 3) {
            let tPulse = parseInt(parts[1]);
            let tFreq = parseInt(parts[2]);
            actionQueue.push(async () => {
                if (!port.isOpen) return;
                await writeDRegister(100, tPulse);
                await delay(50);
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
            await delay(60);
            let m100Val = await readM100State();
            await delay(60);
            let yVal = await readYState();
            await delay(60);

            let dValues = [];
            for (let dNum of TARGET_D_REGISTERS) {
                let dVal = await readDRegister(dNum);
                if (dVal !== null) dValues.push(`D${dNum}:${dVal}`);
                await delay(60);
            }

            if (mVal !== -1 && yVal !== -1) {
                cachedMState = mVal;
                cachedM100State = Boolean(m100Val);
                shouldPollM100 = cachedM100State; // Nếu lúc mới vào mà motor đang chạy thì mới quét

                let payload = [];
                for (let i = 0; i <= 7; i++) payload.push(`M${i}_${(mVal & (1 << i)) ? 'ON' : 'OFF'}`);
                payload.push(`M100_${cachedM100State ? 'ON' : 'OFF'}`);
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
        // ƯU TIÊN CAO NHẤT: Nếu người dùng bấm nút M_ON / M_OFF, xóa bớt các lệnh đọc M100 tự động đang xếp hàng
        // để lệnh bấm nút được thực thi NGAY LẬP TỨC!
        actionQueue = actionQueue.filter(fn => !fn.isAutoPoll);

        actionQueue.push(async () => {
            if (!port.isOpen) {
                mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'ACTION_FAILED');
                return;
            }

            let commands = msg.split(',');
            let payload = [];
            let hasError = false;
            let newMVal = cachedMState;
            let touchedMotorSwitch = false;

            for (let cmd of commands) {
                let cleanCmd = cmd.trim();
                if (!cleanCmd) continue;
                let [mName, state] = cleanCmd.split('_');
                let mNumber = parseInt(mName.replace('M', ''));

                if (mNumber >= 0 && mNumber <= 7) {
                    if (state === "ON") newMVal |= (1 << mNumber);
                    else newMVal &= ~(1 << mNumber);

                    if (mNumber === 1 || mNumber === 2) {
                        touchedMotorSwitch = true;
                    }
                }
            }

            if (newMVal !== cachedMState) {
                let success = await writeMByte(newMVal);
                if (!success) hasError = true;
                else cachedMState = newMVal;
                // Nếu vừa kích hoạt/tắt motor, chờ 80ms để PLC thực thi lệnh MOV & DRVI xong trong vòng quét
                await delay(touchedMotorSwitch ? 80 : 40);
            } else {
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

                // NẾU CÓ TÁC ĐỘNG VÀO M1 HOẶC M2: Kiểm tra ngay M100 và D100/D102
                if (touchedMotorSwitch) {
                    await delay(60);
                    let m100On = await readM100State();
                    if (m100On !== null) {
                        cachedM100State = m100On;
                        payload.push(`M100_${m100On ? 'ON' : 'OFF'}`);

                        if (m100On) {
                            // Nếu Motor bắt đầu quay -> Đọc D100, D102 gửi về Unity để xoay đúng chiều/tốc độ
                            // và BẬT cờ shouldPollM100 để canh lúc nào motor dừng!
                            shouldPollM100 = true;
                            for (let dNum of TARGET_D_REGISTERS) {
                                await delay(50);
                                let dVal = await readDRegister(dNum);
                                if (dVal !== null) payload.push(`D${dNum}:${dVal}`);
                            }
                        } else {
                            // Nếu tắt M1_OFF làm motor dừng ngay -> Tắt luôn việc quét M100!
                            shouldPollM100 = false;
                        }
                    }
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
// HỆ THỐNG GIÁM SÁT CỜ M100 THÔNG MINH (CHỈ QUÉT KHI MOTOR ĐANG CHẠY)
// ==========================================
async function pollM100() {
    // Nếu motor đang KHÔNG chạy (hoặc đang ở bài Bật Đèn), hoặc Queue đang bận -> Không làm phiền cổng COM!
    if (!shouldPollM100 || isProcessing) {
        setTimeout(pollM100, 400);
        return;
    }

    const pollTask = async () => {
        if (!port.isOpen || !shouldPollM100) return;

        let m100On = await readM100State();

        if (m100On !== null && m100On !== cachedM100State) {
            cachedM100State = m100On;
            let stateStr = m100On ? 'ON' : 'OFF';

            mqttClient.publish('iot/lab602/dieu_khien_plc/status', `M100_${stateStr}`);
            console.log(`\n🔄 [AUTO UPDATE] Trạng thái Motor: M100_${stateStr}`);

            // Ngay khi phát hiện Motor đã tự dừng (M100 về OFF) -> Ngừng quét tự động luôn!
            if (!m100On) {
                shouldPollM100 = false;
                console.log("🛑 [AUTO POLL] Motor đã dừng. Tắt quét M100 để giải phóng cổng COM.");
            }
        }
    };
    pollTask.isAutoPoll = true; // Đánh dấu để nhường đường ngay lập tức nếu người dùng bấm nút

    actionQueue.push(pollTask);
    await processActionQueue();

    // Quét nhịp 600ms/lần để cổng phụ RS232 hoàn toàn thoải mái khi PLC đang băm xung
    setTimeout(pollM100, 600);
}

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