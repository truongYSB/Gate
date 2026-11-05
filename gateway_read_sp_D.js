const mqtt = require('mqtt');
const { SerialPort } = require('serialport');
const express = require("express");

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const BROKER_URL = 'ws://localhost:9001';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM4';

let cachedMState = 0;
let cachedM100State = false;
let cachedD = { 100: 3200, 102: 1600 };
const TARGET_D_REGISTERS = [100, 102];

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
let expectedType = null;

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
        rxBuffer = Buffer.alloc(0);
        return;
    }

    rxBuffer = Buffer.concat([rxBuffer, data]);

    if (expectedType === 'ACK') {
        if (rxBuffer.includes(0x06)) completeResponse(Buffer.from([0x06]));
        else if (rxBuffer.includes(0x15)) completeResponse(Buffer.from([0x15]));
        return;
    }

    if (expectedType === 'DATA') {
        let stxIndex = rxBuffer.indexOf(0x02);
        if (stxIndex === -1) return;
        if (stxIndex > 0) rxBuffer = rxBuffer.slice(stxIndex);

        let etxIndex = rxBuffer.indexOf(0x03);
        if (etxIndex !== -1 && rxBuffer.length >= etxIndex + 3) {
            let validFrame = rxBuffer.slice(0, etxIndex + 3);
            completeResponse(validFrame);
        }
    }
});

function sendFrame(frame, type = 'DATA', quiet = false) {
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

        timeoutHandle = setTimeout(() => {
            if (!isResolved) {
                if (!quiet) console.log("⚠️ PLC Timeout không phản hồi (Giải cứu hàng đợi)!");
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

async function executeWithRetry(frame, type, validateFunc, retryName, quiet = false) {
    let maxRetries = quiet ? 1 : 3;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        let res = await sendFrame(frame, type, quiet);
        if (validateFunc(res)) return res;

        if (attempt < maxRetries && !quiet) {
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

async function readM100State(quiet = false) {
    let frame = buildReadFrame('010C', '01');
    let res = await executeWithRetry(frame, 'DATA', (data) => {
        return (data && data[0] === 0x02 && validateResponseChecksum(data));
    }, "ĐỌC M100", quiet);

    if (res) {
        let byteVal = parseInt(res.toString('ascii', 1, 3), 16);
        if (!quiet) console.log('[M100] VALUE:', byteVal);
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

let actionQueue = [];
let isProcessing = false;

async function processActionQueue() {
    if (isProcessing) return;
    isProcessing = true;
    while (actionQueue.length > 0) {
        let action = actionQueue.shift();
        try {
            await action();
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

    if (msg === "HEARTBEAT" || msg === "MODE_MOTOR" || msg === "MODE_BASIC") return;
    if (msg !== "SYNC_STATE") console.log(`\n📥 [MQTT IN]: ${msg}`);

    if (msg === "INIT_PLC" || msg === "WEBGL_CLOSED") {
        shouldPollM100 = false;
        actionQueue.push(async () => {
            if (!port.isOpen) return;
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
                await writeDRegister(102, tFreq);
                cachedD[100] = tPulse;
                cachedD[102] = tFreq;
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
                if (dVal !== null) {
                    cachedD[dNum] = dVal;
                    dValues.push(`D${dNum}:${dVal}`);
                }
                await delay(50);
            }

            if (mVal !== -1 && yVal !== -1) {
                cachedMState = mVal;
                cachedM100State = Boolean(m100Val);
                shouldPollM100 = cachedM100State;

                let payload = [];
                for (let i = 0; i <= 7; i++) payload.push(`M${i}_${(mVal & (1 << i)) ? 'ON' : 'OFF'}`);
                if (dValues.length > 0) payload.push(...dValues);
                payload.push(`M100_${cachedM100State ? 'ON' : 'OFF'}`);
                for (let i = 0; i <= 7; i++) payload.push(`Y${i}_${(yVal & (1 << i)) ? 'ON' : 'OFF'}`);
                payload.push('SYNC_DONE');

                mqttClient.publish('iot/lab602/dieu_khien_plc/status', payload.join(','));
            }
        });
        processActionQueue();
        return;
    }

    if (msg.includes('_ON') || msg.includes('_OFF')) {
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
                    if (mNumber === 1 || mNumber === 2) touchedMotorSwitch = true;
                }
            }

            if (newMVal !== cachedMState) {
                let success = await writeMByte(newMVal);
                if (!success) hasError = true;
                else cachedMState = newMVal;
                await delay(touchedMotorSwitch ? 80 : 40);
            } else {
                await delay(30);
            }

            let yVal = await readYState();

            if (hasError) {
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

                if (yVal !== -1) {
                    for (let i = 0; i <= 7; i++) {
                        let isON = (yVal & (1 << i)) !== 0;
                        payload.push(`Y${i}_${isON ? 'ON' : 'OFF'}`);
                    }
                }

                // KIỂM CHỨNG LADDER BẰNG SUY LUẬN LOGIC
                if (touchedMotorSwitch) {
                    let isM1_ON = (cachedMState & (1 << 1)) !== 0;
                    let isM2_ON = (cachedMState & (1 << 2)) !== 0;

                    if (isM1_ON || isM2_ON) {
                        await delay(120); // Đợi PLC kích hoạt lệnh DRVI

                        // Thăm dò lần 1
                        let m100On = await readM100State(true);

                        // Lọc nhiễu: Nếu bị Timeout (null), ta thử hỏi nhanh thêm 1 lần nữa cho chắc
                        if (m100On === null) {
                            m100On = await readM100State(true);
                        }

                        // Nếu PLC rảnh rỗi và trả lời ngay M100=0 -> CHƯA NẠP CODE HOẶC NẠP SAI!
                        if (m100On === false) {
                            console.log(`⚠️ [VERIFY] PLC trả lời M100=0. (Sinh viên chưa nạp code Ladder!)`);
                            cachedM100State = false;
                            shouldPollM100 = false;
                            payload.push('M100_OFF');
                        }
                        // Nếu bắt được 16 (true) HOẶC bị Timeout kép (null) ngay sau khi Ghi thành công
                        // -> CHẮC CHẮN ĐANG BĂM XUNG! Giao lại việc đếm giờ cho Unity.
                        else {
                            console.log(`🚀 [VERIFY] ${m100On === true ? 'Đọc được 16' : 'Timeout kép'} -> Motor chạy! Báo Unity tự đếm giờ.`);
                            cachedM100State = true;
                            shouldPollM100 = true; // Bật cờ ngầm để sau này Gateway còn biết đường báo M100_OFF

                            let pulses = Math.abs(cachedD[100]) > 0 ? Math.abs(cachedD[100]) : 3200;
                            let freq = Math.abs(cachedD[102]) > 0 ? Math.abs(cachedD[102]) : 1600;
                            let signedPulses = isM2_ON ? -pulses : pulses;

                            // Đẩy thông số sang cho thuật toán AutoSpinRoutine() của Unity
                            payload.push(`D100:${signedPulses}`);
                            payload.push(`D102:${freq}`);
                            payload.push('M100_ON');
                        }
                    } else {
                        // Nút tắt
                        cachedM100State = false;
                        shouldPollM100 = false;
                        payload.push('M100_OFF');
                        console.log(`🛑 [MOTOR STOP] Gạt tắt. Báo Unity dừng ngay.`);
                    }
                }
                payload.push('ACTION_DONE');
            }
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', payload.join(','));
        });
        processActionQueue();
    }
});

app.get("/health", (req, res) => res.json({ service: "PLC Gateway", status: "running" }));
app.listen(5003, () => console.log("🚀 Gateway HTTP API đang chạy ở cổng 5003"));

// ==========================================
// HỆ THỐNG GIÁM SÁT M100 THỰC TẾ
// ==========================================
async function pollM100() {
    if (!shouldPollM100 || isProcessing) {
        setTimeout(pollM100, 500);
        return;
    }

    let pollTask = async () => {
        if (!port.isOpen || !shouldPollM100) return;

        // Đọc M100 trong yên lặng
        let m100On = await readM100State(true);

        if (m100On !== null) {
            // [CỐT LÕI]: Chỉ báo Unity quay khi THỰC SỰ đọc được VALUE = 16 (m100On = true)
            if (m100On !== cachedM100State) {
                cachedM100State = m100On;

                if (m100On) {
                    let isM2_ON = (cachedMState & (1 << 2)) !== 0;
                    let pulses = Math.abs(cachedD[100]) > 0 ? Math.abs(cachedD[100]) : 3200;
                    let freq = Math.abs(cachedD[102]) > 0 ? Math.abs(cachedD[102]) : 1600;
                    let signedPulses = isM2_ON ? -pulses : pulses;

                    let payloadStr = `D100:${signedPulses},D102:${freq},M100_ON`;
                    mqttClient.publish('iot/lab602/dieu_khien_plc/status', payloadStr);
                    console.log(`\n🚀 [REAL SIGNAL] Đọc được M100=16 thật từ cáp! Báo Unity quay (${payloadStr}).`);
                } else {
                    shouldPollM100 = false;
                    mqttClient.publish('iot/lab602/dieu_khien_plc/status', `M100_OFF`);
                    console.log(`\n🛑 [REAL SIGNAL] Đọc được M100=0. Motor băm xung xong, tắt quét cáp.`);
                }
            }
        } else if (cachedM100State === false) {
            // Nếu gửi lệnh Đọc bị Timeout (null) tức là PLC đang bận băm xung, ta cứ kiên nhẫn đợi 
            // đến khi nào nhặt được đúng số 16 thì mới kích hoạt khối lệnh (m100On) bên trên.
        }
    };

    pollTask.isAutoPoll = true;
    actionQueue.push(pollTask);
    await processActionQueue();

    setTimeout(pollM100, 800);
}
pollM100();

process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));