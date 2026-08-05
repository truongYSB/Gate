const mqtt = require('mqtt');
const { SerialPort } = require('serialport');
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const BROKER_URL = 'ws://localhost:9001'; 
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM8';

// 1. THÊM THUỘC TÍNH autoOpen: false ĐỂ CỔNG COM ĐÓNG MẶC ĐỊNH LÚC KHỞI ĐỘNG
const port = new SerialPort({ 
    path: PLC_PORT_NAME, 
    baudRate: 9600, 
    dataBits: 7, 
    parity: 'even', 
    stopBits: 1, 
    autoOpen: false 
});
const express = require("express");
const app = express();

let pendingResolve = null;
let rxBuffer = Buffer.alloc(0);
let rxTimer = null;

// ========================================================
// BIẾN QUẢN LÝ HEARTBEAT VÀ ĐÓNG MỞ COM PORT
// ========================================================
let heartbeatTimer = null;
const HEARTBEAT_TIMEOUT = 5000; // 5 giây không nhận được tín hiệu -> Đóng COM

port.on('open', () => console.log(`✅ Đã CHIẾM DỤNG cổng ${PLC_PORT_NAME} do có WebGL kết nối`));
port.on('close', () => console.log(`🔒 Đã NHẢ cổng ${PLC_PORT_NAME} do WebGL ngắt kết nối`));
port.on('error', (err) => console.log(`❌ Lỗi cổng COM:`, err.message));

port.on('data', (data) => {
    rxBuffer = Buffer.concat([rxBuffer, data]);
    clearTimeout(rxTimer);
    rxTimer = setTimeout(() => {
        if (pendingResolve) {
            pendingResolve(rxBuffer);
            pendingResolve = null;
        }
        rxBuffer = Buffer.alloc(0);
    }, 50);
});

function sendFrame(frame) {
    return new Promise((resolve) => {
        pendingResolve = resolve;
        port.write(frame);
        setTimeout(() => {
            if (pendingResolve === resolve) {
                pendingResolve = null;
                console.log("⚠️ PLC Timeout không phản hồi!");
                resolve(null);
            }
        }, 800);
    });
}

function buildReadFrame(addressHex, countHex) {
    let payload = '0' + addressHex + countHex + '\x03';
    let sum = 0;
    for (let i = 0; i < payload.length; i++) sum += payload.charCodeAt(i);
    let checksum = sum.toString(16).slice(-2).toUpperCase();
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

function buildWriteFrame(mNumber, isON) {
    let cmdChar = isON ? '7' : '8';
    let hexAddress = (0x0800 + parseInt(mNumber)).toString(16).padStart(4, '0').toUpperCase();
    let swappedAddress = hexAddress.substring(2, 4) + hexAddress.substring(0, 2);
    let payload = cmdChar + swappedAddress + '\x03';
    let sum = 0;
    for (let i = 0; i < payload.length; i++) sum += payload.charCodeAt(i);
    let checksum = sum.toString(16).slice(-2).toUpperCase();
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

async function setMState(mNumber, isON) {
    let frame = buildWriteFrame(mNumber, isON);
    let res = await sendFrame(frame);
    return res && res[0] === 0x06; 
}

async function readYState() {
    let frame = buildReadFrame('00A0', '01'); 
    let res = await sendFrame(frame);
    if (res && res[0] === 0x02) {
        let hexData = res.toString('ascii', 1, 3);
        return parseInt(hexData, 16); 
    }
    return -1;
}

async function readMState() {
    let frame = buildReadFrame('0100', '01'); 
    let res = await sendFrame(frame);
    if (res && res[0] === 0x02) {
        let hexData = res.toString('ascii', 1, 3);
        return parseInt(hexData, 16);
    }
    return -1;
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
        } catch (e) {
            console.error("Lỗi Action:", e);
        }
    }
    isProcessing = false;
}

mqttClient.on('connect', () => {
    console.log("✅ Gateway đã kết nối với Mosquitto Broker Local!");
    mqttClient.subscribe('iot/lab602/dieu_khien_plc/control');
});

mqttClient.on('message', (topic, message) => {
    let msg = message.toString().replace(/\0/g, '').trim();

    // ========================================================
    // XỬ LÝ HEARTBEAT (NHỊP TIM TỪ WEBGL)
    // ========================================================
    if (msg === "HEARTBEAT") {
        // Nếu cổng COM đang đóng (WebGL mới bật), thì mở lên
        if (!port.isOpen) {
            port.open((err) => {
                if (err) console.error("❌ Lỗi khi tự động mở COM:", err.message);
            });
        }
        
        // Reset lại bộ đếm tự sát 5 giây
        clearTimeout(heartbeatTimer);
        heartbeatTimer = setTimeout(() => {
            // Đóng cổng COM vì sau 5s không nhận được Heartbeat (đã tắt Web)
            if (port.isOpen) {
                console.log("🥀 Phát hiện WebGL ngắt kết nối. Đang nhả cổng COM...");
                port.close();
            }
        }, HEARTBEAT_TIMEOUT);

        return; // Dừng xử lý ở đây, không in log để tránh đầy Terminal
    }

    // (Tùy chọn) Có thể giữ lại hoặc bỏ các lệnh thủ công cũ
    if (msg === "COM_RELEASE") {
        if (port.isOpen) port.close();
        return;
    }
    if (msg === "COM_CLAIM") {
        if (!port.isOpen) port.open();
        return;
    }
    
    if (msg === "CLEAR_QUEUE") {
        actionQueue = []; 
        console.log("🧹 [DỌN DẸP] Đã xóa sạch hàng đợi lệnh cũ!");
        return;
    }

    // Các lệnh điều khiển khác
    if (msg.includes('_ON') || msg.includes('_OFF')) {
        console.log(`\n📥 [MQTT ĐÃ NHẬN] Lệnh điều khiển: ${msg}`);
        
        // Kiểm tra an toàn: Nếu COM chưa kịp mở mà đã có lệnh thì bỏ qua cảnh báo
        if (!port.isOpen) {
            console.log("⚠️ Bỏ qua lệnh do cổng COM chưa sẵn sàng (Chưa nhận được Heartbeat)!");
            return;
        }

        actionQueue.push(async () => {
            let commands = msg.split(','); 
            let payload = [];

            for (let cmd of commands) {
                let cleanCmd = cmd.trim();
                if(!cleanCmd) continue;

                let [mName, state] = cleanCmd.split('_');
                let mNumber = mName.replace('M', '');

                console.log(`⚙️ [XỬ LÝ BATCH] Đang gửi lệnh xuống PLC: ${mName} -> ${state}`);
                await setMState(mNumber, state === "ON");
                await delay(50); 
            }

            await delay(300);
            let yVal = await readYState();
            await delay(300);
            let mVal = await readMState();

            if (mVal !== -1) {
                for (let i = 0; i <= 2; i++) { 
                    let isON = (mVal & (1 << i)) !== 0;
                    payload.push(`M${i}_${isON ? 'ON' : 'OFF'}`); 
                }
            }
            if (yVal !== -1) {
                for (let i = 1; i <= 2; i++) { 
                    let isON = (yVal & (1 << i)) !== 0;
                    payload.push(`Y${i}_${isON ? 'ON' : 'OFF'}`); 
                }
            }

            payload.push('ACTION_DONE');
            let finalMessage = payload.join(',');
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', finalMessage);
            console.log(`📤 [HOÀN TẤT] Đã xử lý cụm lệnh: [${msg}]`);
        });
        processActionQueue();
    }
});