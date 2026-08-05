const mqtt = require('mqtt');
const { SerialPort } = require('serialport');
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const BROKER_URL = 'ws://localhost:9001';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM4';

const port = new SerialPort({ path: PLC_PORT_NAME, baudRate: 9600, dataBits: 7, parity: 'even', stopBits: 1 });
const express = require("express");
const app = express();

let pendingResolve = null;
// 1. TẠO MỘT MẢNG ĐỂ GOM TẤT CẢ DỮ LIỆU
let payload = [];
// --- BỘ GOM MẢNH VỠ SERIAL (BUFFER ACCUMULATOR) ---
let rxBuffer = Buffer.alloc(0);
let rxTimer = null;

port.on('open', () => console.log(`✅ Đã mở cổng ${PLC_PORT_NAME}`));
port.on('error', (err) => console.log(`❌ Lỗi cổng COM:`, err.message));

// Bắt sự kiện Data và trả về cho Promise đang chờ

port.on('data', (data) => {
    // 1. Nối các mảnh vỡ dữ liệu mới vào chuỗi Buffer tổng
    rxBuffer = Buffer.concat([rxBuffer, data]);

    // 2. Hủy timer cũ, bắt đầu chờ 50ms xem cổng COM còn rớt mảnh nào nữa không
    clearTimeout(rxTimer);
    rxTimer = setTimeout(() => {
        console.log(`[RAW DATA HOÀN CHỈNH]:`, rxBuffer.toString('hex').toUpperCase());

        // Trả kết quả về cho hàm async đang chờ
        if (pendingResolve) {
            pendingResolve(rxBuffer);
            pendingResolve = null;
        }

        // 3. Xóa sạch Buffer để đón lệnh tiếp theo của Unity
        rxBuffer = Buffer.alloc(0);
    }, 50);
});

// Helper: Gửi frame và chờ phản hồi (Chống nghẽn COM)
function sendFrame(frame) {
    return new Promise((resolve) => {
        pendingResolve = resolve;
        port.write(frame);

        // Timeout 300ms nếu PLC không phản hồi hoặc cáp bị lỏng
        setTimeout(() => {
            if (pendingResolve === resolve) {
                pendingResolve = null;
                console.log("⚠️ PLC Timeout không phản hồi!");
                resolve(null);
            }
        }, 800);
    });
}

// ---------------------------------------------------------
// CÁC HÀM XÂY DỰNG MÃ LỆNH MITSUBISHI FX
// ---------------------------------------------------------
function buildReadFrame(addressHex, countHex) {
    // 1. Thêm '\x03' vào chuỗi payload ngay từ đầu để tính Checksum cho đúng
    let payload = '0' + addressHex + countHex + '\x03';
    let sum = 0;

    // 2. Tính tổng Checksum bao gồm cả '\x03'
    for (let i = 0; i < payload.length; i++) {
        sum += payload.charCodeAt(i);
    }

    let checksum = sum.toString(16).slice(-2).toUpperCase();

    // 3. Build buffer (không cần cộng thêm '\x03' ở đây nữa vì đã có trong payload)
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

// ---------------------------------------------------------
// HÀM GIAO TIẾP VỚI PLC THEO DẠNG ASYNC
// ---------------------------------------------------------
async function setMState(mNumber, isON) {
    let frame = buildWriteFrame(mNumber, isON);
    let res = await sendFrame(frame);
    return res && res[0] === 0x06; // Trả về true nếu nhận được ACK (0x06)
}

async function readYState() {
    let frame = buildReadFrame('00A0', '01'); // Đọc Y0 -> Y7
    let res = await sendFrame(frame);
    if (res && res[0] === 0x02) {
        let hexData = res.toString('ascii', 1, 3);
        return parseInt(hexData, 16); // Trả về số nguyên mô tả trạng thái Bit
    }
    return -1;
}

async function readMState() {
    let frame = buildReadFrame('0100', '01'); // Đọc M0 -> M7
    let res = await sendFrame(frame);
    if (res && res[0] === 0x02) {
        let hexData = res.toString('ascii', 1, 3);
        return parseInt(hexData, 16);
    }
    return -1;
}

// ---------------------------------------------------------
// QUẢN LÝ HÀNG ĐỢI LỆNH (Tránh xung đột COM)
// ---------------------------------------------------------
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
    console.log("✅ Gateway đã kết nối lên Đám mây EMQX!");
    mqttClient.subscribe('iot/lab602/dieu_khien_plc/control');
});

mqttClient.on('message', (topic, message) => {
    let msg = message.toString();

    // BỔ SUNG LOG BƯỚC 1: Xác nhận đã nhận được tin nhắn từ đám mây MQTT
    console.log(`\n📥 [MQTT ĐÃ NHẬN] Topic: ${topic} | Lệnh: ${msg}`);

    // 2. Nhả / Chiếm cổng COM
    if (msg === "COM_RELEASE") {
        if (port.isOpen) port.close(() => console.log("✅ Đã giải phóng COM"));
        return;
    }
    if (msg === "COM_CLAIM") {
        if (!port.isOpen) port.open(() => console.log("✅ Đã chiếm dụng COM"));
        return;
    }

    // BỔ SUNG ĐOẠN NÀY ĐỂ RESET HÀNG ĐỢI
    if (msg === "CLEAR_QUEUE") {
        actionQueue = []; // Bắn bỏ toàn bộ các lệnh đang xếp hàng
        console.log("🧹 [DỌN DẸP] Đã xóa sạch hàng đợi lệnh cũ!");
        return;
    }

    // 3. Lệnh M_ON / M_OFF thông thường từ Unity
    // 3. XỬ LÝ LỆNH TỪ UNITY (Hỗ trợ cả Lệnh Đơn và Lệnh Gộp cách nhau bởi dấu phẩy)
    if (msg.includes('_ON') || msg.includes('_OFF')) {
        actionQueue.push(async () => {
            // Tách các lệnh nếu có dấu phẩy (vd: "M0_OFF,M1_OFF,M2_ON")
            let commands = msg.split(',');
            
            // Làm mới payload cho mỗi vòng đời gửi (để tránh rác dữ liệu từ các lần trước)
            payload = [];

            // 1. VIẾT TRẠNG THÁI M LIÊN TỤC XUỐNG PLC
            for (let cmd of commands) {
                let cleanCmd = cmd.trim();
                if (!cleanCmd) continue;

                let [mName, state] = cleanCmd.split('_');
                let mNumber = mName.replace('M', '');

                console.log(`⚙️ [XỬ LÝ BATCH] Đang gửi lệnh xuống PLC: ${mName} -> ${state}`);
                await setMState(mNumber, state === "ON");

                // Nghỉ rất ngắn (50ms) giữa các lệnh ghi để phần cứng PLC xử lý kịp
                await delay(50);
            }

            // Nghỉ 400ms chờ PLC thực thi xong toàn bộ logic Relay
            await delay(300);

            // 2. ĐỌC LẠI TRẠNG THÁI Y
            let yVal = await readYState();
            await delay(300);

            // 3. ĐỌC LẠI TRẠNG THÁI M
            let mVal = await readMState();

            if (mVal !== -1) {
                for (let i = 0; i <= 2; i++) { // Chỉ gửi M0, M1, M2
                    let isON = (mVal & (1 << i)) !== 0;
                    payload.push(`M${i}_${isON ? 'ON' : 'OFF'}`);
                }
            }

            if (yVal !== -1) {
                for (let i = 1; i <= 2; i++) { // Chỉ gửi Y1, Y2
                    let isON = (yVal & (1 << i)) !== 0;
                    payload.push(`Y${i}_${isON ? 'ON' : 'OFF'}`);
                }
            }

            payload.push('ACTION_DONE');

            // 4. GỬI ĐÚNG 1 TIN NHẮN CHỨA TOÀN BỘ TRẠNG THÁI LÊN UNITY
            let finalMessage = payload.join(',');
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', finalMessage);

            console.log(`📤 [HOÀN TẤT] Đã xử lý cụm lệnh: [${msg}]`);
        });
        processActionQueue();
    }
});

// ---------------------------------------------------------
// API ENDPOINT FOR HEALTH CHECK
// ---------------------------------------------------------
app.get("/health", (req, res) => {
    res.json({
        service: "PLC2",
        status: "running",
        mqtt: mqttClient.connected,
        com: port.isOpen
    });
});

app.listen(5003, () => {
    console.log("PLC2 HTTP chạy ở cổng 5003");
});