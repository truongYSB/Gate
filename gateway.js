const mqtt = require('mqtt');
const { SerialPort } = require('serialport');
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const BROKER_URL = 'mqtt://broker.emqx.io:1883';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM4';

const port = new SerialPort({ path: PLC_PORT_NAME, baudRate: 9600, dataBits: 7, parity: 'even', stopBits: 1 });

let pendingResolve = null;

port.on('open', () => console.log(`✅ Đã mở cổng ${PLC_PORT_NAME}`));
port.on('error', (err) => console.log(`❌ Lỗi cổng COM:`, err.message));

// Bắt sự kiện Data và trả về cho Promise đang chờ
port.on('data', (data) => {
    // In ra dạng Hex để dễ phân tích
    console.log(`[RAW DATA TỪ PLC]:`, data.toString('hex').toUpperCase());

    if (pendingResolve) {
        pendingResolve(data);
        pendingResolve = null;
    }
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

    // 3. Lệnh M_ON / M_OFF thông thường từ Unity
    if (msg.includes('_ON') || msg.includes('_OFF')) {
        actionQueue.push(async () => {
            let [mName, state] = msg.split('_');
            let mNumber = mName.replace('M', '');

            // BỔ SUNG LOG BƯỚC 2: Bắt đầu xử lý ghi dữ liệu xuống PLC
            console.log(`⚙️ [XỬ LÝ] Đang gửi lệnh xuống PLC: Bật/Tắt ${mName} -> ${state}`);

            // 1. Viết trạng thái M
            await setMState(mNumber, state === "ON");

            // Nghỉ 100ms cho PLC xử lý xong lệnh Ghi
            await delay(100);

            // 2. Đọc lại trạng thái thực tế Y
            let yVal = await readYState();

            // Nghỉ 100ms tiếp theo trước khi gửi lệnh Đọc M
            await delay(100);

            // 3. Đọc lại trạng thái thực tế M
            let mVal = await readMState();

            // BỔ SUNG LOG BƯỚC 3: Hiển thị kết quả thu được từ PLC thật
            console.log(`📊 [KẾT QUẢ PLC] Trạng thái M (Hex->Int): ${mVal} | Trạng thái Y (Hex->Int): ${yVal}`);

            // --- XỬ LÝ VÀ LOG TRẠNG THÁI M ---
            if (mVal !== -1) {
                let activeM = []; // Mảng chứa các M đang bật
                for (let i = 0; i <= 7; i++) {
                    let isON = (mVal & (1 << i)) !== 0;
                    let st = isON ? 'ON' : 'OFF';

                    if (isON) activeM.push(`M${i}`); // Nếu bật thì thêm vào mảng

                    mqttClient.publish('iot/lab602/dieu_khien_plc/status', `M${i}_${st}`);
                }
                // In ra danh sách M đang bật
                console.log(`🟢 [TRẠNG THÁI M] Đang BẬT: ${activeM.length > 0 ? activeM.join(', ') : 'Không có'}`);
            } else {
                console.log("⚠️ [LỖI] Không đọc được trạng thái M từ PLC!");
            }

            // --- XỬ LÝ VÀ LOG TRẠNG THÁI Y ---
            if (yVal !== -1) {
                let activeY = []; // Mảng chứa các Y đang bật
                for (let i = 0; i <= 7; i++) { // Giả sử bạn kiểm tra Y0, Y1, Y2, Y3, Y4, Y5, Y6, Y7
                    let isON = (yVal & (1 << i)) !== 0;
                    let st = isON ? 'ON' : 'OFF';

                    if (isON) activeY.push(`Y${i}`);

                    mqttClient.publish('iot/lab602/dieu_khien_plc/status', `Y${i}_${st}`);
                }
                // In ra danh sách Y đang bật
                console.log(`🔵 [TRẠNG THÁI Y] Đang BẬT: ${activeY.length > 0 ? activeY.join(', ') : 'Không có'}`);
            } else {
                console.log("⚠️ [LỖI] Không đọc được trạng thái Y từ PLC!");
            }

            // BỔ SUNG LOG BƯỚC 4: Hoàn tất một chu kỳ
            console.log("📤 [HOÀN TẤT] Đã đẩy trạng thái mới lên Unity.");
        });
        processActionQueue();
    }
});