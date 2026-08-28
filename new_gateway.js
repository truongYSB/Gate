const express = require('express');
const mqtt = require('mqtt');
const { SerialPort } = require('serialport');

const app = express();
const port = 3000;

// ==========================================
// 1. CẤU HÌNH MQTT
// ==========================================
const BROKER_URL = 'ws://localhost:9001';
const mqttClient = mqtt.connect(BROKER_URL);

const TOPIC_STATUS = 'iot/lab602/dieu_khien_plc/status';
const TOPIC_CONTROL = 'iot/lab602/dieu_khien_plc/control';

mqttClient.on('connect', () => {
    console.log('🟢 Đã kết nối tới Mosquitto Broker qua WebSocket');
    mqttClient.subscribe(TOPIC_CONTROL, (err) => {
        if (!err) console.log(`📡 Đã subscribe topic: ${TOPIC_CONTROL}`);
    });
});

// ==========================================
// 2. CẤU HÌNH SERIALPORT (ĐẶC TRƯNG CỦA MITSUBISHI FX)
// ==========================================
const plcPort = new SerialPort({
    path: 'COM8',
    baudRate: 9600,
    dataBits: 7,         // FX Protocol dùng 7 bits
    parity: 'even',      // FX Protocol dùng parity Even
    stopBits: 1,
    autoOpen: false
});

plcPort.open((err) => {
    if (err) return console.error('🔴 Lỗi mở port COM8: ', err.message);
    console.log('🔌 Đã kết nối thành công tới PLC FX3U qua COM8');
});

// ==========================================
// 3. HÀM TẠO FRAME THEO FX PROGRAMMING PROTOCOL
// ==========================================
const STX = '\x02'; // Start of Text
const ETX = '\x03'; // End of Text

// Tính Checksum (Tổng mã ASCII từ CMD đến ETX, lấy 2 ký tự Hex cuối)
function calculateChecksum(payload) {
    let sum = 0;
    for (let i = 0; i < payload.length; i++) {
        sum += payload.charCodeAt(i);
    }
    return sum.toString(16).toUpperCase().slice(-2);
}

// Bật/Tắt bit cho M hoặc Y (Lệnh 7: Force ON, Lệnh 8: Force OFF)
function getForceBitFrame(type, address, value) {
    let base = 0;
    if (type === 'M') base = 0x0800 + parseInt(address);
    if (type === 'Y') base = 0x0500 + parseInt(address, 8); // Y là Octal

    let addrHex = base.toString(16).toUpperCase().padStart(4, '0');
    let addrSwapped = addrHex.substring(2, 4) + addrHex.substring(0, 2); // Đảo byte

    let cmd = value ? '7' : '8';
    let payload = `${cmd}${addrSwapped}${ETX}`;
    let sum = calculateChecksum(payload);

    return Buffer.from(STX + payload + sum, 'ascii');
}

// Ghi dữ liệu vào thanh ghi D (Lệnh 1: Write)
function getWriteWordFrame(address, value) {
    let baseAddr = 0x1000 + (parseInt(address) * 2);
    let addrStr = baseAddr.toString(16).toUpperCase().padStart(4, '0');

    let valHex = parseInt(value).toString(16).toUpperCase().padStart(4, '0');
    let valSwapped = valHex.substring(2, 4) + valHex.substring(0, 2); // Đảo byte dữ liệu

    // 02 = số lượng byte cần ghi (1 Word = 2 byte)
    let payload = `1${addrStr}02${valSwapped}${ETX}`;
    let sum = calculateChecksum(payload);

    return Buffer.from(STX + payload + sum, 'ascii');
}

// Đọc dữ liệu Word (Lệnh 0: Read)
// count: Số lượng thanh ghi D cần đọc (1 thanh ghi = 2 bytes)
function getReadWordFrame(address, count) {
    let baseAddr = 0x1000 + (parseInt(address) * 2);
    let addrStr = baseAddr.toString(16).toUpperCase().padStart(4, '0');
    let addrSwapped = addrStr.substring(2, 4) + addrStr.substring(0, 2); // Đảo byte địa chỉ

    // Đảm bảo số lượng byte cần đọc là chuỗi hex 2 ký tự. Đọc 1 thanh ghi D = 2 bytes, 2 thanh ghi D = 4 bytes.
    let bytesToRead = (count * 2).toString(16).toUpperCase().padStart(2, '0');

    let payload = `0${addrSwapped}${bytesToRead}${ETX}`; // Command '0' là Read
    let sum = calculateChecksum(payload);

    return Buffer.from(STX + payload + sum, 'ascii');
}

// ==========================================
// 4. LUỒNG NHẬN LỆNH TỪ UNITY VÀ GHI XUỐNG PLC
// ==========================================
// Unity cần gửi JSON dạng: 
// Bật Y0: {"action": "write_bit", "type": "Y", "address": 0, "value": 1}
// Ghi D100: {"action": "write_word", "type": "D", "address": 100, "value": 255}

// ==========================================
// 4. LUỒNG NHẬN LỆNH TỪ UNITY VÀ GHI XUỐNG PLC
// ==========================================
mqttClient.on('message', (topic, message) => {
    if (topic === TOPIC_CONTROL) {
        try {
            const cmd = JSON.parse(message.toString());
            let frame = null;

            if (cmd.action === 'write_bit') {
                frame = getForceBitFrame(cmd.type, cmd.address, cmd.value);
            } else if (cmd.action === 'write_word' && cmd.type === 'D') {
                frame = getWriteWordFrame(cmd.address, cmd.value);
            } else if (cmd.action === 'read_word' && cmd.type === 'D') {
                // BỔ SUNG: Xử lý lệnh đọc thanh ghi D
                const count = cmd.count || 1; // Mặc định đọc 1 thanh ghi nếu không truyền count
                frame = getReadWordFrame(cmd.address, count);
                console.log(`👉 Yêu cầu đọc ${count} thanh ghi từ D${cmd.address}`);
            }

            if (frame) {
                plcPort.write(frame, (err) => {
                    if (err) console.error('🔴 Lỗi gửi lệnh xuống Serial:', err.message);
                });
            }
        } catch (error) {
            console.error('🔴 Lỗi parse JSON:', error.message);
        }
    }
});

// ==========================================
// 5. LUỒNG ĐỌC PHẢN HỒI TỪ PLC
// ==========================================
// Biến đệm lưu trữ dữ liệu Serial trong trường hợp bị ngắt mảnh (phân mảnh gói tin)
let serialBuffer = '';

plcPort.on('data', (data) => {
    // Chuyển buffer thành chuỗi ASCII
    serialBuffer += data.toString('ascii');

    // Xử lý phản hồi ACK/NAK cho lệnh Write
    const hexRes = data.toString('hex').toUpperCase();
    if (hexRes.includes('06')) { // 0x06 = ACK
        console.log('✅ PLC xác nhận lệnh (ACK)');
        serialBuffer = ''; // Xóa buffer
        return;
    } else if (hexRes.includes('15')) { // 0x15 = NAK
        console.log('❌ PLC từ chối lệnh (NAK)');
        serialBuffer = ''; // Xóa buffer
        return;
    }

    // Xử lý phản hồi có chứa STX và ETX (dành cho lệnh Read)
    if (serialBuffer.includes(STX) && serialBuffer.includes(ETX)) {
        // Cắt lấy phần dữ liệu nằm giữa STX và ETX
        let stxIndex = serialBuffer.indexOf(STX);
        let etxIndex = serialBuffer.indexOf(ETX);

        let payload = serialBuffer.substring(stxIndex + 1, etxIndex);

        console.log(`📥 Raw Payload từ PLC: ${payload}`);

        // FX Protocol trả dữ liệu theo từng block 4 ký tự hex (2 byte) cho mỗi thanh ghi
        // Nó bị đảo byte (Little Endian) nên cần đảo ngược lại
        let values = [];
        for (let i = 0; i < payload.length; i += 4) {
            let chunk = payload.substring(i, i + 4);
            if (chunk.length === 4) {
                // Đảo byte: VD "3412" -> "1234"
                let correctHex = chunk.substring(2, 4) + chunk.substring(0, 2);
                let intValue = parseInt(correctHex, 16);
                values.push(intValue);
            }
        }

        // Bắn dữ liệu đã parse lên Unity qua MQTT
        let responseJson = {
            status: "Success",
            action: "read_response",
            data: values
        };
        mqttClient.publish(TOPIC_STATUS, JSON.stringify(responseJson));
        console.log('🚀 Đã gửi kết quả đọc lên Unity:', responseJson);

        // Reset buffer sau khi xử lý xong
        serialBuffer = '';
    }
});

// ==========================================
// 6. KHỞI ĐỘNG EXPRESS SERVER
// ==========================================
app.get('/', (req, res) => res.send('API Node.js <-> FX3U đang chạy.'));
app.listen(port, () => console.log(`🚀 Server chạy tại port ${port}`));