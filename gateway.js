const mqtt = require('mqtt');
const { SerialPort } = require('serialport');

const BROKER_URL = 'mqtt://broker.emqx.io:1883';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM4';

let pendingCommand = null; 
let rxBuffer = ''; // THÊM MỚI: Biến lưu trữ chuỗi dữ liệu dội về từ PLC

const port = new SerialPort({ path: PLC_PORT_NAME, baudRate: 9600, dataBits: 7, parity: 'even', stopBits: 1 });

port.on('open', () => console.log(`✅ Đã mở cổng ${PLC_PORT_NAME}`));
port.on('error', (err) => console.log(`❌ Lỗi cổng COM:`, err.message));

port.on('data', (data) => {
    // 1. XỬ LÝ LỆNH XÁC NHẬN (ACK/NAK) GHI ĐÈ
    if (data[0] === 0x06) {
        if (pendingCommand) {
            console.log(`[<<] PLC xác nhận lệnh: ${pendingCommand}`);
            mqttClient.publish('iot/lab602/dieu_khien_plc/status', pendingCommand);
            pendingCommand = null; 
        }
        return;
    }
    else if (data[0] === 0x15) {
        console.log("[!!] PLC từ chối: Lỗi lệnh!");
        mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'ERROR');
        return;
    }

    // 2. THÊM MỚI: XỬ LÝ DỮ LIỆU ĐỌC Y TRẢ VỀ TỪ PLC
    rxBuffer += data.toString('ascii'); // Ghép các byte lẻ tẻ thành chuỗi

    // Một khung truyền dữ liệu luôn bắt đầu bằng STX (0x02) và kết thúc bằng ETX (0x03)
    if (rxBuffer.includes('\x02') && rxBuffer.includes('\x03')) {
        let stxIdx = rxBuffer.indexOf('\x02');
        let etxIdx = rxBuffer.indexOf('\x03');
        
        if (etxIdx > stxIdx) {
            // Lấy phần dữ liệu nằm giữa STX và ETX (bỏ qua ký tự STX)
            let payload = rxBuffer.substring(stxIdx + 1, etxIdx);
            
            // Lệnh đọc 1 byte (8 bit) sẽ trả về 2 ký tự Hex. Ví dụ: "03" (tức là Y0 và Y1 đang bật)
            if (payload.length === 2) {
                let yByte = parseInt(payload, 16); // Chuyển chuỗi Hex thành số nguyên
                
                // Trích xuất từng bit: Bit 0 là Y0, Bit 1 là Y1
                let isY0_On = (yByte & 0x01) !== 0; 
                let isY1_On = (yByte & 0x02) !== 0; 
                
                // Đẩy trạng thái thực tế lên EMQX
                mqttClient.publish('iot/lab602/dieu_khien_plc/status', isY0_On ? "Y0_ON" : "Y0_OFF");
                mqttClient.publish('iot/lab602/dieu_khien_plc/status', isY1_On ? "Y1_ON" : "Y1_OFF");
            }
            // Reset buffer sau khi đã xử lý xong để đón gói tin tiếp theo
            rxBuffer = ''; 
        }
    }
});

// Hàm sinh mã Hex chuẩn cho Mitsubishi FX để Tắt/Bật M
function buildFrame(mNumber, isON) {
    let cmdChar = isON ? '7' : '8';
    let hexAddress = (0x0800 + parseInt(mNumber)).toString(16).padStart(4, '0').toUpperCase();
    let swappedAddress = hexAddress.substring(2, 4) + hexAddress.substring(0, 2);
    let payload = cmdChar + swappedAddress + '\x03';

    let sum = 0;
    for (let i = 0; i < payload.length; i++) sum += payload.charCodeAt(i);
    let checksum = sum.toString(16).slice(-2).toUpperCase();
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

// THÊM MỚI: Hàm sinh mã Hex để ĐỌC trạng thái vùng Y0-Y7
function buildReadYFrame() {
    // Lệnh '0': Đọc bộ nhớ
    // Địa chỉ Y0-Y7 là 00A0
    // Đọc 1 byte (01)
    let payload = '0' + '00A0' + '01' + '\x03';
    let sum = 0;
    for (let i = 0; i < payload.length; i++) sum += payload.charCodeAt(i);
    let checksum = sum.toString(16).slice(-2).toUpperCase();
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

// THÊM MỚI: Tạo vòng lặp để Node.js tự động hỏi PLC mỗi giây
setInterval(() => {
    // Chỉ "hỏi" khi cổng COM đang mở và không có lệnh bật/tắt (M) nào đang chờ xử lý
    if (port.isOpen && !pendingCommand) {
        let readFrame = buildReadYFrame();
        port.write(readFrame);
    }
}, 1000); 

mqttClient.on('connect', () => mqttClient.subscribe('iot/lab602/dieu_khien_plc/control'));

mqttClient.on('message', (topic, message) => {
    let msg = message.toString(); 
    if (msg === "COM_RELEASE") {
        if (port.isOpen) {
            port.close((err) => {
                if (err) console.log("Lỗi khi nhả cổng: ", err.message);
                else console.log("🛑 ĐÃ NHẢ CỔNG COM cho phần mềm khác!");
            });
        }
        return; 
    }

    if (msg === "COM_CLAIM") {
        if (!port.isOpen) {
            port.open((err) => {
                if (err) console.log("Lỗi khi chiếm cổng: ", err.message);
                else console.log("✅ ĐÃ CHIẾM LẠI CỔNG COM thành công!");
            });
        }
        return; 
    }
    pendingCommand = msg; 
    let [mName, state] = msg.split('_');
    let mNumber = mName.replace('M', '');

    let frame = buildFrame(mNumber, state === "ON");
    port.write(frame);
    console.log(`[>>] Đã gửi lệnh ${msg} tới PLC`);
});