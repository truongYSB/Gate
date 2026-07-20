 const mqtt = require('mqtt');
const { SerialPort } = require('serialport');

const BROKER_URL = 'mqtt://broker.emqx.io:1883';
const mqttClient = mqtt.connect(BROKER_URL);
const PLC_PORT_NAME = 'COM4';

let pendingCommand = null; // Biến lưu lệnh đang chờ xác nhận từ PLC

const port = new SerialPort({ path: PLC_PORT_NAME, baudRate: 9600, dataBits: 7, parity: 'even', stopBits: 1 });

port.on('open', () => console.log(`✅ Đã mở cổng ${PLC_PORT_NAME}`));
port.on('error', (err) => console.log(`❌ Lỗi cổng COM:`, err.message));

port.on('data', (data) => {
    // data[0] là byte đầu tiên PLC trả về
    if (data[0] === 0x06) {
        // Khi PLC xác nhận, gửi chính xác cái lệnh đã lưu vào topic status
        console.log(`[<<] PLC xác nhận lệnh: ${pendingCommand}`);
        mqttClient.publish('iot/lab602/dieu_khien_plc/status', pendingCommand);
        pendingCommand = null; // Reset lại
    }
    else if (data[0] === 0x15) {
        console.log("[!!] PLC từ chối: Lỗi lệnh!");
        mqttClient.publish('iot/lab602/dieu_khien_plc/status', 'ERROR');
    }
});

// Hàm sinh mã Hex chuẩn cho Mitsubishi FX
function buildFrame(mNumber, isON) {
    let cmdChar = isON ? '7' : '8';
    // M2 = 0800 + 2 = 0802 -> Hex: 0802 -> Swapped: 0208
    // M3 = 0800 + 3 = 0803 -> Hex: 0803 -> Swapped: 0308
    let hexAddress = (0x0800 + parseInt(mNumber)).toString(16).padStart(4, '0').toUpperCase();
    let swappedAddress = hexAddress.substring(2, 4) + hexAddress.substring(0, 2);
    let payload = cmdChar + swappedAddress + '\x03';

    let sum = 0;
    for (let i = 0; i < payload.length; i++) sum += payload.charCodeAt(i);
    let checksum = sum.toString(16).slice(-2).toUpperCase();
    return Buffer.from('\x02' + payload + checksum, 'ascii');
}

mqttClient.on('connect', () => mqttClient.subscribe('iot/lab602/dieu_khien_plc/control'));

mqttClient.on('message', (topic, message) => {
    let msg = message.toString(); // VD: "M2_ON"
    if (msg === "COM_RELEASE") {
        if (port.isOpen) {
            port.close((err) => {
                if (err) console.log("Lỗi khi nhả cổng: ", err.message);
                else console.log("🛑 ĐÃ NHẢ CỔNG COM cho phần mềm khác!");
            });
        }
        return; // Dừng lại, không chạy tiếp đoạn code dịch mã hex bên dưới
    }

    if (msg === "COM_CLAIM") {
        if (!port.isOpen) {
            port.open((err) => {
                if (err) console.log("Lỗi khi chiếm cổng: ", err.message);
                else console.log("✅ ĐÃ CHIẾM LẠI CỔNG COM thành công!");
            });
        }
        return; // Dừng lại
    }
    pendingCommand = msg; // Ghi nhớ lệnh này là lệnh vừa được gửi
    let [mName, state] = msg.split('_');
    let mNumber = mName.replace('M', '');

    let frame = buildFrame(mNumber, state === "ON");
    port.write(frame);
    console.log(`[>>] Đã gửi lệnh ${msg} tới PLC`);
});