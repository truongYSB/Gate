const mqtt = require('mqtt');
const { SerialPort } = require('serialport');

const BROKER_URL = 'mqtt://172.18.0.252:1884';
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
        mqttClient.publish('iot/plc/status', pendingCommand);
        pendingCommand = null; // Reset lại
    }
    else if (data[0] === 0x15) {
        console.log("[!!] PLC từ chối: Lỗi lệnh!");
        mqttClient.publish('iot/plc/status', 'ERROR');
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

mqttClient.on('connect', () => mqttClient.subscribe('iot/plc/control'));

mqttClient.on('message', (topic, message) => {
    let msg = message.toString(); // VD: "M2_ON"
    pendingCommand = msg; // Ghi nhớ lệnh này là lệnh vừa được gửi
    let [mName, state] = msg.split('_');
    let mNumber = mName.replace('M', '');

    let frame = buildFrame(mNumber, state === "ON");
    port.write(frame);
    console.log(`[>>] Đã gửi lệnh ${msg} tới PLC`);
});


///

// const mqtt = require('mqtt');
// const { SerialPort } = require('serialport'); // Sử dụng thư viện cổng COM

// // 1. Cấu hình MQTT (Giữ nguyên của bạn)
// const BROKER_URL = 'mqtt://172.18.52.110:1884';
// const mqttClient = mqtt.connect(BROKER_URL);

// // 2. Cấu hình Cổng COM của cáp nạp PLC
// const PLC_PORT_NAME = 'COM3'; // ĐỔI TÊN NÀY THÀNH CỔNG COM BẠN TÌM THẤY Ở BƯỚC 2

// // Cấu hình BaudRate mặc định của cáp nạp dòng Mitsubishi FX
// const port = new SerialPort({
//     path: PLC_PORT_NAME,
//     baudRate: 9600,    
//     dataBits: 7,
//     parity: 'even',
//     stopBits: 1
// });

// port.on('open', () => {
//     console.log(`Đã mở cổng ${PLC_PORT_NAME} kết nối với PLC!`);
// });

// port.on('error', (err) => {
//     console.error("Lỗi cổng Serial: ", err.message);
// });

// // 3. Kết nối MQTT và Lắng nghe lệnh từ Unity
// mqttClient.on('connect', () => {
//     console.log("Gateway đã kết nối Mosquitto Broker!");
//     mqttClient.subscribe('iot/plc/light'); 
// });

// mqttClient.on('message', (topic, message) => {
//     if (topic === 'iot/plc/light') {
//         let command = message.toString();
        
//         // Chuỗi lệnh thô (Raw Byte) ép biến M0 bật/tắt theo chuẩn Mitsubishi:
//         // Lệnh ON:  STX(0x02) + '7' + Địa chỉ('0800') + ETX(0x03) + Checksum('02')
//         // Lệnh OFF: STX(0x02) + '8' + Địa chỉ('0800') + ETX(0x03) + Checksum('03')
        
//         let frame = (command === "ON") 
//                     ? Buffer.from('\x0270800\x0302', 'ascii') 
//                     : Buffer.from('\x0280800\x0303', 'ascii');
        
//         port.write(frame, (err) => {
//             if (err) {
//                 console.log('Lỗi ghi lệnh xuống PLC: ', err.message);
//             } else {
//                 console.log(`Đã gửi lệnh ${command} xuống relay M0 qua cáp nạp`);
//             }
//         });
//     }
// });