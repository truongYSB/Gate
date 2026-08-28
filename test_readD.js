const mqtt = require('mqtt');

const BROKER_URL = 'ws://localhost:9001';
const TOPIC_CONTROL = 'iot/lab602/dieu_khien_plc/control';
const TOPIC_STATUS = 'iot/lab602/dieu_khien_plc/status';

const client = mqtt.connect(BROKER_URL);

// Hàm tạo trễ
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

client.on('connect', async () => {
    console.log('✅ Đã kết nối MQTT Broker. Bắt đầu đọc D100, D101 liên tục...');
    
    // Subscribe để nhận dữ liệu trả về từ Gateway
    client.subscribe(TOPIC_STATUS);

    // Vòng lặp gửi yêu cầu đọc liên tục
    while (true) {
        // Yêu cầu đọc 2 thanh ghi (count: 2) bắt đầu từ D100 -> Đọc D100 và D101
        let cmdRead = { 
            action: 'read_word', 
            type: 'D', 
            address: 100, 
            count: 2 
        };
        
        client.publish(TOPIC_CONTROL, JSON.stringify(cmdRead));
        
        // Nghỉ 1 giây (1000ms) giữa các lần đọc để tránh nghẽn cổng COM
        await delay(1000); 
    }
});

client.on('message', (topic, message) => {
    if (topic === TOPIC_STATUS) {
        console.log(`[${new Date().toLocaleTimeString()}] DỮ LIỆU TỪ PLC: ${message.toString()}`);
    }
});

client.on('error', (err) => {
    console.error('🔴 Lỗi kết nối MQTT:', err);
});