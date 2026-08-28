const mqtt = require('mqtt');

// Cấu hình kết nối tới Mosquitto qua WebSocket
const BROKER_URL = 'ws://localhost:9001';
const TOPIC_CONTROL = 'iot/lab602/dieu_khien_plc/control';
const TOPIC_STATUS = 'iot/lab602/dieu_khien_plc/status';

const client = mqtt.connect(BROKER_URL);

// Hàm tạo độ trễ (Delay) - RẤT QUAN TRỌNG
// Vì giao tiếp Serial (RS422) tốc độ 9600 baud khá chậm, 
// nếu gửi lệnh liên tục không có độ trễ sẽ gây nghẽn và lỗi mất frame.
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

client.on('connect', async () => {
    console.log('✅ Đã kết nối tới MQTT Broker. Bắt đầu kịch bản test...');
    
    // Đăng ký nhận bản tin status để xem gateway và PLC phản hồi
    client.subscribe(TOPIC_STATUS);

    // Vòng lặp gửi lệnh liên tục
    while (true) {
        console.log('\n--- BẮT ĐẦU CHU KỲ TEST ---');

        // 1. Ghi lần lượt M0 -> M7 lên mức ON (1)
        console.log('\n[1] Bật lần lượt từ M0 đến M7:');
        for (let i = 0; i < 8; i++) {
            let cmdWriteM = { action: 'write_bit', type: 'M', address: i, value: 1 };
            client.publish(TOPIC_CONTROL, JSON.stringify(cmdWriteM));
            console.log(` -> Đã gửi lệnh BẬT M${i}`);
            await delay(300); // Nghỉ 300ms giữa mỗi lệnh
        }

        // 2. Đọc trạng thái M0 -> M7
        console.log('\n[2] Đọc trạng thái M0 -> M7:');
        let cmdReadM = { action: 'read_bit', type: 'M', address: 0, count: 8 };
        client.publish(TOPIC_CONTROL, JSON.stringify(cmdReadM));
        await delay(500);

        // 3. Đọc trạng thái Y0 -> Y7
        console.log('\n[3] Đọc trạng thái Y0 -> Y7:');
        let cmdReadY = { action: 'read_bit', type: 'Y', address: 0, count: 8 };
        client.publish(TOPIC_CONTROL, JSON.stringify(cmdReadY));
        await delay(500);

        // 4. Đọc giá trị thanh ghi D100
        console.log('\n[4] Đọc thanh ghi D100:');
        let cmdReadD = { action: 'read_word', type: 'D', address: 100, count: 1 };
        client.publish(TOPIC_CONTROL, JSON.stringify(cmdReadD));
        await delay(500);

        // 5. Ghi lần lượt M0 -> M7 xuống mức OFF (0) để test lại
        console.log('\n[5] Tắt lần lượt từ M0 đến M7:');
        for (let i = 0; i < 8; i++) {
            let cmdWriteMOff = { action: 'write_bit', type: 'M', address: i, value: 0 };
            client.publish(TOPIC_CONTROL, JSON.stringify(cmdWriteMOff));
            console.log(` -> Đã gửi lệnh TẮT M${i}`);
            await delay(300);
        }

        console.log('\n--- KẾT THÚC CHU KỲ. NGHỈ 3 GIÂY TRƯỚC KHI LẶP LẠI ---');
        await delay(3000);
    }
});

// Lắng nghe phản hồi từ Gateway
client.on('message', (topic, message) => {
    if (topic === TOPIC_STATUS) {
        console.log(`[GATEWAY PHẢN HỒI]: ${message.toString()}`);
    }
});

client.on('error', (err) => {
    console.error('🔴 Lỗi kết nối MQTT:', err);
});