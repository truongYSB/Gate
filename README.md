# tải thư viện

<!-- npm install mqtt mcprotocol --> // bỏ vì ở đây không dùng cap mạng
npm install mqtt serialport

# giải thích
Chuỗi lệnh thô (Raw Byte) ép biến M0 bật/tắt theo chuẩn Mitsubishi:
Lệnh ON:  STX + '7' + Địa chỉ M0 đảo byte ('0008') + ETX + Checksum ('02')
Lệnh OFF: STX + '8' + Địa chỉ M0 đảo byte ('0008') + ETX + Checksum ('03')

# Broker URL
## Test local
ws://localhost:9001/mqtt
## Cho Server (Cloud MQTT)
mqtt://broker.emqx.io:1883
## Cho Server (Mosquitto)
ws://localhost:9001

(Lưu ý: Đối với backend Node.js, bạn cũng có thể dùng giao thức MQTT thuần để tối ưu hiệu suất bằng cách trỏ vào cổng 1883: const BROKER_URL = 'mqtt://localhost:1883';. Cả hai đều chạy tốt với Mosquitto).
