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
## Cho Server
mqtt://broker.emqx.io:1883
