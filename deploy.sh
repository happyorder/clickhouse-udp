export GOOS=linux;
export GOARCH=amd64;

echo "Building...";
go build -o ./build/clickhouse-udp;

echo "Copying to server...";
scp ./build/clickhouse-udp clickhouse:/home/ec2-user/clickhouse-udp.temp;

echo "Stopping...";
ssh clickhouse "sudo systemctl stop clickhouse-udp.service"

echo "Renaming";
ssh clickhouse "rm /home/ec2-user/clickhouse-udp; mv /home/ec2-user/clickhouse-udp.temp /home/ec2-user/clickhouse-udp"

echo "Starting";
ssh clickhouse "sudo systemctl restart clickhouse-udp.service"
