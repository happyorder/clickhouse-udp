export GOOS=linux;
export GOARCH=amd64;

echo "Building...";
go build -o ./build/clickhouse-udp;

echo "Copying to server...";
scp ./build/clickhouse-udp questdb:/home/ec2-user/clickhouse-udp.temp;

echo "Killing old process...";
ssh questdb "/usr/sbin/pidof clickhouse-udp | xargs kill -9"

echo "Renaming";
ssh questdb "rm /home/ec2-user/clickhouse-udp; mv /home/ec2-user/clickhouse-udp.temp /home/ec2-user/clickhouse-udp"

