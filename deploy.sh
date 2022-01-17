export GOOS=linux;
export GOARCH=amd64;

echo "Building...";
go build -o ./build/clickhouse-udp;

echo "Killing old process...";
ssh questdb "/usr/sbin/pidof clickhouse-udp | xargs kill -9"

echo "Copying to server...";
scp ./build/clickhouse-udp questdb:/home/ec2-user;

echo "Starting...";
ssh questdb "nohup /home/ec2-user/clickhouse-udp > udp.out 2>&1 &"

