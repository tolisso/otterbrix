pidof clickhouse > /dev/null && exit 1

./clickhouse server > /dev/null 2>&1 &

sleep 5

while true
do
    ./clickhouse client --query "SELECT 1" && break
    sleep 1
done

