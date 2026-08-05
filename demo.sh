#!/bin/bash

# Remove old AOF
rm -f appendonly.aof

echo "🚀 Starting server..."
./server_bin > server.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "💾 Setting two keys: 'name' and 'project'"
printf "*3\r\n\$3\r\nSET\r\n\$4\r\nname\r\n\$11\r\nantigravity\r\n" | timeout 0.2 nc localhost 7378
printf "*3\r\n\$3\r\nSET\r\n\$7\r\nproject\r\n\$16\r\nredis-clone-demo\r\n" | timeout 0.2 nc localhost 7378

echo "📝 Triggering BGREWRITE to save memory to appendonly.aof..."
printf "*1\r\n\$9\r\nBGREWRITE\r\n" | timeout 0.2 nc localhost 7378
sleep 1 

echo "🛑 Stopping the server..."
kill $SERVER_PID
sleep 1

echo "----------------------------------------"
echo "📄 Let's look at the generated AOF file:"
cat appendonly.aof
echo -e "\n----------------------------------------"

echo "🚀 Starting server AGAIN (it should load the AOF now)..."
./server_bin > server2.log 2>&1 &
SERVER_PID2=$!
sleep 1

echo "🔍 Getting the keys back (they should survive the restart!):"
echo -n "GET name -> "
printf "*2\r\n\$3\r\nGET\r\n\$4\r\nname\r\n" | timeout 0.2 nc localhost 7378
echo ""

echo -n "GET project -> "
printf "*2\r\n\$3\r\nGET\r\n\$7\r\nproject\r\n" | timeout 0.2 nc localhost 7378
echo ""

echo "🧹 Cleaning up..."
kill $SERVER_PID2
echo "✅ Demo Complete!"
