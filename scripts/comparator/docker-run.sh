# Start the first process
./m3comparator -D
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start m3comparator: $status"
  exit $status
fi

# Start the second process
./m3query -f $1 -D
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start m3query: $status"
  exit $status
fi

while sleep 60; do
  ps aux |grep m3comparator |grep -q -v grep
  PROCESS_1_STATUS=$?
  ps aux |grep m3query |grep -q -v grep
  PROCESS_2_STATUS=$?
  # If the greps above find anything, they exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $PROCESS_1_STATUS -ne 0 -o $PROCESS_2_STATUS -ne 0 ]; then
    echo "One of the processes has already exited."
    exit 1
  fi
done
