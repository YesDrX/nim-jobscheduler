import std/[asyncdispatch, os, times]

# Simulated race condition test
# This proves whether Monitor can call fetchRemoteResult
# while Engine is between fetch and DB update

var dbStatus = "Running" # Simulated DB status
var remoteFileExists = true # Simulated remote file
var localFileExists = false # Simulated local file

proc fetchRemoteResult() {.async.} =
  echo "[Engine T=", epochTime(), "] Starting fetch..."

  # Simulate SSH calls (blocking in real code with execCmdEx)
  await sleepAsync(100) # This simulates network delay

  if remoteFileExists:
    echo "[Engine T=", epochTime(), "] Remote file found, creating local and deleting remote"
    localFileExists = true
    remoteFileExists = false # Delete remote files
  else:
    echo "[Engine T=", epochTime(), "] ERROR: Remote file not found!"

  # NOT YET UPDATED DB - this is line 372 in engine.nim
  # There's a gap here before updateExecutionStatus

proc engineLoop() {.async.} =
  echo "[Engine T=", epochTime(), "] Detected process finished"
  await fetchRemoteResult()
  await sleepAsync(50) # Gap before DB update (represents lines 356-372)
  echo "[Engine T=", epochTime(), "] Updating DB status to Success"
  dbStatus = "Success"

proc monitorLoop() {.async.} =
  await sleepAsync(120) # Start checking after engine starts fetch
  echo "[Monitor T=", epochTime(), "] Checking DB..."
  if dbStatus == "Running":
    echo "[Monitor T=", epochTime(), "] Status is Running, checking remote..."
    if remoteFileExists:
      echo "[Monitor T=", epochTime(), "] Fetching remote files..."
    else:
      echo "[Monitor T=", epochTime(), "] ERROR: Remote files already deleted!"
  else:
    echo "[Monitor T=", epochTime(), "] Status is", dbStatus, ", skipping"

proc main() {.async.} =
  echo "=== Simulating Race Condition ==="
  echo "Starting at T=", epochTime()

  asyncCheck engineLoop()
  asyncCheck monitorLoop()

  await sleepAsync(500) # Wait for both to finish
  echo "=== Final State ==="
  echo "DB Status: ", dbStatus
  echo "Local file exists: ", localFileExists
  echo "Remote file exists: ", remoteFileExists

waitFor main()
