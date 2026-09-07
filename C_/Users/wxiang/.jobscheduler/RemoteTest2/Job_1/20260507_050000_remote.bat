@echo off
call scp -q -o StrictHostKeyChecking=no -o BatchMode=yes -P 22 -i "C:\Users\wxiang\.ssh\id_rsa" "C_\Users\wxiang\.jobscheduler\RemoteTest2\Job_1\20260507_050000_cmd.sh" wxiang@192.168.1.224:/tmp/RemoteTest2_Job_1_20260507_050000.sh
call ssh -tt -o ServerAliveInterval=60 -o StrictHostKeyChecking=no -o BatchMode=yes -p 22 -i "C:\Users\wxiang\.ssh\id_rsa" wxiang@192.168.1.224 "bash /tmp/RemoteTest2_Job_1_20260507_050000.sh" > "C_\Users\wxiang\.jobscheduler\RemoteTest2\Job_1\20260507_050000.log" 2>&1
echo %ERRORLEVEL% > "C_\Users\wxiang\.jobscheduler\RemoteTest2\Job_1\20260507_050000_remote.bat.exit"
