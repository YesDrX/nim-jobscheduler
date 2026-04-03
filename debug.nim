import times, os, osproc
import ./src/utils

proc main() =
    let filename = "/home/wxiang/.jobscheduler/Test/Job_1/20260403_115555.log"
    # var f: File
    # discard f.open(filename, fmRead)
    # defer: f.close()
    # echo f.getFileSize()
    # echo readFile(filename, 200000)
    echo readFile(filename).len

main()
