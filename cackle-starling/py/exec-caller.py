import subprocess
import socket
import sys
import select

def lambda_handler(event, context):
    print(event)

    p = subprocess.Popen(event.split(" ")+["2>&1"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ipaddress = socket.gethostbyname(socket.gethostname())
    last = ""
    #streamdata = p.communicate();
    
    for line in p.stdout:
        line = line.decode("UTF-8")
        print(line)
        last = line
    p.wait()
    print(p.returncode)
    print(ipaddress + " " + context.log_stream_name + " " + last[:-1])
    
    return ipaddress + " " + context.log_stream_name + " " + last[:-1]
