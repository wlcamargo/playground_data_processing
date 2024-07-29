import socket

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        s.connect(("8.8.8.8", 80))
        
        ip = s.getsockname()[0]
        
        s.close()
        
        return ip
    except Exception as e:
        return str(e)

local_ip = get_local_ip()

content = f"host = '{local_ip}'\n"

file_path = 'configs/host.py'

with open(file_path, 'w') as file:
    file.write(content)

print(f"Host IP saved to {file_path} as 'host = {local_ip}'")
