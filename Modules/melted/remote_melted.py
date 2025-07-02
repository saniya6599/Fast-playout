# import paramiko

# def list_remote_directory_items(host, port, username, password, directory_path):
#     try:
#         # Establish SSH connection
#         ssh = paramiko.SSHClient()
#         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#         ssh.connect(host, port=port, username=username, password=password,timeout=5)

#         # Execute command to list directory items
#         stdin, stdout, stderr = ssh.exec_command(f'ls -l {directory_path}')
#         directory_listing = stdout.read().decode().splitlines()
        
#         ssh.close()
#         return directory_listing
#     except Exception as e:
#         print(f"Error listing directory items: {e}")
#         return []

# def execute_remote_command(host, port, username, password, command):
#     try:

#         # Create an SSH client instance
#         ssh_client = paramiko.SSHClient()
#         ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
#         # Connect to the SSH server
#         ssh_client.connect(host, port=port, username=username, password=password)
        
#         # Execute the command
#         stdin, stdout, stderr = ssh_client.exec_command(command)
        
#         # Read and print the output
#         for line in stdout:
#             print(line.strip())
        
#         # Close the SSH connection
#         ssh_client.close()
#     except Exception as e:
#         print(f"Error: {e}")

# # Example usage
# host = '192.168.15.223'
# port = 22  # Default SSH port
# username = 'root'
# password = 'cloud10.0'
# directory_path = '/root/melted/20140121'
# command = './start-melted-1'

# # List directory items
# # items = list_remote_directory_items(host, port, username, password, directory_path)
# # print(f"Items in {directory_path}:")
# # for item in items:
# #     print(item)

# # Execute a command
# output, error = execute_remote_command("192.168.15.223", "22", "root", "cloud10.0", 'cd "/root/melted/20140121" && ./start-melted-1')
# print("Command Output:", output)
# print("Command Error:", error)
