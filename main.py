from sshtunnel import SSHTunnelForwarder
import psycopg2
from .utils import calculate


def main(
        ssh_address:str, 
        ssh_port:int, 
        ssh_username:str, 
        ssh_password:str, 
        remote_address:str, 
        remote_port:int, 
        database_name:str, 
        db_user:str, 
        db_password:str, 
        table:str, 
        field:str,
        error_log,         
        db_host:str = '127.0.0.1', 
        batch_size = 1000, 
        
):
    server = SSHTunnelForwarder(
    ssh_address_or_host=(ssh_address, ssh_port),
    ssh_username=ssh_username, 
    ssh_password = ssh_password, 
 
    
    remote_bind_address=(remote_address , remote_port)) 
    server.start()
    conn = psycopg2.connect(database = database_name, 
                                user = db_user,     
                                password = db_password, 
                                host = db_host,   
                                port = server.local_bind_port)
    calculate(
        con = conn, 
        table = table, 
        field = field,
        batch_size = batch_size, 
        error_log=error_log
    )
