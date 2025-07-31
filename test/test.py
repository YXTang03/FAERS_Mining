
from sshtunnel import SSHTunnelForwarder
import psycopg2
import pandas as pd
import math
from psycopg2.extras import execute_values


def get_drug_reaction(drugid, con):
    sql = (f"SELECT DISTINCT reactid FROM cdir_fo WHERE drugbank_id = '{drugid}';")
    drug_reaction = pd.read_sql(sql, con)
    #print(drug_reaction)
    reactids = drug_reaction.iloc[:, 0]
    return reactids


def count_drug(drugid, con):
    '''
    Figure out the value 'a+c'(the sum of recordings of all reactions 
    triggered by the given drug) in the Disproportional Method
    '''
    sql = (f"SELECT count FROM drugcount WHERE drugbank_id = '{drugid}';")
    drugcount = pd.read_sql(sql, con)
    return drugcount['count'].values[0]


def count_react(reactid, con):
    '''
    Figure out the value 'a+b'(the sum of recordings of the given reaction 
    ) in the Disproportional Method
    '''
    sql = (f"SELECT count FROM reactcount WHERE reactid = '{reactid}';")
    reactcount = pd.read_sql(sql, con)
    return reactcount['count'].values[0]


def count_a(drugid, reactid, con):
    '''
    Figure out the value 'a' (the count of recordings associating 
    with given drug and focusing adverse reaction) 
    in the Disproportional Method
    '''
    sql = (f"SELECT count(*) FROM cdir_fo WHERE drugbank_id = '{drugid}' AND reactid = '{reactid}';")
    reactcount = pd.read_sql(sql, con)
    return reactcount['count'].values[0]


def ror_ci(a, b, c, d, ror):
    ror_upper_ci_power = math.log(ror) + 1.96 * math.sqrt(1/a + 1/b + 1/c + 1/d)
    ror_lower_ci_power = math.log(ror) - 1.96 * math.sqrt(1/a + 1/b + 1/c + 1/d)
    ror_upper_ci = math.exp(ror_upper_ci_power)
    ror_lower_ci = math.exp(ror_lower_ci_power)
    return '%.2f' % ror_lower_ci, '%.2f' % ror_upper_ci


def prr_ci(a, b, c, d, prr):
    prr_upper_ci_power = math.log(prr) + 1.96 * math.sqrt(1/a + 1/b - 1/(a + c) - 1/(b + d))
    prr_lower_ci_power = math.log(prr) - 1.96 * math.sqrt(1/a + 1/b - 1/(a + c) - 1/(b + d))
    prr_upper_ci = math.exp(prr_upper_ci_power)
    prr_lower_ci = math.exp(prr_lower_ci_power)
    return '%.2f' % prr_lower_ci, '%.2f' % prr_upper_ci

def insert_batch_data(
        cursor, 
        table:str, 
        field:str,
        data_batch
):
    '''
    Insert values into table in batch
    '''
    insert_sql = f"""
    INSERT INTO {table} {field}
    VALUES %s
    """
    try:
        execute_values(cursor, insert_sql, data_batch, template=None, page_size=100)
        return True
    except Exception as e:
        print(f"Error When insert_batch_data: {e}")
        return False
    


def calculate(
        con, 
        table, field,
        batch_size = 1000
):
    total = 6981059
    sql = ("SELECT DISTINCT drugbank_id FROM drug21_24_map ORDER BY drugbank_id ASC limit 2")
    drugids = pd.read_sql(sql, con)


    cursor = con.cursor()
    data_batch = []
    total_inserted = 0

    try:
        for drugid in drugids['drugbank_id']:
            try:
                print(f"Processing: {drugid}")
                reactids = get_drug_reaction(drugid, con)
                ac = count_drug(drugid, con)
                for reactid in reactids:
                    ab = count_react(reactid, con)
                    a = count_a(drugid, reactid, con)
                    b = ab - a
                    c = ac - a
                    d = total - ab - ac + a
                    if b == 0 or c == 0:
                        a+=0.5
                        b+=0.5
                        c+=0.5
                        d+=0.5
                    ror = (a * d) / (b * c)
                    ror_lower_ci, ror_upper_ci = ror_ci(a, b, c, d, ror)

                    prr = a * (b + d)/(b * ac)
                    prr_lower_ci, prr_upper_ci = prr_ci(a, b, c, d, prr)

                    row_data = (
                        drugid, reactid, 
                        a, b, c, d, 
                        float('%.2f' % ror), ror_lower_ci, ror_upper_ci, 
                        float('%.2f' % prr), prr_lower_ci, prr_upper_ci
                    )
                    data_batch.append(row_data)
                    if len(data_batch) >= batch_size:
                        if insert_batch_data(cursor, table, field, data_batch):
                            con.commit()
                        else:
                            con.rollback()
                        data_batch = []


                    print(drugid, reactid, a, b, c, d, '%.2f' % ror, ror_lower_ci, ror_upper_ci, '%.2f' % prr, prr_lower_ci, prr_upper_ci)
                    #cur = conn.cursor()
                    #cur.execute(f"INSERT INTO faersmining (drugid, reactid, a, b, c, d, ror, ror_lower_ci, ror_upper_ci, prr, prr_lower_ci, prr_upper_ci) VALUES('{drugid}', '{reactid}', {a}, {b}, {c}, {d}, {ror}, {ror_lower_ci}, {ror_upper_ci}, {prr}, {prr_lower_ci}, {prr_upper_ci})")
            except Exception as e:
                    print(f"Error When {drugid}-{reactid}: {e}")
                    continue
            
        if data_batch:
            if insert_batch_data(cursor, table, field, data_batch):
                con.commit()
            else:
                con.rollback()
    
    except Exception as e:
        print(f"Error: 1{e}")
        con.rollback()
    finally:
        cursor.close()        

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
        db_host:str = '127.0.0.1', 
        batch_size = 1000
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
        batch_size = batch_size
    )


main(
    '10.12.0.8', 2043,
    'root','ynby20250625','localhost' ,5432,
    'FAERS20-24','postgres','woshipostgres','script_efficiency_test', 
    '(drugid, reactid, a, b, c, d, ror, ror_lower_ci, ror_upper_ci, prr, prr_lower_ci, prr_upper_ci)'
)


