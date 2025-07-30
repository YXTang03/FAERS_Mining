from sshtunnel import SSHTunnelForwarder
import psycopg2
import pandas as pd




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


def calculate(con):
    total = 6981059
    sql = ("SELECT DISTINCT drugbank_id FROM drug21_24_map ORDER BY drugbank_id ASC limit 4")
    drugids = pd.read_sql(sql, con)
    for drugid in drugids['drugbank_id']:
        print(drugid)
        reactids = get_drug_reaction(drugid)
        ac = count_drug(drugid)
        for reactid in reactids:
            ab = count_react(reactid)
            a = count_a(drugid, reactid)
            b = ab - a
            c = ac - a
            d = total - ab - ac + a
            if b == 0 or c == 0:
                a+=0.5
                b+=0.5
                c+=0.5
                d+=0.5
            ror = (a * d) / (b * c)
            prr = 
            print(f'reaction_id: {reactid}, a: {a}, b: {b}, c: {c}, d: {d}, ror: {ror}')