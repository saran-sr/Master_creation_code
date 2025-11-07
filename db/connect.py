import mysql.connector
def get_database_connection(server):
    """Establish database connection using the specific logic"""
    if server=="anton":   
        conx=mysql.connector.connect(host='takeleap.in',user='seekright',password='Takeleap@123',port='3307')
    elif server=="production":
        conx= mysql.connector.connect(host='seekright-db.ce3lsmnwzkln.ap-south-1.rds.amazonaws.com',user='admin',password='BXWUCSpjRxEqzxXYTF9e',port='3306')
    elif server=="enigma":
        conx = mysql.connector.connect(host='mariadb.seekright.ai', user='enigma', password='Takeleap@123', port='3307')
    
    return conx