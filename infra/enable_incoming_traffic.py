import boto3
import psycopg2

# read config

import configparser

config = configparser.ConfigParser()
config.read_file(open('../dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_IAM_ROLE_NAME = config.get('IAM_ROLE', 'ROLE_NAME')
DWH_CLUSTER_TYPE = config.get('CLUSTER', 'DWH_CLUSTER_TYPE')
DWH_NODE_TYPE = config.get('CLUSTER', 'DWH_NODE_TYPE')
DWH_NUM_NODES = config.get('CLUSTER', 'DWH_NUM_NODES')
DWH_DB = config.get('CLUSTER', 'DB_NAME')
DWH_CLUSTER_IDENTIFIER = config.get('CLUSTER', 'HOST')
DWH_DB_USER = config.get('CLUSTER', 'DB_USER')
DWH_DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DWH_DB_PORT = config.get('CLUSTER', 'DB_PORT')

#EndPoint from CHECK_CLUSTER_STATUS step
DWH_ENDPOINT = 'dwhhost.cgzfv3rsniat.us-west-2.redshift.amazonaws.com'
#VPC_ID from CHECK_CLUSTER_STATUS step
VPC_ID = 'vpc-781d9500'

if DWH_ENDPOINT == '' :
    raise ValueError('Dont forget to configure DWH_ENDPOINT')

if VPC_ID == '' :
    raise ValueError('Dont forget to configure VPC_ID')


# create clients

ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                     )
# Enable access the cluster
try:
    vpc = ec2.Vpc(id=VPC_ID)
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_DB_PORT),
        ToPort=int(DWH_DB_PORT)
    )
except Exception as e:
    print(e)

#Make sure you can connect to the cluster
#%load_ext sql
#conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_DB_PORT, DWH_DB)
#print(conn_string)
#%sql $conn_string

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
print(conn)