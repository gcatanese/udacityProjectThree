import boto3
import psycopg2
import configparser

# read config
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

DWH_ENDPOINT = config.get('CLUSTER', 'HOST')
VPC_ID = config.get('IAM_ROLE', 'ARN')

# Ensure settings have been defined
if DWH_ENDPOINT == '' :
    raise ValueError('Dont forget to configure DWH_ENDPOINT')

if VPC_ID == '' :
    raise ValueError('Dont forget to configure VPC_ID')


# create client
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

# Verify DB connection is succesful
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
print(conn)