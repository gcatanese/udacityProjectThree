import boto3
import json

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
DWH_CLUSTER_IDENTIFIER = config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB_USER = config.get('CLUSTER', 'DB_USER')
DWH_DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DWH_DB_PORT = config.get('CLUSTER', 'DB_PORT')


# create clients

ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                     )

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

iam = boto3.client('iam', aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                   )

redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

print("redshift", redshift)

# define role

try:
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Version': '2012-10-17',
             'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}]})
    )
except Exception as e:
    print(e)

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                       )['ResponseMetadata']['HTTPStatusCode']

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print('roleArn', roleArn)

# Fire up cluster!

try:
    response = redshift.create_cluster(
        # HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        #role(s)
        IamRoles=[roleArn]
    )
    print('response', response)
except Exception as e:
    print(e)

