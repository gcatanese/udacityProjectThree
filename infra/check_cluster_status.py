import boto3
import pandas as pd
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
DWH_CLUSTER_IDENTIFIER = config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB_USER = config.get('CLUSTER', 'DB_USER')
DWH_DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DWH_DB_PORT = config.get('CLUSTER', 'DB_PORT')

# create client
redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB_PORT, DWH_IAM_ROLE_NAME]
              })

def prettyRedshiftProps(props):
    """
    Collect cluster properties in a Dataframe
    :param props:
    :return:
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print('myClusterProps', myClusterProps)

prettyRedshiftProps(myClusterProps)

# Print Cluster Attributes (once available)
try:
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    VPC_ID = myClusterProps['VpcId']

    print("Cluster IS AVAILABLE")
    print("#######")
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    print("VPC_ID :: ", VPC_ID)
except Exception as e:
    print("#######")
    print("Cluster IS NOT AVAILABLE", e)


