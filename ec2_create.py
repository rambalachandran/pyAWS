import boto3
import configparser
from time import sleep
from datetime import datetime, timezone
import argparse

import mysql.connector
from mysql.connector import Error
from pprint import pprint

"""
TODO: Expand code to create multiple instances with separate tags
TODO: Create function to terminate instance
TODO: Automate instance termination.
"""

def create_instance(config):
    """
    boto3 based function to create a spot EC2 instance.
    """
    session = boto3.session.Session()
    client = session.client("ec2")
    req = client.request_spot_instances(
        InstanceCount=  1, # int(config.get("EC2", "instance_count")),
        Type=config.get("EC2", "instance_run_type"),
        InstanceInterruptionBehavior=config.get("EC2", "instance_interrupt_type"),
        LaunchSpecification={
            "SecurityGroups": [config.get("EC2", "security_group")],
            "ImageId": config.get("EC2", "ami"),
            "InstanceType": config.get("EC2", "instance_machine_type"),
            "KeyName": config.get("EC2", "key_pair"),
            "Placement": {
                "AvailabilityZone": config.get("EC2", "instance_availability_zone")
            },
            "SubnetId": config.get("EC2", "subnet_id"),
            "IamInstanceProfile": {"Arn": config.get("EC2", "iam_role")},
        },
        SpotPrice=config.get("EC2", "max_bid"),
        )   
    print(
        "Spot instance request success, status: " + req["SpotInstanceRequests"][0]["State"]
    )
    while True:
        sleep(1)
        current_req = client.describe_spot_instance_requests(
            SpotInstanceRequestIds=[
                req["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
            ]
        )
        if current_req["SpotInstanceRequests"][0]["State"] == "active":
            instance_id = current_req["SpotInstanceRequests"][0]["InstanceId"]
            create_time = current_req["SpotInstanceRequests"][0]["CreateTime"]
            print(
                "Instance allocated ,Id: ",
                instance_id
            )
            instance_state = client.describe_instances(
                InstanceIds=[instance_id]
            )["Reservations"][0]["Instances"][0]["State"]["Name"]
            client.create_tags(
                Resources=[instance_id],
                Tags=[
                    {"Key": "Name", "Value": config.get("EC2", "tag")},
                    {"Key": "CreatedBy", "Value": config.get("EC2", "created_by")},
                    {"Key": "Team", "Value": config.get("EC2", "team")},
                    {
                        "Key": "Service Type",
                        "Value": config.get("EC2", "service_type"),
                    },
                ],
            )
            return (instance_id, create_time, instance_state)
        print("Waiting...", sleep(10))


def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


def update_db(args, list_keys,list_val_type, list_vals):
    if all([args.host, args.user, args.db, args.table]):
        connection = create_connection(args.host, args.user, args.passwd, args.db)
        sql = " INSERT INTO " + args.table + " (" + ",".join(list_keys) + \
                ") VALUES ("+ ",".join(list_val_type) + ")"
        val = list_vals[0]
        cursor = connection.cursor()
        try:
            cursor.execute(sql, val)
            connection.commit()
            print("Query executed successfully")
        except Error as e:
            print(f"The error '{e}' occurred")
    else:
        print("Must provide -host, -user, -db -table  "
                "values to update database")

def remove_ignore_items(config, list_ignore_keys,
                        list_ignore_val):
    list_keys = [k for k,_ in config.items("EC2")]
    list_val_temp = [v for _,v in config.items("EC2")]
    list_ignore_index = []
    for cnt, val in enumerate(list_keys):
        if val in list_ignore_keys:
            list_ignore_index.append(cnt)

    for cnt, val in enumerate(list_val_temp):
        if val in list_ignore_val:            
            list_ignore_index.append(cnt)

    list_ignore_index = sorted(list(set(list_ignore_index)), reverse=True)
    for i in list_ignore_index:
        del list_keys[i]
        del list_val_temp[i]
    return (list_keys, list_val_temp)


def parse_cmd_args():
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('-config', action='store',
        type=str, default='config.cfg')
    my_parser.add_argument('-host', action='store',
        type=str, default='')
    my_parser.add_argument('-user', action='store',
        type=str, default='')
    my_parser.add_argument('-passwd', action='store',
        type=str, default='')
    my_parser.add_argument('-db', action='store',
        type=str, default='')
    my_parser.add_argument('-table', action='store',
        type=str, default='')    
    args = my_parser.parse_args()
    return args


if __name__ == "__main__":    
    args = parse_cmd_args()
    config = configparser.ConfigParser()
    config.read(args.config)
    list_ignore_val = ['']
    list_ignore_keys = ['instance_count']
    (list_keys_config, list_val_temp_config) = remove_ignore_items(
                                    config,
                                    list_ignore_keys,
                                    list_ignore_val)
    instance_count = int(config.get("EC2", "instance_count"))
    for cnt in range(0, instance_count):
        (instance_id, create_time, instance_state) = create_instance(config)
        create_time_str = create_time.strftime('%Y-%m-%d %H:%M:%S')
        print('instance_id: {}, create_time: {}, instance_state: {}'.format(
            instance_id, create_time.strftime('%Y-%m-%d %H:%M:%S'), instance_state))
        list_keys = ['instance_id', 'create_time', 'instance_state' ] + list_keys_config
        list_vals = []
        list_val_temp = tuple([instance_id, create_time_str, instance_state] + 
                                list_val_temp_config)
        list_vals.append(list_val_temp)
        list_val_type = ['%s']*len(list_keys)
        if args.host:
            update_db(args, list_keys,list_val_type, list_vals)
    

     # list_keys.pop(list_ignore_val)
    # del list_keys[list_ignore_index]
    # print("INDEX of Empty Values: {}".format(list_key_empty))

    # print("LIST VALS: {}".format(list_vals))

    # create_time = datetime(2020, 11, 22, 3, 48, 56)
    # create_time_str = create_time.strftime('%Y-%m-%d %H:%M:%S')
    # print("current time in str: ", create_time_str )
    # list_keys = ['instance_id', 'create_time', 'instance_state']
    # list_val_type = ['%s']*len(list_keys)
    # print("VAL TYPE: ", list_val_type)
    # list_vals = [('i-36f825h342d993s76', create_time.strftime('%Y-%m-%d %H:%M:%S'),
    #             'pending')]

    # instance_id, create_time, instance_state
    # execute_query(connection, insert_query)
 
    # for section in config["EC2"]:
    #     print("SECTION VAL: ", section)
    # print("Config File Read SUCCESS")
    # utc_dt = datetime.now(timezone.utc)
    # dt = utc_dt.astimezone()
    # print("CURRENT TIME: ", dt.strftime('%Y-%m-%d %H:%M:%S'))