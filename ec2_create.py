import boto3
import configparser
from time import sleep
from datetime import datetime, timezone
import argparse
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
        InstanceCount=int(config.get("EC2", "instance_count")),
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
            print(
                "Instance allocated ,Id: ",
                current_req["SpotInstanceRequests"][0]["InstanceId"],
            )
            instance = client.describe_instances(
                InstanceIds=[current_req["SpotInstanceRequests"][0]["InstanceId"]]
            )["Reservations"][0]["Instances"][0]
            client.create_tags(
                Resources=[current_req["SpotInstanceRequests"][0]["InstanceId"]],
                Tags=[
                    {"Key": "Name", "Value": config.get("EC2", "tag")},
                    {"Key": "CreatedBy", "Value": config.get("EC2", "created_by")},
                    {"Key": "Team", "Value": config.get("EC2", "team")},
                    {
                        "Key": "Application",
                        "Value": config.get("EC2", "application"),
                    },
                ],
            )
            return instance
        print("Waiting...", sleep(10))

if __name__ == "__main__":
    utc_dt = datetime.now(timezone.utc)
    dt = utc_dt.astimezone()
    print("CURRENT TIME: ", dt)
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('--config', action='store',
        type=str, required=True)
    args = my_parser.parse_args()
    config = configparser.ConfigParser()
    config.read(args.config)
    print("Config File Read SUCCESS")
    instance = create_instance(config)
