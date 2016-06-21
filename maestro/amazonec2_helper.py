# -*- coding: utf-8 -*-
# @Author: thomasopsomer
from collections import defaultdict
from boto3.session import Session
from datetime import datetime, timedelta


def get_image_id(ec2, image_name):
    """
    Given an AMI name, return the id of this AMI
    """
    image = list(ec2.images.filter(Owners=['self'],
                                   Filters=[{'Name': 'name', 'Values': [image_name]}]))
    if len(image) > 0:
        return image[0].id
    else:
        print("requested AMI not found")


# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(ec2, name):
    """
    Look for a security defined by the requested `name`
    If can't find it, it creates a new security group with
        GroupName=`name`
        Description="Spark EC2 group"

    RETURN:
        an instnace of ec2.security_group
    """
    groups = list(ec2.security_groups.filter(Filters=[{'Name': 'group-name',
                                                       'Values': [name]}
                                                      ]))
    if len(groups) > 0:
        return groups[0]
    else:
        print("Creating security group %s" % name)
        return ec2.create_security_group(GroupName=name, Description="Spark EC2 group")


def configure_security_group(security_group, authorized_group_name=[],
                             authorized_address="0.0.0.0/0", ports=None):
    """
    Configure a `security group` for usage in cluster application
    (Spark, Mesos ...)

    ARGS
    ----
        authorized_address: allows to pass special ip address to authorize
            TODO : default should be the IP of the computer that execute
                   this class
        ports: ports related to authorized_address
        authorized_group_name: authorize all instances of a security_group
    """
    if security_group.ip_permissions == []:
        # authorize other group for isntance master and slave groups
        for group_name in authorized_group_name:
            security_group.authorize_ingress(SourceSecurityGroupName=group_name)

        if ports is None:
            ports = [(22, 22), (8080, 8081), (19999, 19999), (50030, 50030),
                     (50070, 50070), (60070, 60070), (4040, 4045), (8787, 8787), (5050, 6060),
                     (50075, 50075), (50060, 50060), (60075, 60075), (60060, 60060)]
        for fromPort, toPort in ports:
            security_group.authorize_ingress(
                IpProtocol='tcp',
                FromPort=fromPort,
                ToPort=toPort,
                CidrIp=authorized_address)
        # security_group.authorize_ingress(
        #         IpProtocol='http',
        #         FromPort=1,
        #         ToPort=65535,
        #         CidrIp=authorized_address)


def find_cheapest_zone(instance_type, region_name, interval=1):
    """
    Find the cheapest zone over the last minute

    ARGS
    ----
        - ec2_client: a boto3 ec2_client
        - instance_type: type of instance you want as string
        - region_name: name of your region string
        - interval: size of interval for price avering

    RETURN
    ------
        tuple("cheapest_zone_name", average_price_over_interval)

    """
    ec2_client = Session(region_name=region_name).client("ec2")
    start_time = (datetime.now() - timedelta(seconds=1)).isoformat()
    end_time = datetime.now().isoformat()
    all_zones = get_zone_from_region(ec2_client, region_name)
    response = ec2_client.describe_spot_price_history(
        StartTime=start_time,
        EndTime=end_time,
        InstanceTypes=[instance_type],
    )
    prices = defaultdict(list)
    for resp in response["SpotPriceHistory"][0:len(all_zones)]:
        prices[resp["AvailabilityZone"]].append(float(resp["SpotPrice"]))
    avg_prices = map(lambda x: (x[0], mean(x[1])), prices.iteritems())
    return avg_prices[argmin(avg_prices)]


def get_zone_from_region(ec2_client, region_name):
    z = ec2_client.describe_availability_zones(
        Filters=[{'Name': 'region-name', 'Values': [region_name]}])
    return [zone["ZoneName"] for zone in z["AvailabilityZones"]]


def mktag(val):
    return [{'Key': 'Name', 'Value': val}]


def argmin(iterable):
    """
        argmax function on a iterable
    """
    return min(enumerate(iterable), key=lambda x: x[1])[0]


def mean(array):
    return sum(array) / len(array)
