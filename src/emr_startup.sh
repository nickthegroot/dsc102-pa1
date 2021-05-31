#! /bin/bash

aws emr create-cluster \
    --applications Name=Spark Name=Zeppelin \
    --ebs-root-volume-size 10 \
    --ec2-attributes '{"KeyName":"dsc102-pa1","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-cfa20385","EmrManagedSlaveSecurityGroup":"sg-09218e53e9e1ab2ad","EmrManagedMasterSecurityGroup":"sg-0582180ef22c38a8a"}' \
    --service-role EMR_DefaultRole \
    --enable-debugging \
    --log-uri 's3n://aws-logs-035170873046-us-west-2/elasticmapreduce/' \
    --name 'DSC102 PA1' \
    --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m4.large","Name":"Master Instance Group"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m4.large","Name":"Core Instance Group"}]' \
    --release-label emr-6.3.0 \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --region us-west-2