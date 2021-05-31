#! /bin/bash

aws ec2 run-instances \
    --image-id ami-0cf6f5c8a62fa5da6 \
    --instance-type t3.large \
    --key-name dsc102-pa1 \
    --instance-market-options "{ \"MarketType\": \"spot\" }" \