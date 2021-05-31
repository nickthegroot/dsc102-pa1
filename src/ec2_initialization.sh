# Prereqs
sudo amazon-linux-extras enable python3.8

sudo yum -y update
sudo yum -y install git python3.8
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
alias poetry="$HOME/.poetry/bin/poetry"


git clone https://github.com/nickthegroot/dsc102-pa1.git pa1
cd pa1

poetry install --no-dev

# PASTE AWS CREDENTIALS

poetry run python src/feature_prep.py "s3://dsc102-nickdegroot-scratch/data/historical_data_2017*.txt" "s3://dsc102-nickdegroot-scratch/2017-features.parquet"