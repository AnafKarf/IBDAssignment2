#!/bin/bash

service ssh restart

bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -o .venv.tar.gz

# bash prepare_data.sh
bash set_cassandra.sh
# Run the indexer: first on collected dataset, then on additional file
bash index.sh data
bash index.sh 123_cute_cats.txt

# Test on three queries
bash search.sh "famous scientist"
bash search.sh "cute cats"
bash search.sh "a spatial Fourier transform"
