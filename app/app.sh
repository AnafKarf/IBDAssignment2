service ssh restart

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh

bash set_cassandra.sh

# Run the indexer
bash index.sh hdfs://index/data

# Run the ranker
bash search.sh "this is a query!"