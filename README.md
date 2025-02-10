This repo was used for testing airflow with wsl, cause's i'm a windows user. When you want to try airflow-apache on windows, this is "how to" using airflow on windows :
Running Airflow On Windows :
- Download and Install WSL
- Donwload conda on wsl
	with command -> history | grep Miniconda -> and then copy the repo of conda -> wget <repo 'sourcelink'>
- Set up env -> command -> conda create -n <name 'of env> <python 'version>
	
- Activate the env -> command -> conda activate <name 'of env>
- Install apache airflow -> command -> pip install apache-airflow
- Set the airflow home -> command -> vim ~/.bashrc -> <set 'airflow-home in vim> export AIRFLOW_HOME=~/<name> (up to us for the placement)
- Init db -> command -> airflow db init
- Create user -> command -> airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
	set password...
- Open new wsl terminal
- Activate env
- Run airflow scheduler -> command -> airflow scheduler
( 3 instances of airflow : db (must init db first), scheduler, webserver)
- Activate webserver -> command -> airflow webserver
- Access localhost::8080 to see Airflow UI in web

Create DAG:
- create folder -> command -> mkdir <folder 'name>
- enter the folder -> command -> cd <folder 'name>
- open the code editor -> command -> code .
