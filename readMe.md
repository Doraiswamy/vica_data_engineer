Instructions to run the application

1. Make sure docker is installed in the system intended to run

2. Navigate to vica_data_engineer folder

3. Excecute the command 'docker build -t vica_data_engineer .' where vica_data_engineer is the name of the image

4. Execute the command 'docker run -p 8000:5000 -it vica_data_engineer' and access the application in port 8000

5. To execute the ETL function 'http:localhost:8000/run_etl' must be executed

6. A csv file will be created with the data pipeline executed with the name 'transformed_data.csv' 