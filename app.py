import logging
import glob
import pandas as pd
from flask import Flask

from ETL import extract, transform, load

tmpfile = "temp.tmp"  # store all extracted data

logfile = "logfile.txt"  # all event logs will be stored

targetfile = "data/transformed_data.csv"  # transformed data is stored

transformation_list = []


def extractTransformAndLoad(file):
    for file in glob.glob(file):
        logging.info("Extract phase Started")
        data = extract(file)
        logging.info("Extract phase Ended")

        logging.info("Transform phase Started")
        transformed_data = transform(data, file)
        transformation_list.append(transformed_data)
        logging.info("Transform phase Ended")
    print(transformation_list[0].columns)
    df_cd = pd.merge(transformation_list[0], transformation_list[1], how='right', left_on='_id', right_on='app')

    df_cd_final = pd.merge(df_cd, transformation_list[2], how='left', left_on='_id_y', right_on='session')

    logging.info("Load phase")
    load(targetfile, df_cd_final)
    return 'ETL completed'


app = Flask(__name__)


@app.route("/run_etl")
def ETL():
    extractTransformAndLoad('data/*.json')


if __name__ == "__main__":
    app.run(host='0.0.0.0')
