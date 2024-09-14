import glob
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET

log_file = 'log.txt'
target_file = 'transformed_data.csv'

def extractFromCSV(fileToProcess):
    data = pd.read_csv(fileToProcess)
    return data

def extractFromJSON(fileToProcess):
    data = pd.read_json(fileToProcess, lines=True)
    return data

def extractFromXML(fileToProcess):
    data = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    tree = ET.parse(fileToProcess)
    root = tree.getroot()
    for row in root:
        car_model = row.find('car_model').text
        year_of_manufacture = row.find('year_of_manufacture').text
        price = float(row.find('price').text)
        fuel = row.find('fuel').text
        data = pd.concat([data, pd.DataFrame([
            {
                'car_model' : car_model,
                'year_of_manufacture' : year_of_manufacture,
                'price' : price,
                'fuel' : fuel,
            }
        ])], ignore_index=True)
    return data

def extract():

    data = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])

    for csvFile in glob.glob("./data/*.csv"):
        data = pd.concat([data, extractFromCSV(csvFile)])

    for JSONFile in glob.glob("./data/*.json"):
        data = pd.concat([data, extractFromJSON(JSONFile)])

    for XMLFile in glob.glob("./data/*.xml"):
        data = pd.concat([data, extractFromXML(XMLFile)])

    return data

def transform(extracted_data):
    extracted_data['price'] = round(extracted_data['price'],2)
    return extracted_data

def load(transformed_data, targetFile):
    transformed_data.to_csv(targetFile)

def logger(message):
    TS_format = '%y-%h-%d:%H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(TS_format)

    with open(log_file,'+a') as f:
        f.write(timestamp + " | " + message + "\n")

def main():
    logger('ETL Job Started.')

    logger('Extraction Phase Started')
    extracted_data = extract()
    logger('Extraction Phase Completed')

    logger('Transformation Phase Started')
    transformed_data = transform(extracted_data=extracted_data)
    logger('Transformation Phase Completed')

    logger('Loading Phase Started')
    load(transformed_data=transformed_data,targetFile=target_file)
    logger('Loading Phase Completed')

    logger('ETL Job Completed.')


if __name__ == '__main__':
    main()
