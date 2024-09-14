import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

def extract_from_csv(fileToProcess):
    df = pd.read_csv(fileToProcess)
    return df

def extract_from_json(fileToProcess):
    df = pd.read_json(fileToProcess,lines=True)
    return df

def extract_from_xml(fileToProcess):
    df = pd.DataFrame(columns=['name', 'height', 'weight'])
    tree = ET.parse(source=fileToProcess)
    root = tree.getroot()
    for person in root:
        name = person.find('name').text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)
        df = pd.concat([df, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True) 
    return df


def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    for csvFile in glob.glob("./data/*.csv"):
        extracted_data = pd.concat([extracted_data,extract_from_csv(csvFile)], ignore_index=True)
    
    for jsonFile in glob.glob("./data/*.json"):
        extracted_data = pd.concat([extracted_data,extract_from_json(jsonFile)], ignore_index=True)

    for xmlFile in glob.glob("./data/*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlFile)], ignore_index=True)

    return extracted_data
    # print(extracted_data)

def transform(data):
    data['height'] = round(data['height'] * 0.0254, 2)
    data['weight'] = round(data['weight'] * 0.45359237,2)

    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_process(message):
    timestamp_format = '%y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + " | " + message + "\n")

def main():
    log_process('ETL Job Started.')

    log_process('Extraction Phase Started.')
    extracted_data = extract()
    log_process('Extraction Phase Completed.')

    log_process('Transformation Phase Started')
    transformed_data = transform(extracted_data)
    log_process('Transformation Phase Completed')

    log_process('Loading Phase Started')
    load_data(target_file=target_file, transformed_data=transformed_data)
    log_process('Loading Phase Completed')

    log_process('ETL Job Completed.')

if __name__ == '__main__':
    main()

