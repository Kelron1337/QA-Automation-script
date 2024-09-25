import pymongo
import argparse
import csv
import pandas as pd

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]

mycol1 = mydb["Collection1"]
mycol2 = mydb["Collection2"]

def InsertCol1(file_path):
    list = []

    df = pd.read_csv(file_path, encoding_errors= 'ignore')
    df = df.drop_duplicates(subset = ['Test Case'], keep = 'first')
    for index, row in df.iterrows():
        if row['Test #'] == 0:
            continue
        document = {
            'test_number': row['Test #'],
            'date': row['Build #'],
            'category': row['Category'],
            'issue_desc': row['Test Case'],
            'expected': row['Expected Result'],
            'actual': row['Actual Result'],
            'repeatable': row['Repeatable?'],
            'blocker': row['Blocker?'],
            'reported_by': row['Test Owner']
        }
        list.append(document)

    if list:    
        mycol1.insert_many(list)

def InsertCol2(file_path):
    list = []

    # Read the Excel file directly into a pandas DataFrame
    df = pd.read_excel(file_path)
    df = df.dropna(how = 'all')
    df = df.drop_duplicates(subset = ['Test Case', 'Test Owner'], keep = 'first')
    for index, row in df.iterrows():
        # Construct the document to insert into MongoDB
        document = {
            'test_number': row['Test #'],
            'date': str(row['Build #']),
            'category': row['Category'],
            'issue_desc': row['Test Case'],
            'expected': row['Expected Result'],
            'actual': row['Actual Result'],
            'repeatable': row['Repeatable?'],
            'blocker': row['Blocker?'],
            'reported_by': row['Test Owner']
        }
        
        list.append(document)

    if list:    
       mycol2.insert_many(list)


def DBAnswers(file_name):
    answer1 = ['Answer1']
    headers = ['Test #', 'Build #', 'Category', 'Test Case', 'Expected Result', 'Actual Result', 'Repeatable?', 'Blocker?', 'Test Owner']
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        def write_docs_to_csv(collections, query, answer_label, file_writer):
            writer.writerow([answer_label])
            writer.writerow(headers)

             # Combine documents from both collections
            combined_documents = []
            seen_identifiers = set()  # Set to track unique identifiers
            for collection in collections:
                for document in collection.find(query):
                    # Create a unique identifier, could be a combination of fields
                    identifier = (document.get('test_number'), document.get('issue_desc'))
                    if identifier not in seen_identifiers:
                        seen_identifiers.add(identifier)
                        combined_documents.append(document)
                    # Retrieve and write the documents based on the query.
                    documents = list(collection.find(query))
            for document in documents:
                row = [
                    document.get('test_number', ''),
                    document.get('date', ''),
                    document.get('category', ''),
                    document.get('issue_desc', ''),
                    document.get('expected', ''),
                    document.get('actual', ''),
                    document.get('repeatable', ''),
                    document.get('blocker', ''),
                    document.get('reported_by', '')
                ]
                file_writer.writerow(row)
            file_writer.writerow([])  # Write an empty row after the documents for spacing.

        # Answer 1
        query1 = {'reported_by': 'Dawson Ford'}
        write_docs_to_csv([mycol1, mycol2], query1, 'Answer1', writer)

        query2 = {'repeatable': {'$in': ['yes','Yes']}}
        write_docs_to_csv([mycol1, mycol2], query2, 'Answer2', writer)

        query3 = {'blocker': {'$in': ['yes','Yes']}}
        write_docs_to_csv([mycol1, mycol2], query3, 'Answer3', writer)

        query4 = {'date': {'$in': ['03/19/24','03-19-24', '03/19/2024', '03-19-2024', '3/19/24','3-19-24', '3/19/2024', '3-19-2024', '2024-03-19 00:00:00']}}
        write_docs_to_csv([mycol1, mycol2], query4, 'Answer4', writer)

        print("Answer5")
        slist = []
    for document5 in mycol2.find():
        row = [
            document5.get('test_number', ''),
            document5.get('date', ''),
            document5.get('category', ''),
            document5.get('issue_desc', ''),
            document5.get('expected', ''),
            document5.get('actual', ''),
            document5.get('repeatable', ''),
            document5.get('blocker', ''),
            document5.get('reported_by', '')
        ]
        slist.append(row)
    if slist:  # Check if slist is not empty
        print(slist[0])  # First element
        print(slist[len(slist) // 2])  # Middle element; use // for integer division
        print(slist[-1])  # Last element; Python supports negative indexing
            

    

    
def ExportCSV(file_name):
    headers = ['Test #', 'Build #', 'Category', 'Test Case', 'Expected Result', 'Actual Result', 'Repeatable?', 'Blocker?', 'Test Owner']
    data = []
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for document in mycol2.find({'reported_by': {'$in': ['Kevin Chaja', 'kevin chaja']}}):
            row = [
                document.get('test_number'),
                document.get('date'),
                document.get('category'),
                document.get('issue_desc'),
                document.get('expected'),
                document.get('actual'),
                document.get('repeatable'),
                document.get('blocker'),
                document.get('reported_by')
            ]
            data.append(row)
        for row in data:
            writer.writerow(row)


    

parser = argparse.ArgumentParser(description='Process CSV/Excel file and insert into MongoDB.')
parser.add_argument('--file', type=str, help='File to process')
parser.add_argument('--col1', action='store_true', help='Insert into Collection1')
parser.add_argument('--col2', action='store_true', help='Insert into Collection2')
parser.add_argument('--dbanswers', action='store_true', help='Get DB Answers')
parser.add_argument('--exportcsv', action='store_true', help='export CSV')

args = parser.parse_args()

if args.col1:
    InsertCol1(args.file)
if args.col2:
    InsertCol2(args.file)
if args.dbanswers:
    DBAnswers(args.file)
if args.exportcsv:
    ExportCSV(args.file)


