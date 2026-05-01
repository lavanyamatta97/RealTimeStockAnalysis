import extract
import config

def main():
    response = extract.connect_to_api()
    records = extract.extract_json(response)
    print(records)
main()