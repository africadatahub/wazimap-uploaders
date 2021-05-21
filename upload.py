import argparse
from sources import owid
import logging
import requests

loglevels = [
    logging.WARNING,
    logging.INFO,
    logging.DEBUG,
]

def main():
    parser = argparse.ArgumentParser(description='Upload data to Africa Data Hub wazimap')
    parser.add_argument("--wazimap-endpoint", type=str, required=True)
    parser.add_argument("--wazimap-token", type=str, required=True)
    parser.add_argument("--dont-upload", action="store_true")
    parser.add_argument("-v", "--verbose", action="count", default=0)

    group = parser.add_argument_group('Datasets')
    group.add_argument("--owid-vaccines-distributed-to-date", metavar="DATASET_ID")

    args = parser.parse_args()

    logging.basicConfig(level=loglevels[args.verbose])

    if args.owid_vaccines_distributed_to_date:
        logging.info("Downloading OWID file")
        owid.download_file()

        logging.info("Processing vaccines_distributed_to_date")
        file_path = owid.write_vaccines_distributed_to_date()
        logging.info(f"File written to {file_path}")

        if args.dont_upload:
            logging.info(f"Not uploading {file_path} due to --dont-upload")
        else:
            logging.info(f"Uploading {file_path} to dataset id {args.owid_vaccines_distributed_to_date}")
            upload(
                args.wazimap_endpoint,
                args.wazimap_token,
                args.owid_vaccines_distributed_to_date,
                file_path
            )


def upload(wazimap_endpoint, wazimap_token, dataset_id, file_path):
    url = f"{wazimap_endpoint}/api/v1/datasets/{dataset_id}/upload/"

    headers = {'authorization': f"Token {wazimap_token}"}
    files = {'file': open(file_path, 'rb')}
    payload = {'update': True, 'overwrite': True}

    r = requests.post(url, headers=headers, data=payload, files=files)
    r.raise_for_status()


if __name__ == "__main__":
    main()
