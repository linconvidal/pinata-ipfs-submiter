import os, logging, requests, argparse, csv

def scan_dir(path, file_extension):
    """Scan folder and get all filenames of `.jpg` files."""
    return [filename for filename in os.listdir(path) if file_extension in filename]


def change_filename(prefix, filename, filename_prefix, file_extension):
    """Change filename"""
    logging.debug(f"Renaming file'{filename}'...")
    new_filename = "{}{:05d}{}".format(
        filename_prefix, 
        int(filename.split(".")[0]), 
        file_extension
        )
    os.rename(os.path.join(prefix, filename), os.path.join(prefix, new_filename))
    logging.info(f"File '{filename}' renamed to '{new_filename}'")
    
    return new_filename


def submit_to_pinata(prefix, fnm):
    """Submit image to ipfs 
      Para cara Feijão, 
        submete pro pinata e obtém o hash 
        armazena hash na tabela
    """
    
    api_key = str(os.getenv('PINATA_API_KEY'))
    secret = str(os.getenv('PINATA_SECRET_API_KEY'))
    
    headers = {
        'pinata_api_key': api_key, 
        'pinata_secret_api_key': secret,  
    }

    url = "https://api.pinata.cloud/pinning/pinFileToIPFS"
    
    with open(os.path.join(prefix, fnm), 'rb') as f:
        files = {'file': f}

        r = requests.post(url, files=files, headers=headers)
        logging.debug(f"Called {url} with {fnm} and got response code {r.status_code}")
    
    if r.status_code == 200:
        img_hash = r.json().get("IpfsHash")
        logging.info(f"File '{fnm}' was pinned: '{img_hash}'")
        
        return img_hash
        
    else:
        logging.error(f"Couldn't pin! Response code: {r.status_code}")
        logging.debug(r.text)


def main():
    logging.basicConfig(level=logging.DEBUG)


    parser = argparse.ArgumentParser(
        description="""
        Performs the final steps of data preparation and generates
        the data set specification files.
        """
    )

    parser.add_argument(
        "--dir_prefix",
        type=str,
        help="""
        Prefix folder of NFT images.
        """
    )

    parser.add_argument(
        "--filename_prefix",
        type=str,
        help="""
        Prefix for filename.
        """
    )

    parser.add_argument(
        "--file_extension",
        type=str,
        help="""
        Extension for scan images in folder.
        """
    )

    args, _ = parser.parse_known_args()

    path = args.dir_prefix
    fnm_prefix = args.filename_prefix
    file_ext = args.file_extension

    fnms = scan_dir(path, file_ext)
    logging.info("A total of {} files was founded.".format(len(fnms)))

    with open('output.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=';')

        for fnm in fnms:
            new_fnm = change_filename(path, fnm, fnm_prefix, file_ext)
            img_hash = submit_to_pinata(path, new_fnm)
            
            if img_hash:
                ipfs_url =  os.getenv('IPFS_GATEWAY') + img_hash
                csv_writer.writerow(
                    [new_fnm.replace(file_ext,""), img_hash, ipfs_url]
                    )

if __name__ == '__main__':
    main()
            
            
            
            
            
            
            
            
            
