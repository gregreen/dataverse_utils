#!/usr/bin/env python

from __future__ import print_function, division

import requests
import json
import os.path
from argparse import ArgumentParser
from requests.exceptions import HTTPError, ConnectionError, Timeout
from time import time


def main():
    parser = ArgumentParser(
        description='Upload a file to a Dataverse.',
        add_help=True
    )
    parser.add_argument(
        'input',
        type=str,
        nargs='+',
        help='File(s) to upload.'
    )
    parser.add_argument(
        '--api-key',
        type=str,
        required=True,
        help='API key (used for authentication).'
    )
    parser.add_argument(
        '--doi',
        type=str,
        required=True,
        help='DOI of dataset.'
    )
    parser.add_argument(
        '--server',
        type=str,
        default='https://dataverse.harvard.edu',
        help='URL of dataverse server (default: https://dataverse.harvard.edu).'
    )
    parser.add_argument(
        '--mime',
        type=str,
        help='MIME type (default: guess type).'
    )
    parser.add_argument(
        '--timeout',
        type=float,
        default=999.,
        help='Timeout (in seconds) for server response (default: 999).'
    )
    parser.add_argument(
        '--checksum',
        type=str,
        help='File to write checksums to.'
    )
    parser.add_argument(
        '--preload',
        action='store_true',
        help='Load the file in its entirety before uploading.'
    )
    parser.add_argument(
        '--ignore-failure',
        action='store_true',
        help='Proceed to next file on failure.'
    )
    args = parser.parse_args()
    
    # URL to POST file to
    url = r'{:s}/api/datasets/:persistentId/add?persistentId={:s}&key={:s}'.format(
        args.server,
        args.doi,
        args.api_key
    )
    print('url = {:s}'.format(url))
        
    for i,fn in enumerate(args.input):
        print('File {:d} of {:d}:'.format(i+1, len(args.input)))
        
        # Get name of file
        name = os.path.basename(fn)
        print('  name: {:s}'.format(name))
        
        # Guess MIME type
        if args.mime is None:
            import magic
            mime = magic.from_file(fn, mime=True)
            print('  Detected MIME type: {:s}'.format(mime))
        else:
            mime = None
        
        # Preload file?
        if args.preload:
            print('  Reading file ...')
            with open(fn, 'rb') as f:
                file_contents = f.read()
        else:
            file_contents = open(fn, 'rb')
        
        # Upload file
        if mime is None:
            files = {'file': (name, file_contents)}
        else:
            files = {'file': (name, file_contents, mime)}
        
        try:
            print('  Uploading file ...')
            t0 = time()
            r = requests.post(url, files=files, timeout=args.timeout)
            t1 = time()
        except ConnectionError as err:
            print(err)
            print('Retrying upload ...')
            if isinstance(file_contents, file):
                file_contents.close()
                file_contents = open(fn, 'rb')
                files = {'file': (name, file_contents, mime)}
            t0 = time()
            r = requests.post(url, files=files, timeout=args.timeout)
            t1 = time()
        
        # Report on status
        print('  time elapsed: {:.2f} s'.format(t1-t0))
        print('  status code: {}'.format(r.status_code))
        
        def handle_err(err):
            print(r.text)
            if args.ignore_failure:
                print(err)
            else:
                raise(err)
        
        try:
            r.raise_for_status()
        except HTTPError as err:
            handle_err(err)
        except ConnectionError as err:
            handle_err(err)
        except Timeout as err:
            handle_err(err)
        else:
            # Write checksum
            r_json = r.json()
            checksum = r_json['data']['files'][0]['dataFile']['checksum']['value']
            print('  checksum: {:s}'.format(checksum))
            with open(args.checksum, 'a') as f_c:
                f_c.write('{:s}  {:s}\n'.format(checksum, name))
    
    return 0


if __name__ == '__main__':
    main()
