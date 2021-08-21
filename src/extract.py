import requests
import json
import csv
from dataclasses import dataclass
import os

#from __future__ import print_function

import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

url = 'https://api.solanabeach.io/v1/'

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

@dataclass
class SolBlock(object):
    '''
    Class for solana block data
    '''
    blocknumber: int = None
    blockhash: str = None
    previousblockhash: str = None
    parentslot: int = None
    blocktime_abs: float = None
    blocktime_rel: float = None
    txcount: int = None
    failedtxs: int = None
    totalfees : float = None
    instructions: int = None
    sucessfultxs: int = None
    totalvaluemoved: float = None
    innerinstructions: int = None



class StagingTable():
    '''
    Class for temporary table with block data
    
 
    
    '''
    def __init__(self, filename:str = 'temp.csv', token:str=None, start_number:int=None):
        '''
        Args:
            filename(str): name of temporary csv file
            token(list): list of blocks
            start_number(int): starting block number

        '''
        if not token:
            raise ValueError(f'_solana_api requires api token')
        
        if not isinstance(filename, str):
            raise ValueError(f'filename must be string')
            
        if not isinstance(start_number, int):
            raise ValueError(f'start_number must be integer')
            
            
        self.token = token
        self.filename = filename
        self.start_number= start_number
        self.chain = []
        
        
        
    def extract_data(self):
        '''
        method to get data from api and write to temporary csv file
        '''
        #call api
        chain, strt, stp = self._create_chain()
        
        #create temp file
        with open(self.filename, 'w') as csv_file:
            wr = csv.DictWriter(csv_file, fieldnames=chain[0].__dict__.keys())
            wr.writeheader()
            for block in chain:
                wr.writerow(block.__dict__)
            
    def drop_temp(self):
        '''
        method to drop temporary table
        '''
        if(os.path.exists(self.filename) and os.path.isfile(self.filename)):
            os.remove(self.filename)
            logging.info("temp file deleted")
        else:
            logging.warning("temp file not found")
            
            
    def _solana_api(self, limit:int = 1, cursor:int = None):
        '''
        API for fetching latest blocks

        Args:
            limit(int) = the number of blocks to retur
            cursor_blocknumber(str) = the starting block number

        Return:
            Response(Json) = returned api json
        '''
        api_get = 'latest-blocks'

        if not isinstance(limit, int):
            raise TypeError(f'_solana_api: limit requires integer value')

        if cursor and not isinstance(cursor, int):
            raise TypeError(f'_solana_api: cursor requires integer value')

        # Set Query Parameters
        query = {'limit': limit}
        if cursor:
            query['cursor'] = str(cursor)

        # Call API
        response = requests.get(url + api_get, params=query, auth=BearerAuth(self.token))
        #print(response)
        if not response.ok:
            raise Error(f"_solana_api api failed: {response}")

        return response.json()

    def _transform_json(self, response:json):
        '''
        Function to parse api response for block data

        Args:
            response(json): successful response from api
        Retrun:
            chain(list): list of SolBlocks
        '''

        chain = []
        start_block_number = float('inf')
        end_block_number = -float('inf')
        for record in response:
            block = SolBlock()
            block.blocknumber = record['blocknumber']
            block.blockhash = record['blockhash']
            block.previousblockhash = record['previousblockhash']
            block.parentslot = record['parentslot']
            block.blocktime_abs = record['blocktime']['absolute']
            block.blocktime_rel = record['blocktime']['relative']
            block.txcount = record['metrics']['txcount']
            block.failedtxs = record['metrics']['failedtxs']
            block.totalfees = record['metrics']['totalfees']
            block.instructions = record['metrics']['instructions']
            block.sucessfultxs = record['metrics']['sucessfultxs']
            block.totalvaluemoved = record['metrics']['totalvaluemoved']
            block.innerinstructions = record['metrics']['innerinstructions']
            if block.blocknumber <= self.start_number:
                break
            chain.append(block)

            start_block_number = min(start_block_number, record['blocknumber'])
            end_block_number = max(end_block_number, record['blocknumber'])
        return chain, start_block_number, end_block_number

    # BLOCKS TO APPEND
    def _create_chain(self):
        '''
        Creates chain of all recent SolBlocks given a starting number

        Return:
            chain(list): all blocks since start number
            start_number(int): starting block number
            end_number(int): ending block number
        '''
        latest_block = self._solana_api(limit = 1)
        end_number = latest_block[0]['blocknumber']
        print(f'retrieving blocks {self.start_number} to {end_number}: total = {end_number-self.start_number}')
        init_end_number = end_number
        chain = []
        while self.start_number + 1 < end_number:

            diff = min(50, end_number - self.start_number)
            blocks = self._solana_api(limit = diff, cursor=end_number)
            rollup, start_roll_number, end_roll_number = self._transform_json(blocks)
            chain.extend(rollup)
            end_number = start_roll_number

        return chain, self.start_number, end_number
