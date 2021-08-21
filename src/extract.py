import requests
import json
from dataclasses import dataclass

url = 'https://api.solanabeach.io/v1/'

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

def _solana_api(limit:int = 1, cursor:str = None, token:str = None):
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
  
  if not token:
    raise TypeError(f'_solana_api requires api token')
  
  if cursor and not isinstance(cursor, str):
    raise TypeError(f'_solana_api: cursor requires str value')

  # Set Query Parameters
  query = {'limit': limit}
  if cursor:
    query['cursor'] = cursor
  
  # Call API
  response = requests.get(url + api_get, params=query, auth=BearerAuth(token))
  #print(response)
  if not response.ok:
    raise Error(f"_solana_api api failed: {response}")

  return response.json()

@dataclass
class SolBlock(object):
  '''
  Class for solana block data
  '''
  def __init__(self):
    self.blocknumber: int
    self.blockhash: str
    self.previousblockhash: str
    self.parentslot: int
    self.blocktime_abs: float
    self.blocktime_rel: float
    self.txcount: int
    self.failedtxs: int
    self.totalfees : float
    self.instructions: int
    self.sucessfultxs: int
    self.totalvaluemoved: float
    self.innerinstructions: int

def parse_json(response:json):
  '''
  Function to parse api response for block data
  
  Args:
    response(json): successful response from api

  Retrun:
    chain(list): list of SolBlocks
  '''

  chain = []
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
    chain.append(block)
  return chain
