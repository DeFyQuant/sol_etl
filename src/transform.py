import json
from dataclasses import dataclass

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
