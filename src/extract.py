import requests
import json

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

