**Strike Protocols API Code Samples**


This repository contains code samples for working with the Strike Protocols API.

* exchange_api/
  Contains a complete example python client for communicating with the Strike Exchange API, as well as
  a test suite which can be executed by editing the Makefile to set the API key credentials and the path
  to the private signing key and running `make test`.

* trade_hash.py
  This python script shows how to compute the Strike Trade Hash for a trade.

* settlement_hash.py
  This python script shows how to compute the Strike Settlement Hash for a settlement plan or settlement.

* settlement_flow_hash.py
  This python script shows how to compute the Strike Settlement Flow Hash for a settlement plan or settlement.

* auth.py
  This contains a python class that shows how to authenticate to the Strike Protocols API.
