import glob
from setuptools import setup

setup(
   name='strike-exchange-api-client',
   version='1.0.0',
   description='Sample client for the Strike Exchange API',
   author='Strike Protocols, Inc.',
   author_email='developers@strikeprotocols.com',
   packages=['exchange_api'],
   install_requires=['ecdsa', 'requests', 'pytz']
)
