import os

# Tests must not inherit service credentials or external metrics backends from the shell.
os.environ['BTS_SERVER_HMAC_KEY'] = 'dGVzdC1obWFjLWtleQ=='
os.environ['BTS_ENABLE_METRICS'] = 'false'
