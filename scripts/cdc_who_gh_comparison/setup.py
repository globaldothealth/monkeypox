import logging
import os
from time import sleep

import requests


MOUNTEBANK_URL = os.environ.get("MOUNTEBANK_URL", "http://mountebank:2525")
IMPOSTER_PORT = os.environ.get("IMPOSTER_PORT", 4242)

IMPOSTER_REQUEST = {
  "port": IMPOSTER_PORT,
  "protocol": "http",
  "recordRequests": True
}


def wait_for_mountebank() -> None:
	logging.info("Waiting for Mountebank")
	counter = 0
	while counter < 42:
		try:
			response = requests.get(MOUNTEBANK_URL)
			return
		except requests.exceptions.ConnectionError:
			pass
		counter += 1
		sleep(5)
	raise Exception("Mountebank not available")


def create_imposter() -> None:
	logging.info(f"Creating imposter at port {IMPOSTER_PORT}")
	_ = requests.post(f"{MOUNTEBANK_URL}/imposters", json=IMPOSTER_REQUEST)


if __name__ == "__main__":
	logging.info("Starting local/testing setup script")
	wait_for_mountebank()
	create_imposter()
	logging.info("Done")
