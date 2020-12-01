STRIKE_API_KEY=
STRIKE_API_SECRET=
SIGNING_KEY=/path/to/private/signing/key.pem

test: setup
	. ./venv/bin/activate && PYTHONPATH=. python3 -m examples.run_examples \
		--key ${STRIKE_API_KEY} \
		--secret "${STRIKE_API_SECRET}" \
		--signing-key-file ${SIGNING_KEY} \
		--url https://api-uat1.strikeprotocols.com

setup:
	test -d venv || python3 -m venv venv
	. ./venv/bin/activate && pip3 install -r requirements.txt
