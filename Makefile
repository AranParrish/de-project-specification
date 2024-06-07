#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = de-project-specification
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install pip-tools)
	# $(call execute_in_env, pip-compile requirements.in)
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up dev requirements (bandit, safety, black)
dev-setup: bandit safety black coverage

# Build / Run

## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*/*.py *c/*/*/*.py)

## Run the black code check
extract-run-black:
	$(call execute_in_env, black  ./extract/src/*.py ./extract/tests/*.py)

## Run the black code check
transform-run-black:
	$(call execute_in_env, black  ./transform/src/*.py ./transform/tests/*.py)

## Run the black code check
load-run-black:
	$(call execute_in_env, black  ./load/src/*.py ./load/tests/*.py)

## Run the unit tests
extract-unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vvv --testdox ./extract/tests/)

## Run the unit tests
transform-unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vvv --testdox ./transform/tests/)

## Run the unit tests
load-unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vvv --testdox ./load/tests/)

## Run the coverage check
extract-check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=./extract/src ./extract/tests/)

## Run the coverage check
transform-check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=./transform/src ./transform/tests/)
	
## Run the coverage check
load-check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=./load/src ./load/tests/)

## Run extract checks
extract-run-checks: security-test extract-run-black extract-unit-test extract-check-coverage

## Run transform checks
transform-run-checks: security-test transform-run-black transform-unit-test transform-check-coverage

## Run load checks
load-run-checks: security-test load-run-black load-unit-test load-check-coverage

## Run all dependencies
run-all: requirements dev-setup extract-run-checks transform-run-checks load-run-checks
