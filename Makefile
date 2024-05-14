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
	$(call execute_in_env, pip-compile requirements.in)
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

## Install flake8
flake8:
	$(call execute_in_env, $(PIP) install flake8)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up dev requirements (bandit, safety, black, flake8)
dev-setup: bandit safety black coverage flake8

# Build / Run

## Run the security test (bandit + safety)
extract-security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*/*.py *c/*/*/*.py)

## Run the black code check
extract-run-black:
	$(call execute_in_env, black  ./extract/src/*.py ./extract/tests/*.py)

## Run the flake8 (PEP8) code check
extract-run-flake8:
	$(call execute_in_env, flake8  ./extract/src/*.py ./extract/tests/*.py)


## Run the unit tests
extract-unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -vvv --testdox ./extract/tests/)

## Run the coverage check
extract-check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src ./extract/tests/)

## Run all checks
extract-run-checks: extract-security-test extract-run-black extract-run-flake8 extract-unit-test extract-check-coverage

## Run all dependencies
run-all: requirements dev-setup extract-run-checks
