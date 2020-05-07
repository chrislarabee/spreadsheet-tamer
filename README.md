# Data-Genius

An attempt at a universal data normalizer and formatter.

## Setup

Create and activate a virtual environment with:

```
python -m venv venv
```

For windows run `venv\Scripts\activate`, for linux 
`. venv/bin/activate` to activate it.

If you're using pycharm, you should set the interpreter for the
project to be the python interpreter in your venv by going to 
Settings > Project > Python interpreter. Then, click the gear
and click 'Add', check 'Existing Environment' and find the venv
interpreter, which will have the name of your project name in 
parentheses.

Install the requirements with `pip install -r requirements.txt`.

## Testing
Run the tests with `pytest` or you can run a specific test module 
with:

```
pytest tests/path/to/module_test.py
```
