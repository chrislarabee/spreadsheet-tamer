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

## Definitions

### Stages

Stages are groups of transmutations that can be executed in sequence 
before stopping and outputting progress. Data-Genius has a number of 
pre-defined generalized stages, each with its own module of functions 
that can be called upon with DataFrame.genius.

#### Preprocess

The Preprocess stage encompasses very basic standardization 
transmutations designed to make a dataset more machine-readable. 
Elimination of 'report-like' features like total rows and titles and 
such, as well as locating header rows if they are not the first row of 
the dataset. 

Basically, Preprocess transmutations are transmutations that can be 
executed on just about any dataset 'blind', before you know anything
about the contents of the dataset.

#### Explore

The Explore stage encompasses transmutations designed to reveal patterns
and inconsistencies in the data. These transmutations are designed to
automatically detect as many possible oddities and errors in the data, so
that you can select which Clean transmutations you need, or write your
own transmutations to further standardize the data. 

#### Clean

The Clean stage encompasses transmutations designed to correct errors in
the dataset and to standardize all the data in the dataset so that it 
follows the appropriate rules. Clean transmutations are designed to 
correct issues found during the Explore stage. 

#### Reformat
