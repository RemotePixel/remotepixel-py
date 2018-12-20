========
Remotepixel
========

**remotepixel-py** is desgined to be used inside lambda function (see https://github.com/RemotePixel/remotepixel-api)

Install
-------

You can install remotepixel-py using pip

.. code-block:: console

    $ pip install -U pip
    $ pip install remotepixel

or install from source:

.. code-block:: console

    $ git clone https://github.com/remotepixel/remotepixel-py.git
    $ cd remotepixel-py
    $ pip install -U pip
    $ pip install -e .


License
-------

See `LICENSE.txt <LICENSE.txt>`__.

Authors
-------

See `AUTHORS.txt <AUTHORS.txt>`__.

Changes
-------

See `CHANGES.txt <CHANGES.txt>`__.

Contribution & Devellopement
============================

Issues and pull requests are more than welcome.

**Dev install & Pull-Request**

.. code-block:: console

  $ git clone https://github.com/remotepixel/remotepixel-py.git
  $ cd remotepixel-py
  $ pip install -e .[dev]

*Python3.6 only*

This repo is set to use `pre-commit` to run *flake8*, *pydocstring* and *black* ("uncompromising Python code formatter") when committing new code.

.. code-block:: console

  $ pre-commit install
  $ git add .
  $ git commit -m'my change'
  black....................................................................Passed
  Flake8...................................................................Passed
  Verifying PEP257 Compliance..............................................Passed
  $ git push origin
