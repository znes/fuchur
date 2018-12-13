========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |coveralls| |codecov|
        | |scrutinizer| |codeclimate|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|

.. |docs| image:: https://readthedocs.org/projects/fuchur/badge/?style=flat
    :target: https://readthedocs.org/projects/fuchur
    :alt: Documentation Status


.. |travis| image:: https://travis-ci.org/znes/fuchur.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/znes/fuchur

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/znes/fuchur?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/znes/fuchur

.. |requires| image:: https://requires.io/github/znes/fuchur/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/znes/fuchur/requirements/?branch=master

.. |coveralls| image:: https://coveralls.io/repos/znes/fuchur/badge.svg?branch=master&service=github
    :alt: Coverage Status
    :target: https://coveralls.io/r/znes/fuchur

.. |codecov| image:: https://codecov.io/github/znes/fuchur/coverage.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/znes/fuchur

.. |codeclimate| image:: https://codeclimate.com/github/znes/fuchur/badges/gpa.svg
   :target: https://codeclimate.com/github/znes/fuchur
   :alt: CodeClimate Quality Status

.. |version| image:: https://img.shields.io/pypi/v/fuchur.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/fuchur

.. |commits-since| image:: https://img.shields.io/github/commits-since/znes/fuchur/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/znes/fuchur/compare/v0.0.0...master

.. |wheel| image:: https://img.shields.io/pypi/wheel/fuchur.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/fuchur

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/fuchur.svg
    :alt: Supported versions
    :target: https://pypi.org/project/fuchur

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/fuchur.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/fuchur

.. |scrutinizer| image:: https://img.shields.io/scrutinizer/g/znes/fuchur/master.svg
    :alt: Scrutinizer Status
    :target: https://scrutinizer-ci.com/g/znes/fuchur/


.. end-badges

An optimizaton model for Europe based on oemof.

* Free software: BSD 3-Clause License

Installation
============

::

    pip install fuchur

Documentation
=============


https://fuchur.readthedocs.io/


Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
