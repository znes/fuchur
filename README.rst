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


.. |travis| image:: https://travis-ci.org/simnh/fuchur.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/simnh/fuchur

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/simnh/fuchur?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/simnh/fuchur

.. |requires| image:: https://requires.io/github/simnh/fuchur/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/simnh/fuchur/requirements/?branch=master

.. |coveralls| image:: https://coveralls.io/repos/simnh/fuchur/badge.svg?branch=master&service=github
    :alt: Coverage Status
    :target: https://coveralls.io/r/simnh/fuchur

.. |codecov| image:: https://codecov.io/github/simnh/fuchur/coverage.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/simnh/fuchur

.. |codeclimate| image:: https://codeclimate.com/github/simnh/fuchur/badges/gpa.svg
   :target: https://codeclimate.com/github/simnh/fuchur
   :alt: CodeClimate Quality Status

.. |version| image:: https://img.shields.io/pypi/v/fuchur.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/fuchur

.. |commits-since| image:: https://img.shields.io/github/commits-since/simnh/fuchur/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/simnh/fuchur/compare/v0.0.0...master

.. |wheel| image:: https://img.shields.io/pypi/wheel/fuchur.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/fuchur

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/fuchur.svg
    :alt: Supported versions
    :target: https://pypi.org/project/fuchur

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/fuchur.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/fuchur

.. |scrutinizer| image:: https://img.shields.io/scrutinizer/g/simnh/fuchur/master.svg
    :alt: Scrutinizer Status
    :target: https://scrutinizer-ci.com/g/simnh/fuchur/


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
