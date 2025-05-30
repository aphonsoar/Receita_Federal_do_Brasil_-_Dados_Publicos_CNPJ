# Tests

This directory contains the unit and integration tests for the project.

## Running Tests

Currently, the tests are primarily placeholders. As the test suite grows, more specific instructions may be added here.

To discover and run all tests using the `unittest` module, navigate to the root directory of the project and run:

```bash
python -m unittest discover tests
```

Alternatively, individual test files can often be run directly:

```bash
python tests/test_etl.py
```

## Test Structure

- `test_etl.py`: Contains placeholder tests for the main ETL processes. This should be expanded to cover data scraping, processing, loading, and auditing functionalities.
- Other `test_*.py` files can be added to test specific modules or components (e.g., `test_database_utils.py`, `test_core_utils.py`).

## Dependencies

Ensure that any dependencies required for running tests are installed in your environment. If specific test data or configurations are needed, details will be provided here.
