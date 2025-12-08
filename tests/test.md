docker compose up --build --abort-on-container-exit --exit-code-from tests
docker compose run --rm -e TEST_SELECTOR="tests/test_myfile.py::TestMyClass::test_something"
docker compose run --rm \
  -e TEST_SELECTOR="tests/test_myfile.py" \
  -e PYTEST_ARGS="-q -k 'fast and not network'" \
  tests
CI-friendly single-shot (build + run + exit with test code):
docker compose up --build --abort-on-container-exit --exit-code-from tests
<!-- best logger -->
docker compose run --rm -e TEST_SELECTOR="tests/grpc_server/job_manager" tests
