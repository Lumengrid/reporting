aamon


# TESTS:
\_\_tests\_\_
- unit
- integration
- functional
- E2E

\_\_src\_\_
- unit

<br>


Run all tests in this way:

```jest```

To run the test `refresh-on-demand.e2e.spec`
you need to change the file `docker-compose.override.yml` and add these lines:
```
services:
 aamon:
  ports:
   - "1234:80"
 ```
and change the `.env-e2e` file in this way:
```
# Aamon settings
AAMON_PORT=1234
AAMON_HOST=127.0.0.1
```

If you don't have this keys in the env file the test will be skipped.

<br>


## To run a single file test:
`npm run test:jest -- __tests__/functional/reports/reports.spec.ts --silent=false --detectOpenHandles`

## To run a single test in specific file (add -t "my test description"):
`npm run test:jest -- __tests__/functional/reports/reports.spec.ts -t "Should be able to get reports filtered by permissions" --silent=false --detectOpenHandles`

#### Options:
- `--silent=false`: ensures that all console outputs like logs, errors, and warnings that occur during tests are displayed in the output
- `--detectOpenHandles`: to identify and report any resources (like timers, databases, system files, or network sockets) that remain open after the tests have completed
