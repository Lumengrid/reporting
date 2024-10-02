import * as dotenv from 'dotenv';
import * as fs from 'fs';
import httpContext from 'express-http-context';

// Interfaces and Types
interface Logger {
  debug: jest.Mock;
  error: jest.Mock;
  errorWithStack: jest.Mock;
}
export interface MockExpressHttpContext {
  mocks?: {
    mockGet: jest.Mock;
    mockLogger: Logger;
  };
  afterEachRestoreAllMocks: () => void;
}

// Test Env variables
export function loadTestEnv(): void {
    const pathEnvTest = '.env-test';

    if (fs.existsSync(pathEnvTest)) {
      dotenv.config({ path: `${pathEnvTest}` });
    }
  }

/** express-http-context Mock
 * To avoid logger error when launch tests
    Implementation where needed:

    let mockExpressHttpContext: MockExpressHttpContext;
    beforeEach(() => {
        mockExpressHttpContext = configureMockExpressHttpContext();
    });

    afterEach(() => {
        mockExpressHttpContext.afterEachRestoreAllMocks();
    });
*/

const MOCK_LOGGER: Logger = {
  debug: jest.fn(),
  error: jest.fn(),
  errorWithStack: jest.fn(),
};
export const configureMockExpressHttpContext = (mockLogger: Logger = MOCK_LOGGER): MockExpressHttpContext => {
  const mockGet = jest.fn();

  mockGet.mockReturnValue(mockLogger);
  httpContext.get = mockGet as any;

  const afterEachRestoreAllMocks = () => {
    jest.restoreAllMocks();
  };

  // If needed you can return the "mocks" object, to test that the logger have been called for example
  // mocks: {
  //   mockGet,
  //   mockLogger
  // },
  return {
    afterEachRestoreAllMocks
  };
};