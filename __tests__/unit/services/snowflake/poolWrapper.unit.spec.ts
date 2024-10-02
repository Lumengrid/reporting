import { PoolWrapper } from '../../../../src/services/snowflake/poolWrapper';
import { ConnectionDataSourceException } from '../../../../src/exceptions/connectionDataSourceException';
import { ErrorsCode } from '../../../../src/models/base';
import { MockSnowflakeConnection } from '../../../../__mocks__/snowflake-connection';
import { Parameters } from '../../../../src/services/snowflake/interfaces/snowflake.interface';
import { PoolFactory } from '../../../../src/services/snowflake/poolFactory';
import { DBConnection } from '../../../../src/services/snowflake/interfaces/snowflake.interface';
import { Pool } from 'generic-pool';
import { createConnection } from 'snowflake-sdk';
import { ExtractionFailedException } from '../../../../src/exceptions/extractionFailedException';
import { loggerFactory } from '../../../../src/services/logger/logger-factory';
import { SessionLoggerService } from '../../../../src/services/logger/session-logger.service';

jest.setTimeout(5000);
jest.mock('snowflake-sdk', () => ({
    createConnection: jest.fn(),
}));
describe('Snowflake PoolWrapper Test', () => {
    const logger = new SessionLoggerService('unit');
    async function getPool(): Promise<Pool<DBConnection>> {
        const parameters: Parameters = {
            connection: {
                account: 'account',
                username: 'username',
                password: 'password',
                database: 'database',
                schema: 'schema',
                role: 'role',
                warehouse: 'warehouse',
                timeout: 1000,
                clientSessionKeepAlive: false,
                clientSessionKeepAliveHeartbeatFrequency: 1000
            },
            pool: {
                minSize: 1,
                maxSize: 160,
                acquireTimeoutMillis: 30000,
            }
        };
        const poolFactory = new PoolFactory(loggerFactory.buildLogger('[UnitTests]'));

        return poolFactory.createPool({ ...parameters.connection }, { ...parameters.pool });
    }

    function mockSnowflake(failConnect = false,
                  failSwitchSchema = false,
                  failExecute = false): void {
        (createConnection as jest.Mock).mockReturnValue(new MockSnowflakeConnection(failConnect, failSwitchSchema, failExecute, false, false));
    }

    async function genericTest(schema: string, createPool = true, waitResults = true): Promise<any> {
        const pool = createPool ? await getPool() : null;
        const wrapper = new PoolWrapper(pool, logger, 'DATABASE', 'PUBLIC', schema);
        let error = undefined;
        try {
            await wrapper.runQuery('SELECT 1', waitResults, true);
        } catch (err) {
            error = err;
        }
        expect(createPool ? pool.borrowed : 0).toBe(0);

        return error;
    }

    function expectConnectionError(error: any, code: ErrorsCode): void {
        expect(error).toBeDefined();
        expect(error instanceof ConnectionDataSourceException).toBe(true);
        expect(error).toHaveProperty('code');
        expect(error.code).toBe(code);
    }

    it('Pool not instantiated', async () => {
        const error = await genericTest('SCHEMA', false);
        expectConnectionError(error, ErrorsCode.PoolNotInstantiated);
    });

    it('Switch schema not valid', async () => {
        mockSnowflake();
        const error = await genericTest('');
        expectConnectionError(error, ErrorsCode.DatabaseSchemaError);
    });

    it('Switch schema error', async () => {
        mockSnowflake(false, true);
        const error = await genericTest('SCHEMA');
        expectConnectionError(error, ErrorsCode.ConnectionErrorDataSource);
    });

    it('Query error', async () => {
        mockSnowflake(false, false, true);
        const error = await genericTest('SCHEMA');
        expect(error).toBeDefined();
        expect(error instanceof ExtractionFailedException).toBe(true);
        expect(error).toHaveProperty('code');
        expect(error.code).toBe(ErrorsCode.ExtractionFailed);
    });

    it('Query waitResults ok', async () => {
        mockSnowflake();
        const error = await genericTest('SCHEMA');
        expect(error).toBe(undefined);
    });

    it('Query no-waitResults ok', async () => {
        mockSnowflake();
        const error = await genericTest('SCHEMA', true, false);
        expect(error).toBe(undefined);
    });
});
