import { MockSnowflakeConnection, RetrySequence, RetryType } from '../../../../__mocks__/snowflake-connection';
import { ConnectionParameters } from '../../../../src/services/snowflake/interfaces/snowflake.interface';
import { createConnection } from 'snowflake-sdk';
import { Connection } from '../../../../src/services/snowflake/connection';
import { RetryWrapper } from '../../../../src/services/snowflake/retryWrapper';
import { loggerFactory } from '../../../../src/services/logger/logger-factory';

jest.setTimeout(5000);
jest.mock('snowflake-sdk', () => ({
    createConnection: jest.fn(),
}));
describe('Snowflake RetryWrapper Test', () => {
    function mockSnowflake(failDestroy = false,
                           failValid = false,
                           failExecute = false,
                           retrySequences: RetrySequence[] = []): void {
        (createConnection as jest.Mock).mockReturnValue(
            new MockSnowflakeConnection(
                false,
                false,
                failExecute,
                failValid,
                failDestroy,
                retrySequences));
    }

    async function getRetryObject(): Promise<RetryWrapper> {
        const parameters: ConnectionParameters = {
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
        };
        const conn = createConnection({...parameters});
        const connection = new Connection(conn, loggerFactory.buildLogger('[UnitTests]'));
        await connection.connect();

        return new RetryWrapper(connection, loggerFactory.buildLogger('[UnitTests]'));
    }

    async function genericTest(type: RetryType): Promise<any> {
        const retry = await getRetryObject();
        let error = undefined;
        try {
            switch (type) {
                case RetryType.RUN_QUERY:
                    await retry.runQuery('SELECT 1', true, false);
                    break;
                case RetryType.IS_VALID:
                    await retry.isValid();
                    break;
                case RetryType.CLOSE:
                    await retry.close();
                    break;
            }
        } catch (err) {
            error = err;
        }

        return error;
    }

    async function testOk(type: RetryType): Promise<void> {
        mockSnowflake(false, false, false, []);
        const error = await genericTest(type);
        expect(error).toBe(undefined);
    }

    async function testError(type: RetryType, retrySequences: RetrySequence[], expectedAttempt: number): Promise<void> {
        const failDestroy = type === RetryType.CLOSE;
        const failValid = type === RetryType.IS_VALID;
        const failExecute = type === RetryType.RUN_QUERY;
        mockSnowflake(failDestroy, failValid, failExecute, retrySequences);
        const error = await genericTest(type);
        expect(error).toBeDefined();
        expect(error).toHaveProperty('attempt');
        expect(error.attempt).toBe(expectedAttempt);
    }

    function getRetrySequences(type: number): RetrySequence[] {
        switch (type) {
            case 0:
                return [
                    {
                        error: true,
                        code: 505,
                    },
                    {
                        error: true,
                        code: 404,
                    }
                ];
            case 1:
                return [
                    {
                        error: true,
                        code: 504,
                    },
                    {
                        error: true,
                        code: 503,
                    },
                    {
                        error: true,
                        code: 502,
                    },
                    {
                        error: false,
                        code: 200,
                    },
                    {
                        error: true,
                        code: 501,
                    }
                ];
            default:
                const retrySequences: RetrySequence[] = [];
                for (let i = 0; i < 11; i++) {
                    retrySequences.push({
                        error: true,
                        code: 502,
                    });
                }
                return retrySequences;
        }
    }

    it('Valid ok no retry', async () => {
        await testOk(RetryType.IS_VALID);
    });

    it('Query ok no retry', async () => {
        await testOk(RetryType.RUN_QUERY);
    });

    it('Close ok no retry', async () => {
        await testOk(RetryType.CLOSE);
    });

    it('Valid error no retry in case error < 500', async () => {
        const retrySequences = getRetrySequences(0);
        await testError(RetryType.IS_VALID, retrySequences, 1);
    });

    it('Query error no retry in case error < 500', async () => {
        const retrySequences = getRetrySequences(0);
        await testError(RetryType.RUN_QUERY, retrySequences, 1);
    });

    it('Close error no retry in case error < 500', async () => {
        const retrySequences = getRetrySequences(0);
        await testError(RetryType.CLOSE, retrySequences, 1);
    });

    it('Valid error retry in case >= 500 and then ok', async () => {
        const retrySequences = getRetrySequences(1);
        await testError(RetryType.IS_VALID, retrySequences, 3);
    });

    it('Query error retry in case >= 500 and then ok', async () => {
        const retrySequences = getRetrySequences(1);
        await testError(RetryType.RUN_QUERY, retrySequences, 3);
    });

    it('Close error retry in case >= 500 and then ok', async () => {
        const retrySequences = getRetrySequences(1);
        await testError(RetryType.CLOSE, retrySequences, 3);
    });

    it('Valid error retry exhausted', async () => {
        const retrySequences = getRetrySequences(2);
        await testError(RetryType.IS_VALID, retrySequences, 10);
    });

    it('Query error retry exhausted', async () => {
        const retrySequences = getRetrySequences(2);
        await testError(RetryType.RUN_QUERY, retrySequences, 10);
    });

    it('Close error retry exhausted', async () => {
        const retrySequences = getRetrySequences(2);
        await testError(RetryType.CLOSE, retrySequences, 10);
    });
});
