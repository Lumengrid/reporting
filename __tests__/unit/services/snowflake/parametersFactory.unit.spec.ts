import { ParametersFactory } from '../../../../src/services/snowflake/parametersFactory';
import { Redis } from '../../../../src/services/redis/redis';
import { loggerFactory } from '../../../../src/services/logger/logger-factory';
import Config from '../../../../src/config';
import { redisFactory } from '../../../../src/services/redis/RedisFactory';

jest.setTimeout(5000);

jest.mock('../../../../src/services/redis/redis');

describe('Snowflake Parameters Factory Test', () => {
    afterAll(async () => {
        redisFactory.drainPools();
    });

    it('All params set', async () => {
        const values = {
            snowflakeLocator: 'snowflakeLocator',
            snowflakeUsername: 'snowflakeUsername',
            snowflakePassword: 'snowflakePassword',
            snowflakeDatabase: 'snowflakeDatabase',
            snowflakeWarehouse: 'snowflakeWarehouse',
            snowflakeRole: 'snowflakeRole',
            snowflakeStorageIntegration: 'snowflakeStorageIntegration',
            snowflakeStorageIntegrationBucket: 'snowflakeStorageIntegrationBucket',
            snowflakeLockTable: 'snowflakeLockTable',
            snowflakePoolMin: '1',
            snowflakePoolMax: '160',
            snowflakeClientSessionKeepAliveEnabled: '0',
            snowflakeClientSessionKeepAliveFrequency: '1000',
            snowflakeAcquireTimeout: '30000',
            snowflakeTimeout: '1800000',
            snowflakeDefaultSchema: 'PUBLIC',
            snowflakeLogLevel: 'TRACE',
        };

        const mock = jest.fn();
        const redis = new mock() as Redis;
        redis.getRedisCommonParams = jest.fn((platform?: string) => Promise.resolve(values));
        jest.spyOn(redisFactory, 'getRedis').mockReturnValue(redis);
        const factory = new ParametersFactory(new Config(), loggerFactory.buildLogger('[UnitTests]'));
        const parameters = await factory.getParameters();
        expect(parameters).toBeDefined();

        return;
    });

    it('Missing some mandatory params', async () => {
        const values = {
            snowflakeLocator: '',
            snowflakeUsername: 'snowflakeUsername',
            snowflakePassword: 'snowflakePassword',
            snowflakeDatabase: 'snowflakeDatabase',
            snowflakeWarehouse: '',
            snowflakeRole: 'snowflakeRole',
            snowflakeStorageIntegration: 'snowflakeStorageIntegration',
            snowflakeStorageIntegrationBucket: 'snowflakeStorageIntegrationBucket',
            snowflakeLockTable: 'snowflakeLockTable',
            snowflakePoolMin: '1',
            snowflakePoolMax: '',
            snowflakeClientSessionKeepAliveEnabled: '0',
            snowflakeClientSessionKeepAliveFrequency: '1000',
            snowflakeAcquireTimeout: '30000',
            snowflakeTimeout: '1800000',
            snowflakeDefaultSchema: 'PUBLIC',
            snowflakeLogLevel: 'TRACE',
        };
        const mock = jest.fn();
        const redis = new mock() as Redis;
        redis.getRedisCommonParams = jest.fn((platform?: string) => Promise.resolve(values));
        jest.spyOn(redisFactory, 'getRedis').mockReturnValue(redis);
        const factory = new ParametersFactory(new Config(), loggerFactory.buildLogger('[UnitTests]'));
        const parameters = await factory.getParameters();
        expect(parameters).toBe(undefined);

        return;
    });

    it('Missing non-mandatory params', async () => {
        const values = {
            snowflakeLocator: 'snowflakeLocator',
            snowflakeUsername: 'snowflakeUsername',
            snowflakePassword: 'snowflakePassword',
            snowflakeDatabase: 'snowflakeDatabase',
            snowflakeWarehouse: 'snowflakeWarehouse',
            snowflakeRole: 'snowflakeRole',
            snowflakeStorageIntegration: 'snowflakeStorageIntegration',
            snowflakeStorageIntegrationBucket: 'snowflakeStorageIntegrationBucket',
            snowflakeLockTable: 'snowflakeLockTable',
            snowflakePoolMin: '1',
            snowflakePoolMax: '',
            snowflakeClientSessionKeepAliveEnabled: '0',
            snowflakeClientSessionKeepAliveFrequency: '',
            snowflakeAcquireTimeout: '',
            snowflakeTimeout: '1800000',
            snowflakeDefaultSchema: '',
            snowflakeLogLevel: 'TRACE',
        };
        const mock = jest.fn();
        const redis = new mock() as Redis;
        redis.getRedisCommonParams = jest.fn((platform?: string) => Promise.resolve(values));
        jest.spyOn(redisFactory, 'getRedis').mockReturnValue(redis);
        const factory = new ParametersFactory(new Config(), loggerFactory.buildLogger('[UnitTests]'));
        const parameters = await factory.getParameters();
        expect(parameters).toBeDefined();

        return;
    });

    it('Missing all params', async () => {
        const mock = jest.fn();
        const redis = new mock() as Redis;
        redis.getRedisCommonParams = jest.fn((platform?: string) => Promise.resolve({platform: 'hydra.docebosaas.com'}));
        jest.spyOn(redisFactory, 'getRedis').mockReturnValue(redis);
        const factory = new ParametersFactory(new Config(), loggerFactory.buildLogger('[UnitTests]'));
        const parameters = await factory.getParameters();
        expect(parameters).toBe(undefined);

        return;
    });
});
