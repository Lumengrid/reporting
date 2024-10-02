import { Redis } from '../../../../src/services/redis/redis';
import SessionManager from '../../../../src/services/session/session-manager.session';
import { ArchivedAuditTrailArchiveDateComponent } from '../../../../src/routes/get/archived-audit-trail-log-archive-date-component';
import { Athena } from '../../../../src/services/athena';
import PlatformManager from '../../../../src/services/session/platform-manager.session';
import { redisFactory, RedisFactory } from '../../../../src/services/redis/RedisFactory';


jest.setTimeout(90000);

describe('Archivied Audit Trail', () => {
    afterAll(async () => {
        redisFactory.drainPools();
    });
    it('Should be able to set up dependiences and respond with the proper date', async () => {
        const mock = jest.fn();
        const redis = new mock() as Redis;
        const redisFactory = new mock() as RedisFactory;
        const athena = new mock() as Athena;
        const platform = new mock() as PlatformManager;
        const sessionsManagerMock = new mock() as SessionManager;

        redis.getLegacyAuditTrailLogsDBName = jest.fn((): Promise<string> => new Promise(async (resolve) => { resolve('db_name'); }));
        redis.getLegacyAuditTrailLogsTableName = jest.fn((): Promise<string> => new Promise(async (resolve) => { resolve('table_name'); }));

        athena.runQuery = jest.fn((): Promise<AWS.Athena.GetQueryResultsOutput> => new Promise(async (resolve) => {
            resolve({
                ResultSet: {
                    Rows: [
                        {},
                        {
                            Data: [
                                {
                                    VarCharValue: '2023-11-22 16:11:06'
                                }
                            ]
                        }
                    ]
                }
            });
        }));

        sessionsManagerMock.getAthena = jest.fn((): Athena => athena);
        redisFactory.getRedis = jest.fn((): Redis => redis);

        sessionsManagerMock.platform = platform;
        platform.getPlatformBaseUrlPath = jest.fn((): string => 'platform_url');

        const archiveDateComponent = new ArchivedAuditTrailArchiveDateComponent(sessionsManagerMock);

        expect(await archiveDateComponent.getArchiveDate()).toBe('2023-11-22 16:11:06');

    });

});