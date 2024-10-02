import { Dynamo } from '../../../../src/services/dynamo';
import { ExtractionComponent } from '../../../../src/models/extraction.component';
import SessionManager from '../../../../src/services/session/session-manager.session';
import PlatformManager from '../../../../src/services/session/platform-manager.session';
import { FunctionalTestUtils } from '../../utils';
import { configureMockExpressHttpContext, MockExpressHttpContext } from '../../../utils';
import { redisFactory } from '../../../../src/services/redis/RedisFactory';

jest.setTimeout(25000);
jest.mock('../../../../src/services/session/platform-manager.session');

describe('Dynamo Test', () => {
    let commonKeys: any;
    let mockExpressHttpContext: MockExpressHttpContext;
    const mock = jest.fn();
    const sessionsManagerMock = new mock() as SessionManager;
    sessionsManagerMock.platform = new PlatformManager();

    afterAll(async () => {
        redisFactory.drainPools();
    });

    beforeAll(async () => {
        FunctionalTestUtils.init();
        FunctionalTestUtils.loadTestEnv();
        if (!process.env.REPORTS_TABLE) throw new Error('No test table');

        await FunctionalTestUtils.loadFixtureFromFile(process.env.REPORTS_TABLE, '__tests__/functional/services/dynamo/fixture.json');

        commonKeys = await redisFactory.getRedis().getRedisCommonParams();
    });

    beforeEach(() => {
        mockExpressHttpContext = configureMockExpressHttpContext();
    });

    afterEach(() => {
        if (mockExpressHttpContext) {
            mockExpressHttpContext.afterEachRestoreAllMocks();
        }
    });

    it('Should be able to write new data lake updates items', async () => {
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, '', '', sessionsManagerMock.platform);
        const platforms = ['byee byeee', 'rick.test.com'];
        const extractionComponent = new ExtractionComponent();
        await expect(extractionComponent.updateDataLakeRefreshTimestamp(platforms, dynamo, false))
            .resolves.toBe(undefined);

    });

    it('Should be able to read the reports of a platform', async () => {
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, 'hydra.docebosaas.com', '', sessionsManagerMock.platform);
        const reports = await dynamo.getReports(sessionsManagerMock);

        expect(reports).toBeDefined();
        expect(reports.length).toBeGreaterThan(0);

        // Check that a report data is consistent
        const report = reports[0];

        expect(report.idReport).toBeDefined();
        expect(report.title).toBeDefined();

    });
    it('Should be able to read the reports of a single user of the platform', async () => {
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, 'hydra.docebosaas.com', '', sessionsManagerMock.platform);

        const reports = await dynamo.getUserIdReports(114718);
        expect(reports).toBeDefined();

    });

    it('Should be able to get all the reportsID of the platform', async () => {
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, 'hydra.docebosaas.com', '', sessionsManagerMock.platform);

        const reports = await dynamo.getAllIdReports();
        expect(reports).toBeDefined();
        expect(reports.length).toBeGreaterThan(0);

    });

    // Skipped for now because the logger issue
    it('Should be able to see platform settings', async () => {
        const platforms = ['byee byeee', 'rick.test.com', 'hydra.docebosaas.com'];
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, 'hydra.docebosaas.com', '', sessionsManagerMock.platform);
        const settings = await dynamo.getPlatformsSettings(platforms);
        expect(settings.length).toEqual(3);

    });
});
