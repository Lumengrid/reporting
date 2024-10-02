import { FunctionalTestUtils } from '../utils';
import { ErrorCode } from '../../../src/exceptions';
import { Dynamo } from '../../../src/services/dynamo';
import PlatformManager from '../../../src/services/session/platform-manager.session';
import SessionManager from '../../../src/services/session/session-manager.session';
import { CustomReportType } from '../../../src/query-builder/interfaces/query-builder.interface';
import { redisFactory } from '../../../src/services/redis/RedisFactory';

jest.setTimeout(90000);

describe('Custom Report Types', () => {
    let commonKeys: any;
    const customReportTypesId = '04efcaac-7ea3-4749-8619-51fdb58615f4';
    const changeCustomReportTypesId = '04efcaac-7ea4-4749-8619-51fdb58615f4';

    const mock = jest.fn();
    let sessionsManagerMock = new mock() as SessionManager;
    let platformManager = new mock() as PlatformManager;

    afterAll(async () => {
        redisFactory.drainPools();
    });

    beforeAll(async () => {
        FunctionalTestUtils.init();
        FunctionalTestUtils.loadTestEnv();
        if (!process.env.CUSTOM_REPORT_TYPES_TABLE) throw new Error('No test table');
        await FunctionalTestUtils.loadFixtureFromFile(process.env.CUSTOM_REPORT_TYPES_TABLE, '__tests__/functional/custom-report-types/fixture.json');

        commonKeys = await redisFactory.getRedis().getRedisCommonParams();

        platformManager.getCustomReportTypesTableName = jest.fn((): string => process.env.CUSTOM_REPORT_TYPES_TABLE as string);
        platformManager.getDbHostOverride = jest.fn((): string => 'db_host_override');
        platformManager.getDatalakeV2Host = jest.fn((): string => 'get_datalake_V2_host');
        platformManager.getAthenaSchemaNameOverride = jest.fn((): string => 'athena_scehma_override');
        sessionsManagerMock.platform = platformManager;
    });


    it('Should be able to deleted custom report by Id', async () => {
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, 'hydra.docebosaas.com', '', platformManager);
        await dynamo.deleteCustomReportTypeById(customReportTypesId);

        let error = {} as any;
        try {
            await dynamo.getCustomReportTypesById(customReportTypesId);
        } catch (err) {
            error = err;
        }
        expect(error.code).toEqual(ErrorCode.REPORT_NOT_FOUND);
    });

    it('Should be able to see only customReportTypes active', async () => {
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, 'hydra.docebosaas.com', '', sessionsManagerMock.platform);
        const customReportTypes = await dynamo.getActiveCustomReportTypes();

        expect(customReportTypes.length).toEqual(2);

    });

    it('Should be able to change custom report type name', async () => {

        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, 'hydra.docebosaas.com', '', sessionsManagerMock.platform);

        const customReportTypes: CustomReportType = await dynamo.getCustomReportTypesById(changeCustomReportTypesId) as CustomReportType;

        expect(customReportTypes.name).toEqual('no change it2');

        customReportTypes.name = 'I change it2';
        await dynamo.createOrEditCustomReportTypes(customReportTypes);

        const customReportTypesChanged: CustomReportType = await dynamo.getCustomReportTypesById(changeCustomReportTypesId) as CustomReportType;
        expect(customReportTypesChanged.name).toEqual('I change it2');
    });

});
