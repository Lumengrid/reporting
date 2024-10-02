import { Dynamo } from '../../../src/services/dynamo';
import { DocumentClient } from 'aws-sdk/lib/dynamodb/document_client';
import ItemList = DocumentClient.ItemList;
import PlatformManager from '../../../src/services/session/platform-manager.session';
import { configureMockExpressHttpContext, MockExpressHttpContext } from '../../utils';

describe('DynamoDB', () => {
    let mockExpressHttpContext: MockExpressHttpContext;
    beforeEach(() => {
        mockExpressHttpContext = configureMockExpressHttpContext();
    });

    afterEach(() => {
        mockExpressHttpContext.afterEachRestoreAllMocks();
    });

    it('Should be able to see the platforms settings by platform list', async () => {
        const mock = jest.fn();
        const platformManager = new mock() as PlatformManager;
        platformManager.getCustomReportTypesTableName = jest.fn((): string => process.env.CUSTOM_REPORT_TYPES_TABLE);
        platformManager.getDbHostOverride = jest.fn((): string => 'db_host_override');
        platformManager.getAthenaSchemaNameOverride = jest.fn((): string => 'athena_scehma_override');
        platformManager.getDatalakeV2Host = jest.fn((): string => 'datalake_v2_host');

        const dynamo = new Dynamo('', '', '', platformManager);
        dynamo.batchGetItem = (): Promise<ItemList> => {
            return new Promise(async (resolve) => {
                resolve([{
                    toggleDatalakeV2: false,
                    platform: 'test1.docebosaas.com',
                    dailyRefreshTokens: 30,
                    monthlyRefreshTokens: 30
                }]);
            });
        };
        const result = await dynamo.getPlatformsSettings(['test1.docebosaas.com', 'test2.docebosaas.com']);
        expect(result.length).toEqual(2);
        expect(result[0].platform).toEqual('test1.docebosaas.com');
        expect(result[0].toggleDatalakeV2).toEqual(false);
        expect(result[0].dailyRefreshTokens).toEqual(30);
        expect(result[0].monthlyRefreshTokens).toEqual(30);
        expect(result[1].platform).toEqual('test2.docebosaas.com');
        expect(result[1].toggleDatalakeV2).toBeUndefined();
        expect(result[1].dailyRefreshTokens).toBeUndefined();
        expect(result[1].monthlyRefreshTokens).toBeUndefined();
    });
});
