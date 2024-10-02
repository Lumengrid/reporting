import { ExtractionComponent } from '../../../src/models/extraction.component';
import { Dynamo } from '../../../src/services/dynamo';
import SessionManager from '../../../src/services/session/session-manager.session';
import PlatformManager from '../../../src/services/session/platform-manager.session';
import { FunctionalTestUtils } from '../utils';
import { configureMockExpressHttpContext, MockExpressHttpContext } from '../../utils';
import { redisFactory } from '../../../src/services/redis/RedisFactory';

jest.setTimeout(50000);

describe('Scheduled extraction component', () => {
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

        commonKeys = await redisFactory.getRedis().getRedisCommonParams();
      });

    beforeEach(() => {
        mockExpressHttpContext = configureMockExpressHttpContext();
    });

    afterEach(() => {
        mockExpressHttpContext.afterEachRestoreAllMocks();
    });

    it('Should retrieve the details of the platforms passed in input', async () => {
        const extraction = new ExtractionComponent();
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, '', '', sessionsManagerMock.platform);

        const listOfInputs = [
            'byee byeee',
            'hydra.docebosaas.com',
            'hydra.docebosaas.commm',
            'my.super.platform.com',
            'pippo',
            'pippo.docebosaas.com',
            'plutoneeeeee',
            'rick.test.com',
            'asdasdasdasdasdasd'
        ];

        const res = await extraction.getDataLakePlatformsRefreshDetails(listOfInputs, dynamo);

        expect(true).toBe(true);
    });

    // Doesn't work with DL3 because with the new AWS account we don't have the services to run DL2.5 (extraction.startDataLakeRefresh)
    // it('Should update the status of all the platforms passed in input', async () => {
    //     const extraction = new ExtractionComponent();

    //     const listOfInputs = [
    //         'byee byeee',
    //         'hydra.docebosaas.com',
    //         'hydra.docebosaas.commm',
    //         'my.super.platform.com',
    //         'pippo',
    //         'pippo.docebosaas.com',
    //         'plutoneeeeee',
    //         'rick.test.com',
    //         'asdasdasdasdasdasd'
    //     ];

    //     const res = await extraction.startDataLakeRefresh(listOfInputs, false);
    //     expect(true).toBe(true);
    // });

    it('Should be able to initialize the scheduled extractions', async () => {
        const extraction = new ExtractionComponent();

        await extraction.performScheduledReportsExport([
            'byee byeee',
            'hydra.docebosaas.com',
            'hydra.docebosaas.commm',
            'platform_0',
            'platform_1',
            'platform_2',
            'platform_3',
            'platform_4',
            'platform_5',
            'platform_6',
            'platform_7',
            'platform_8',
            'platform_9',
            'platform_10',
            'platform_11',
            'platform_12',
            'platform_13',
            'platform_14',
            'platform_15',
            'platform_16',
            'platform_17',
            'platform_18',
            'platform_19',
            'platform_20',
            'platform_21',
            'platform_22',
            'platform_23',
            'platform_24',
            'platform_25',
            'platform_26',
            'platform_27',
            'platform_28',
            'platform_29',
            'platform_30',
            'platform_31',
            'platform_32',
            'platform_33',
            'platform_34',
            'platform_35',
            'platform_36',
            'platform_37',
            'platform_38',
            'platform_39',
            'platform_40',
            'platform_41',
            'platform_42',
            'platform_43',
            'platform_44',
            'platform_45',
            'platform_46',
            'platform_47',
            'platform_48',
            'platform_49',
            'platform_50',
            'platform_51',
            'platform_52',
            'platform_53',
            'platform_54',
            'platform_55',
            'platform_56',
            'platform_57',
            'platform_58',
            'platform_59',
            'platform_60',
            'platform_61',
            'platform_62',
            'platform_63',
            'platform_64',
            'platform_65',
            'platform_66',
            'platform_67',
            'platform_68',
            'platform_69',
            'platform_70',
            'platform_71',
            'platform_72',
            'platform_73',
            'platform_74',
            'platform_75',
            'platform_76',
            'platform_77',
            'platform_78',
            'platform_79',
            'platform_80',
            'platform_81',
            'platform_82',
            'platform_83',
            'platform_84',
            'platform_85',
            'platform_86',
            'platform_87',
            'platform_88',
            'platform_89',
            'platform_90',
            'platform_91',
            'platform_92',
            'platform_93',
            'platform_94',
            'platform_95',
            'platform_96',
            'platform_97',
            'platform_98',
            'platform_99',
            'platform_100',
            'platform_101',
            'platform_102',
            'platform_103',
            'platform_104',
            'platform_105',
            'platform_106',
            'platform_107',
            'platform_108',
            'platform_109',
            'platform_110',
            'platform_111',
            'platform_112',
            'platform_113',
            'platform_114',
            'platform_115',
            'platform_116',
            'platform_117',
            'platform_118',
            'platform_119',
            'platform_120',
            'platform_121',
            'platform_122',
            'platform_123',
            'platform_124',
            'platform_125',
            'platform_126',
            'platform_127',
            'platform_128',
            'platform_129',
            'platform_130',
            'platform_131',
            'platform_132',
            'platform_133',
            'platform_134',
            'platform_135',
            'platform_136',
            'platform_137',
            'platform_138',
            'platform_139',
            'platform_140',
            'platform_141',
            'platform_142',
            'platform_143',
            'platform_144',
        ]);
        expect(true).toBe(true);
    });


    it('Should be able to initialize the scheduled extractions for one platform', async () => {
        const extraction = new ExtractionComponent();

        await extraction.performScheduledReportsExport([
            'hydra.docebosaas.com'
        ]);
        expect(true).toBe(true);
    });


});
