// ONLY WITH Datalake 2.5

// import AWS from 'aws-sdk';
// import { SQS } from './sqs';
// import SessionManager from './session/session-manager.session';
// import { LastRefreshDate } from '../reports/interfaces/extraction.interface';
// import { Dynamo } from './dynamo';
// import PlatformManager from './session/platform-manager.session';
// import Config from '../config';
// import { DataLakeRefreshStatus } from '../models/base';
// import moment from 'moment';


// describe('Step function', () => {
//     let sqs: SQS;
//     const mock = jest.fn();
//     const sessionManager = new mock() as SessionManager;
//     const platformManager = new mock() as PlatformManager;
//     const refreshInfo = new mock() as LastRefreshDate;

//     beforeAll(async (done) => {
//         refreshInfo.refreshStatus = DataLakeRefreshStatus.RefreshError;
//         platformManager.isDatalakeV3ToggleActive = (): boolean => {
//             return false;
//         };

//         jest.spyOn(AWS.Config.prototype, 'update')
//             .mockImplementation(() => '');

//         AWS.SQS.prototype.sendMessage = (params): any => {
//             return {
//                 promise: jest.fn().mockResolvedValue(true)
//             };
//         };

//         jest.spyOn(Config.prototype, 'getDatalakeV2StepFunctionARN')
//             .mockImplementation(() => '123');
//         jest.spyOn(Config.prototype, 'getDataLakeRefreshInfoTableName')
//             .mockImplementation(() => 'test-datalake-refresh-info');

//         sqs = new SQS('test-region');

//         platformManager.getPlatformBaseUrl = (): string => 'test-platform';
//         platformManager.getAthenaSchemaNameOverride = (): string => 'test-schema';
//         platformManager.getDatalakeV2DataBucket = (): string => 'test-bucket';
//         platformManager.getDatalakeV2Host = (): string => 'test-host';
//         platformManager.getDbHostOverride = (): string => 'test-db-host';
//         sessionManager.platform = platformManager;
//         const dynamo = new mock() as Dynamo;
//         dynamo.updateDataLakeNightlyRefreshStatus = (refreshStatus: DataLakeRefreshStatus): Promise<void> => {
//             return;
//         };
//         dynamo.restartDataLakeErrorCount = (): Promise<void> => {
//             return;
//         };
//         sessionManager.getDynamo = (): Dynamo => {
//             return dynamo;
//         };

//         done();
//     });

//     it('Should not be able to call step function if error count >= 3', async (done) => {
//         refreshInfo.refreshStatus = DataLakeRefreshStatus.RefreshError;
//         refreshInfo.errorCount = 3;
//         refreshInfo.lastRefreshStartDate = moment().utc().subtract(2, 'minutes').format('YYYY-MM-DD HH:mm:ss');
//         expect(await sqs.runDataLakeV2Refresh(sessionManager, refreshInfo)).toEqual(false);
//         refreshInfo.refreshStatus = DataLakeRefreshStatus.RefreshError;
//         refreshInfo.errorCount = 4;
//         expect(await sqs.runDataLakeV2Refresh(sessionManager, refreshInfo)).toEqual(false);
//         done();
//     });

//     it('Should be able to call step function if error count >= 3 and at least 20 minutes are passed from the last call', async (done) => {
//         refreshInfo.refreshStatus = DataLakeRefreshStatus.RefreshError;
//         refreshInfo.errorCount = 3;
//         refreshInfo.lastRefreshStartDate = moment().utc().subtract(30, 'minutes').format('YYYY-MM-DD HH:mm:ss');
//         expect(await sqs.runDataLakeV2Refresh(sessionManager, refreshInfo)).toEqual(true);
//         refreshInfo.refreshStatus = DataLakeRefreshStatus.RefreshError;
//         refreshInfo.errorCount = 4;
//         expect(await sqs.runDataLakeV2Refresh(sessionManager, refreshInfo)).toEqual(true);
//         done();
//     });

//     it('Should be able to call step function if error count < 3', async (done) => {
//         refreshInfo.errorCount = 1;
//         expect(await sqs.runDataLakeV2Refresh(sessionManager, refreshInfo)).toEqual(true);
//         refreshInfo.refreshStatus = DataLakeRefreshStatus.RefreshError;
//         refreshInfo.errorCount = 2;
//         expect(await sqs.runDataLakeV2Refresh(sessionManager, refreshInfo)).toEqual(true);
//         done();
//     });

//     it('Should be able to call step function if error count doesn\'t exist', async (done) => {
//         refreshInfo.errorCount = undefined;
//         expect(await sqs.runDataLakeV2Refresh(sessionManager, refreshInfo)).toEqual(true);
//         done();
//     });

//     it('Should not be able to call step function if Datalake V3 Toggle is ON', async (done) => {
//         platformManager.isDatalakeV3ToggleActive = (): boolean => {
//             return true;
//         };
//         expect(await sqs.runDataLakeV2Refresh(sessionManager, refreshInfo)).toEqual(false);
//         done();
//     });
// });
