import { Dynamo } from '../../../src/services/dynamo';
import { DataLakeRefreshItem } from '../../../src/reports/interfaces/extraction.interface';
import { DataLakeRefreshStatus } from '../../../src/models/base';
import SessionManager from '../../../src/services/session/session-manager.session';
import { RefreshDataLake } from '../../../src/models/refresh-data-lake';
import Hydra from '../../../src/services/hydra';
import PlatformManager from '../../../src/services/session/platform-manager.session';

describe('Refresh data lake', () => {
    const mock = jest.fn();

    it('Should see the last updated status as Succeeded', async () => {
        const dynamo = new mock() as Dynamo;
        const sessionsManagerMock = new mock() as SessionManager;
        sessionsManagerMock.getHydra = (): Hydra => {
            return {} as Hydra;
        };

        dynamo.getLastDataLakeUpdate = (): Promise<DataLakeRefreshItem> => {
            const item: DataLakeRefreshItem = {
                refreshTimeZoneStatus: DataLakeRefreshStatus.RefreshError,
                refreshTimezoneLastDateUpdate: '2022-02-14 13:55:23',
                refreshOnDemandStatus: DataLakeRefreshStatus.RefreshSucceeded,
                refreshOnDemandLastDateUpdate: '2022-02-14 13:55:23',
                platform: 'hydra.docebosaas.com',
            };

            return new Promise(async (resolve) => {
                resolve(item);
            });
        };
        sessionsManagerMock.getDynamo = (): Dynamo => {
            return dynamo;
        };

        sessionsManagerMock.platform = new mock() as PlatformManager;
        sessionsManagerMock.platform.isDatalakeV2Active = (): boolean => {
            return false;
        };

        const refreshDataLake = new RefreshDataLake(sessionsManagerMock);
        const latestStatus = await refreshDataLake.getLatestDatalakeStatus(sessionsManagerMock);
        expect(latestStatus).toEqual(DataLakeRefreshStatus.RefreshSucceeded);
    });

    it('Should see the last updated status as Error', async () => {
        const dynamo = new mock() as Dynamo;
        const sessionsManagerMock = new mock() as SessionManager;
        sessionsManagerMock.getHydra = (): Hydra => {
            return {} as Hydra;
        };

        dynamo.getLastDataLakeUpdate = (): Promise<DataLakeRefreshItem> => {
            const item: DataLakeRefreshItem = {
                refreshTimeZoneStatus: DataLakeRefreshStatus.RefreshInProgress,
                refreshTimezoneLastDateUpdate: '2022-02-14 12:55:23',
                refreshOnDemandStatus: DataLakeRefreshStatus.RefreshError,
                refreshOnDemandLastDateUpdate: '2022-02-14 13:55:23',
                platform: 'hydra.docebosaas.com',
            };

            return new Promise(async (resolve) => {
                resolve(item);
            });
        };

        sessionsManagerMock.getDynamo = (): Dynamo => {
            return dynamo;
        };

        sessionsManagerMock.platform = new mock() as PlatformManager;
        sessionsManagerMock.platform.isDatalakeV2Active = (): boolean => {
            return false;
        };

        const refreshDataLake = new RefreshDataLake(sessionsManagerMock);
        const latestStatus = await refreshDataLake.getLatestDatalakeStatus(sessionsManagerMock);
        expect(latestStatus).toEqual(DataLakeRefreshStatus.RefreshError);
    });


    it('Should see the last updated status as InProgress', async () => {
        const dynamo = new mock() as Dynamo;
        const sessionsManagerMock = new mock() as SessionManager;
        sessionsManagerMock.getHydra = (): Hydra => {
            return {} as Hydra;
        };

        dynamo.getLastDataLakeUpdate = (): Promise<DataLakeRefreshItem> => {
            const item: DataLakeRefreshItem = {
                refreshTimeZoneStatus: DataLakeRefreshStatus.RefreshInProgress,
                refreshTimezoneLastDateUpdate: '2022-02-14 14:55:23',
                refreshOnDemandStatus: DataLakeRefreshStatus.RefreshError,
                refreshOnDemandLastDateUpdate: '2022-02-14 13:55:23',
                platform: 'hydra.docebosaas.com',
            };

            return new Promise(async (resolve) => {
                resolve(item);
            });
        };

        sessionsManagerMock.getDynamo = (): Dynamo => {
            return dynamo;
        };
        sessionsManagerMock.platform = new mock() as PlatformManager;
        sessionsManagerMock.platform.isDatalakeV2Active = (): boolean => {
            return false;
        };

        const refreshDataLake = new RefreshDataLake(sessionsManagerMock);
        const latestStatus = await refreshDataLake.getLatestDatalakeStatus(sessionsManagerMock);
        expect(latestStatus).toEqual(DataLakeRefreshStatus.RefreshInProgress);
    });
});
