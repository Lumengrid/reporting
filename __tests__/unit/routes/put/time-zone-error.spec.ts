import { Dynamo } from '../../../../src/services/dynamo';
import SessionManager from '../../../../src/services/session/session-manager.session';
import Hydra from '../../../../src/services/hydra';
import { putRefreshInError } from '../../../../src/routes/put/time-zone-error';
import { Request, Response } from 'express';
import { DataLakeRefreshItem } from '../../../../src/reports/interfaces/extraction.interface';
import { DataLakeRefreshStatus, ReportsSettingsResponse } from '../../../../src/models/base';

describe('Timezone error', () => {
    const mock = jest.fn();

    it('Shouldn\'t set in error status if datalake is not in progress', async () => {
        const dynamo = new mock() as Dynamo;

        const sessionsManagerMock = new mock() as SessionManager;
        sessionsManagerMock.getHydra = (): Hydra => {
            return {} as Hydra;
        };
        sessionsManagerMock.getDynamo = (): Dynamo => {
            return dynamo;
        };

        dynamo.getLastDataLakeUpdate = (sessionsManagerMock): Promise<DataLakeRefreshItem> => {
            const item: DataLakeRefreshItem = {
                platform: 'test.docebo.com',
                refreshOnDemandStatus: DataLakeRefreshStatus.RefreshSucceeded,
                refreshTimeZoneStatus: DataLakeRefreshStatus.RefreshSucceeded
            };
            return new Promise(async (resolve) => {
                resolve(item);
            });
        };

        const mockReq = new mock() as Request;
        const next = jest.fn();
        let responseObject: ReportsSettingsResponse;
        let responseStatus: number;
        const res = new mock() as Response;
        res.locals = new mock();
        res.type = jest.fn();
        res.status = jest.fn().mockImplementation((status) => {
            responseStatus = status;
        });
        res.json = jest.fn().mockImplementation((result) => {
            responseObject = result;
        });

        res.locals.session = sessionsManagerMock;

        await putRefreshInError(mockReq, res, next);
        expect(responseObject.error).toEqual('No datalake in progress found.');
        expect(responseObject.success).toEqual(false);
        expect(responseStatus).toEqual(400);
    });


    it('Shouldn\'t set in error status if datalake doesn\'t exists in platform', async () => {
        const dynamo = new mock() as Dynamo;

        const sessionsManagerMock = new mock() as SessionManager;
        sessionsManagerMock.getHydra = (): Hydra => {
            return {} as Hydra;
        };
        sessionsManagerMock.getDynamo = (): Dynamo => {
            return dynamo;
        };

        dynamo.getLastDataLakeUpdate = (sessionsManagerMock): Promise<DataLakeRefreshItem> => {
            return new Promise(async (resolve) => {
                resolve(undefined);
            });
        };

        const mockReq = new mock() as Request;
        const next = jest.fn();
        let responseObject: ReportsSettingsResponse;
        let responseStatus: number;
        const res = new mock() as Response;
        res.locals = new mock();
        res.type = jest.fn();
        res.status = jest.fn().mockImplementation((status) => {
            responseStatus = status;
        });
        res.json = jest.fn().mockImplementation((result) => {
            responseObject = result;
        });

        res.locals.session = sessionsManagerMock;

        await putRefreshInError(mockReq, res, next);
        expect(responseObject.error).toEqual('Platform not found in refresh_info table');
        expect(responseObject.success).toEqual(false);
        expect(responseStatus).toEqual(404);
    });

    it('Should set in error status if datalake status in \'progress\'', async () => {
        const dynamo = new mock() as Dynamo;
        let datalakeRefreshItemUpdated: DataLakeRefreshItem;
        const sessionsManagerMock = new mock() as SessionManager;
        sessionsManagerMock.getHydra = (): Hydra => {
            return {} as Hydra;
        };
        sessionsManagerMock.getDynamo = (): Dynamo => {
            return dynamo;
        };

        dynamo.getLastDataLakeUpdate = (sessionsManagerMock): Promise<DataLakeRefreshItem> => {
            const item: DataLakeRefreshItem = {
                platform: 'test.docebo.com',
                refreshOnDemandStatus: DataLakeRefreshStatus.RefreshInProgress,
                refreshTimeZoneStatus: DataLakeRefreshStatus.RefreshSucceeded
            };
            return new Promise(async (resolve) => {
                resolve(item);
            });
        };

        dynamo.updateDataLakeRefreshItem = (datalakeRefreshItem): Promise<void> => {
            datalakeRefreshItemUpdated = datalakeRefreshItem;
            return new Promise(async (resolve) => {
                resolve(undefined);
            });
        };

        const mockReq = new mock() as Request;
        const next = jest.fn();
        let responseObject: ReportsSettingsResponse;
        let responseStatus: number;
        const res = new mock() as Response;
        res.locals = new mock();
        res.type = jest.fn();
        res.status = jest.fn().mockImplementation((status) => {
            responseStatus = status;
        });
        res.json = jest.fn().mockImplementation((result) => {
            responseObject = result;
        });

        res.locals.session = sessionsManagerMock;

        await putRefreshInError(mockReq, res, next);
        expect(responseObject.error).not.toBeDefined();
        expect(responseObject.success).toEqual(true);
        expect(responseStatus).toEqual(200);
        expect(datalakeRefreshItemUpdated.refreshOnDemandStatus).toEqual(DataLakeRefreshStatus.RefreshError);
        expect(datalakeRefreshItemUpdated.refreshTimeZoneStatus).toEqual(DataLakeRefreshStatus.RefreshSucceeded);

    });
});
