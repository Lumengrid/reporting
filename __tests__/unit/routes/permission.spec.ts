import {
    checkDatalakeV2ManualRefreshToggleActivation,
    checkDatalakeV2ToggleActivation,
    checkExistDataFresher
} from '../../../src/routes/permissions';
import { Request, Response } from 'express';
import SessionManager from '../../../src/services/session/session-manager.session';
import { Dynamo } from '../../../src/services/dynamo';
import { DataLakeRefreshItem } from '../../../src/reports/interfaces/extraction.interface';
import { BaseResponse, DataLakeRefreshStatus, ErrorsCode } from '../../../src/models/base';
import PlatformManager from '../../../src/services/session/platform-manager.session';

describe('Permission', () => {
    it('Shouldn\'t refresh datalake v2 if we don\'t have last datalake update information', async () => {
        const next = jest.fn();
        const mock = jest.fn();
        const req = new mock() as Request;
        const res = new mock() as Response;
        res.locals = new mock();
        const session = res.locals.session = new mock() as SessionManager;
        const dynamoMock = new mock() as Dynamo;
        session.getDynamo = jest.fn((): Dynamo => dynamoMock);
        dynamoMock.getDatalakeV2LastCompleteTime = jest.fn((): Promise<string> => new Promise(async (resolve, reject) => {
            resolve('2022-01-01 00:00:00');
        }));

        dynamoMock.getLastDataLakeUpdate = jest.fn((): Promise<DataLakeRefreshItem> => new Promise(async (resolve, reject) => {
            resolve({
                platform: 'hydra.docebosaas.com'
            });
        }));
        try {
            await checkExistDataFresher(req, res, next);
        } catch (exception: any) {
            expect(exception.message).toEqual('No detail for the refresh status of the platform');
        }
    });

    it('Shouldn\'t refresh datalake v2 if complete time is empty', async () => {
        const next = jest.fn();
        const mock = jest.fn();
        const req = new mock() as Request;
        const res = new mock() as Response;
        res.locals = new mock();
        res.type = jest.fn();
        res.json = jest.fn((jsonResponse) => res.json = jsonResponse);
        const session = res.locals.session = new mock() as SessionManager;
        const dynamoMock = new mock() as Dynamo;
        session.getDynamo = jest.fn((): Dynamo => dynamoMock);
        dynamoMock.getDatalakeV2LastCompleteTime = jest.fn((): Promise<string> => new Promise(async (resolve, reject) => {
            reject('');
        }));

        await checkExistDataFresher(req, res, next);
        const jsonResponse = res.json as unknown as BaseResponse;
        expect(jsonResponse.error).toEqual('Complete time not found');
        expect(jsonResponse.success).toEqual(false);
        expect(jsonResponse.errorCode).toEqual(ErrorsCode.CompleteTimeNotFound);
        expect(next).toHaveBeenCalledTimes(0);
    });

    it('Can\'t refresh datalake v2 if there aren\'t data fresher', async () => {
        const next = jest.fn();
        const mock = jest.fn();
        const req = new mock() as Request;
        const res = new mock() as Response;
        res.locals = new mock();
        res.type = jest.fn();
        res.json = jest.fn((jsonResponse) => res.json = jsonResponse);
        const session = res.locals.session = new mock() as SessionManager;
        const dynamoMock = new mock() as Dynamo;
        session.getDynamo = jest.fn((): Dynamo => dynamoMock);
        dynamoMock.getDatalakeV2LastCompleteTime = jest.fn((): Promise<string> => new Promise(async (resolve, reject) => {
            resolve('2022-01-01 00:00:00');
        }));

        dynamoMock.getLastDataLakeUpdate = jest.fn((): Promise<DataLakeRefreshItem> => new Promise(async (resolve, reject) => {
            resolve({
                platform: 'hydra.docebosaas.com',
                refreshOnDemandLastDateUpdate: '2022-01-01 00:00:00',
                refreshTimezoneLastDateUpdate: '2022-01-01 00:00:00',
                refreshTimezoneLastDateUpdateV2: '2022-01-01 00:00:00',
                refreshOnDemandStatus: DataLakeRefreshStatus.RefreshSucceeded,
                refreshTimeZoneStatus: DataLakeRefreshStatus.RefreshSucceeded
            });
        }));
        await checkExistDataFresher(req, res, next);
        const jsonResponse = res.json as unknown as BaseResponse;
        expect(jsonResponse.error).toEqual('No Data Fresher');
        expect(jsonResponse.success).toEqual(false);
        expect(jsonResponse.errorCode).toEqual(ErrorsCode.NotDataFresher);
        expect(next).toHaveBeenCalledTimes(0);
    });

    it('Can refresh datalake v2 if there are data fresher' , async () => {
        const next = jest.fn();
        const mock = jest.fn();
        const req = new mock() as Request;
        const res = new mock() as Response;
        res.locals = new mock();
        res.type = jest.fn();
        res.json = jest.fn((jsonResponse) => res.json = jsonResponse);
        const session = res.locals.session = new mock() as SessionManager;
        const dynamoMock = new mock() as Dynamo;
        session.getDynamo = jest.fn((): Dynamo => dynamoMock);
        dynamoMock.getDatalakeV2LastCompleteTime = jest.fn((): Promise<string> => new Promise(async (resolve, reject) => {
            resolve('2022-01-06 00:00:00');
        }));

        dynamoMock.getLastDataLakeUpdate = jest.fn((): Promise<DataLakeRefreshItem> => new Promise(async (resolve, reject) => {
            resolve({
                platform: 'hydra.docebosaas.com',
                refreshOnDemandLastDateUpdate: '2022-01-01 00:00:00',
                refreshTimezoneLastDateUpdate: '2022-01-01 00:00:00',
                refreshTimezoneLastDateUpdateV2: '2022-01-01 00:00:00',
                refreshOnDemandStatus: DataLakeRefreshStatus.RefreshSucceeded,
                refreshTimeZoneStatus: DataLakeRefreshStatus.RefreshSucceeded
            });
        }));
        await checkExistDataFresher(req, res, next);
        expect(next).toBeCalled();
    });

    it('Shouldn\'t refresh Datalake V2 if TOGGLE_FORCE_DATALAKE_V1 is enabled', async () => {
        const next = jest.fn();
        const mock = jest.fn();
        const req = new mock() as Request;
        const res = new mock() as Response;
        let responseStatus: number;

        res.locals = new mock();
        res.json = jest.fn((jsonResponse) => res.json = jsonResponse);
        res.sendStatus = jest.fn().mockImplementation((status) => {
            responseStatus = status;
        });
        const sessionsManagerMock = res.locals.session = new mock() as SessionManager;

        sessionsManagerMock.platform = new mock() as PlatformManager;
        sessionsManagerMock.platform.isDatalakeV2Active = (): boolean => {
            return false;
        };
        await checkDatalakeV2ToggleActivation(req, res, next);
        expect(next).toHaveBeenCalledTimes(0);
        // @ts-ignore
        expect(responseStatus).toEqual(404);
    });

    it('Shouldn\'t refresh datalake v2 if we datalake v2 manual refresh toggle is OFF', async () => {
        const next = jest.fn();
        const mock = jest.fn();
        const req = new mock() as Request;
        const res = new mock() as Response;
        let responseStatus: number;

        res.locals = new mock();
        res.json = jest.fn((jsonResponse) => res.json = jsonResponse);
        res.sendStatus = jest.fn().mockImplementation((status) => {
            responseStatus = status;
        });
        const sessionsManagerMock = res.locals.session = new mock() as SessionManager;

        sessionsManagerMock.platform = new mock() as PlatformManager;
        sessionsManagerMock.platform.isDatalakeV2ManualRefreshToggleActive = (): boolean => {
            return false;
        };
        await checkDatalakeV2ManualRefreshToggleActivation(req, res, next);
        // @ts-ignore
        expect(responseStatus).toEqual(404);
        expect(next).toHaveBeenCalledTimes(0);
    });
});
