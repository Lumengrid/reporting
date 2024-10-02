import { SessionLoggerService } from '../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import Hydra from '../services/hydra';
import SessionManager from '../services/session/session-manager.session';
import AWS from 'aws-sdk';
import { ExtractionComponent } from './extraction.component';
import { DataLakeRefreshStatus } from './base';
import { Dynamo } from '../services/dynamo';
import moment from 'moment';
import Config from '../config';
import SQS from 'aws-sdk/clients/sqs';
import { DatalakeRefreshTokens } from './datalake-refresh-tokens';

export class RefreshDataLake {
    logger: SessionLoggerService;
    hydraService: Hydra;
    sqs: SQS;
    extractionComponent: ExtractionComponent;
    session: SessionManager;
    dynamo: Dynamo;

    constructor(session: SessionManager) {
        this.logger = httpContext.get('logger');
        this.session = session;
        this.hydraService = session.getHydra();
        this.sqs = new AWS.SQS();
        this.extractionComponent = new ExtractionComponent();
        this.dynamo = session.getDynamo();
    }

    async startRefreshDataLake(bjName?: string): Promise<any> {

        // scale the refresh token for day and month. If there aren't more token return false e stop the function
        const datalakeRefreshTokens = new DatalakeRefreshTokens(this.session);
        const isRefreshTokenAvailable = await datalakeRefreshTokens.useRefreshToken();

        if (!isRefreshTokenAvailable) {
            throw new Error('No more token available to perform the datalake refresh on demand');
        }


        // get the platform info
        const installationType = await this.getLmsSetting();
        this.logger.debug(`Lms installation type: ${installationType}`);
        // call the refresh lambda
        await this.sendMessageInSQSRODQueue(this.session.platform.getPlatformBaseUrl(), installationType);
        // refresh the platform status
        await this.extractionComponent.startDataLakeRefresh([this.session.platform.getPlatformBaseUrl()], true, this.session);
        // execute the background job
        await this.startRefreshBackgroundJob(bjName);

    }

    public async getLmsSetting(): Promise<string> {
        const response = await this.hydraService.getInstallationType();
        if (!response.data.installationType) {
            throw new Error('LMS with a null installation type setting');
        }

        return response.data.installationType;
    }

    /**
     * It sends a message in the SQS queue of the Refresh On Demand
     * @param platformName The platform name
     * @param installationType Installation type of the platform (gsc, staging, ecs ...)
     */
    public async sendMessageInSQSRODQueue(platformName: string, installationType: string): Promise<void> {

        const config = new Config();

        const payload = {
            platform_name: platformName,
            platform_type: installationType,
        };

        await this.sqs.sendMessage({
            QueueUrl: config.getStartRefreshSQSUrl(),
            MessageBody: JSON.stringify(payload),
        }).promise();
    }

    /**
     * Check the status of the data lake refresh and if the lastEditDate hit the timeout sets
     * the status of the refresh to RefreshError - called from the refresh data lake background job
     */
    public async checkRefreshStatus(session: SessionManager): Promise<DataLakeRefreshStatus | undefined> {
        const platformRefreshDetail = await this.dynamo.getLastDataLakeUpdate(session);
        if (!platformRefreshDetail) {
            throw new Error('No detail for the refresh status of the platform');
        }

        const now = moment().utc();
        // 5 hours of timeout from the refreshStatusLastEditDate
        const timeoutDate = moment(platformRefreshDetail.refreshOnDemandLastDateUpdate).add(5, 'h');

        // detect if we have to set the status to error because of the timeout
        if (!session.platform.isDatalakeV2Active() && platformRefreshDetail.refreshOnDemandStatus === DataLakeRefreshStatus.RefreshInProgress && now.isAfter(timeoutDate)) {
            // set the state of the data lake to an error state
            await this.dynamo.updateDataLakeRefreshOnDemandStatus(DataLakeRefreshStatus.RefreshError);
            this.logger.error(`Timeout - Set the data lake refresh status to ${DataLakeRefreshStatus.RefreshError}`);

            platformRefreshDetail.refreshOnDemandStatus = DataLakeRefreshStatus.RefreshError;
        }

        return platformRefreshDetail.refreshOnDemandStatus;
    }

    public async startRefreshBackgroundJob(bjName?: string): Promise<any> {

        let payload: any = {
            name: bjName ? bjName : 'Reports Data Refresh',
            type: 'importer',
            method: 'GET',
            notify: true,
            notify_type: 'inbox',
            chunk_size: 1,
            endpoint: '/analytics/v1/exports/polling/echo',
            data_source: {
                type: 'report_refresh_on_demand',
                datasource_params: {
                    authorId: this.session.user.getIdUser(),
                }
            }
        };
        // add email notification if the user has the email
        if (this.session.user.getEMail()) {
            payload = {
                ...payload,
                notify_email: this.session.user.getEMail(),
                notify_type: 'email_and_inbox',
            };
        }

        await this.hydraService.createBackgroundJob(payload);
    }

    /**
     * Get latest datalake refresh status.
     * If the refreshTimezoneLastDateUpdate is equal to refreshOnDemandLastDateUpdate and at least one status is in
     * progress we consider "InProgress" as status, otherwise we consider the refreshOnDemandLastDateUpdate
     */
    public async getLatestDatalakeStatus(session: SessionManager): Promise<null|string> {
        const refreshStatus = await this.dynamo.getLastDataLakeUpdate(session);
        if (!refreshStatus || (!refreshStatus.refreshOnDemandStatus && !refreshStatus.refreshTimeZoneStatus)) {
            return;
        }

        const isInProgress = refreshStatus.refreshTimeZoneStatus === DataLakeRefreshStatus.RefreshInProgress || refreshStatus.refreshOnDemandStatus === DataLakeRefreshStatus.RefreshInProgress;

        if (moment(refreshStatus.refreshTimezoneLastDateUpdate).isSame(moment(refreshStatus.refreshOnDemandLastDateUpdate)) && isInProgress) {
            return DataLakeRefreshStatus.RefreshInProgress;
        }

        if (moment(refreshStatus.refreshTimezoneLastDateUpdate).isAfter(refreshStatus.refreshOnDemandLastDateUpdate)) {
            return refreshStatus.refreshTimeZoneStatus;
        } else {
            return refreshStatus.refreshOnDemandStatus;
        }
    }
}

