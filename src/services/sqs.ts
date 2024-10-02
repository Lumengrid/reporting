import AWS from 'aws-sdk';
import Config from '../config';
import { SessionLoggerService } from './logger/session-logger.service';
import httpContext from 'express-http-context';
import SessionManager from './session/session-manager.session';
import { LastRefreshDate } from '../reports/interfaces/extraction.interface';
import { STEP_FUNCTION_MAX_ATTEMPTS } from '../shared/constants';
import { DataLakeRefreshStatus } from '../models/base';
import { v1, v4 } from 'uuid';
import moment from 'moment';

const config = new Config();

export interface SQSParams {
    QueueUrl: string;
    MessageBody: string;
    MessageGroupId?: string;
    MessageDeduplicationId?: string;
}

export class SQS {
    protected region: string;
    protected logger: SessionLoggerService;

    protected sqs: AWS.SQS;

    public constructor(region: string) {
        this.region = region;

        const awsCredentials = {
            region: this.region
        };
        AWS.config.update(awsCredentials);
        // AWS.config.logger = console; // Uncomment this to have a full log in console for the AWS sdk
        this.sqs = new AWS.SQS();
        this.logger = httpContext.get('logger');
    }

    /**
     * Call the step function.
     * Returns true if it is called, false otherwise
     * @param session
     * @param refreshInfo
     * @param isRefreshOnDemand
     */
    public async runDataLakeV2Refresh(session: SessionManager, refreshInfo: LastRefreshDate, isRefreshOnDemand = false): Promise<boolean> {
        if (session.platform.isDatalakeV3ToggleActive()) {
            return false;
        }

        if (
            (refreshInfo.refreshStatus === DataLakeRefreshStatus.RefreshError || refreshInfo.refreshDate === '') &&
            !await this.canSendMessageAgain(refreshInfo, session)
        ) {
            return false;
        }

        let message = 'Automatic refresh is needed';
        if (isRefreshOnDemand === true) {
            message = 'Refresh on demand request';
        }
        try {
            await session.getDynamo().updateDataLakeNightlyRefreshStatus(DataLakeRefreshStatus.RefreshInProgress);
        } catch (error: any) {
            if (error.message.search('conditional request failed') >= 0) {
                this.logger.debug('Step function run skipped for platform ' + session.platform.getPlatformBaseUrl() + ' because already requested.');
                return true;
            }
            throw (error);
        }
        try {
            const params: SQSParams = {
                QueueUrl: config.getSqsDatalakeV25Refresh(),
                MessageBody: JSON.stringify({
                    original_domain: session.platform.getPlatformBaseUrl(),
                    schema_name: session.platform.getAthenaSchemaNameOverride() !== '' ? session.platform.getAthenaSchemaNameOverride() : session.platform.getAthenaSchemaName(),
                    datalake_info_table_name: config.getDataLakeRefreshInfoTableName(),
                    bucket: session.platform.getDatalakeV2DataBucket(),
                    db_host: session.platform.getDatalakeV2Host() !== '' ? session.platform.getDatalakeV2Host() : (session.platform.getDbHostOverride() !== '' ? session.platform.getDbHostOverride() : session.platform.getDbHost()),
                }),
                MessageGroupId: 'main',
                MessageDeduplicationId: v1() + v4()
            };

           await this.sqs.sendMessage(params).promise();
           this.logger.debug(`${message}. Refresh messsage sent to SQS queue: ${params.MessageDeduplicationId}, platform: ${session.platform.getPlatformBaseUrl()}`);
        } catch (error: any) {
            await session.getDynamo().forceUpdateDataLakeNightlyRefreshStatus(DataLakeRefreshStatus.RefreshError);
            this.logger.errorWithStack('Error on sending SQS message to the queue', error);
            throw error;
        }

        return true;
    }

    public async canSendMessageAgain(refreshInfo: LastRefreshDate, session: SessionManager) {
        if (!refreshInfo.errorCount || refreshInfo.errorCount < STEP_FUNCTION_MAX_ATTEMPTS) {
            return true;
        }
        if (moment().utc() > moment(refreshInfo.lastRefreshStartDate).add(20, 'minutes')) {
            await session.getDynamo().restartDataLakeErrorCount();
            return true;
        }

        return false;
    }
}
