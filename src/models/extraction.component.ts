import { ReportSchedulationService } from '../services/report-schedulation.service';
import { Dynamo } from '../services/dynamo';
import moment from 'moment-timezone';
import 'moment-recur-ts';
import { PlanningOption, TimeFrameOptions } from './custom-report';
import {
    DataLakeRefreshItem,
    ExtractionMapper,
    ExtractionModel,
    LastRefreshDate
} from '../reports/interfaces/extraction.interface';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import SessionManager from '../services/session/session-manager.session';
import { DataLakeRefreshStatus } from './base';
import { RefreshTokenItem } from '../reports/interfaces/tokens.interface';
import PlatformManager from '../services/session/platform-manager.session';
import { StepFunctionStatuses } from '../services/step-function';
import { redisFactory } from '../services/redis/RedisFactory';

export class ExtractionComponent {
    async performScheduledReportsExport(platforms: string[], isRefreshOnDemand = false): Promise<void> {
        const logger: SessionLoggerService = httpContext.get('logger');
        const platformsListString = platforms.join(', ');
        logger.debug(`Start scheduled reports extraction for these platforms: "${platformsListString}" after a ${isRefreshOnDemand ? 'Refresh On Demand' : 'Timezone Refresh'}`);

        const commonKeys = await this.getPlatformCommonConfig();

        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, '', '', new PlatformManager());

        const platformsSettings = await dynamo.getPlatformsSettings(platforms);
        const filteredPlatforms = platformsSettings.filter(platformSetting => !platformSetting.toggleDatalakeV2).map(platformSetting => platformSetting.platform);
        const filteredPlatformsListString = filteredPlatforms.join(', ');

        logger.debug(`Update update the last refresh date in dynamo for these platforms: "${filteredPlatformsListString}"`);

        // update the last refresh date in dynamo
        await this.updateDataLakeRefreshTimestamp(filteredPlatforms, dynamo, isRefreshOnDemand);

        // detect if we have to run the scheduled reports
        if (!isRefreshOnDemand) {
            logger.debug(`Start the scheduled reports extraction process for these platforms: "${filteredPlatformsListString}"`);
            const reports = await this.getScheduledReports(filteredPlatforms, dynamo, commonKeys.schedulationPrivateKey);
            for (const platform in reports) {
                await (new ReportSchedulationService(platform, commonKeys.schedulationPrivateKey))
                    .requestScheduledReportsExportByPlatform(reports[platform]);
            }
            logger.debug(`Scheduled reports extraction executed for these platforms: "${filteredPlatformsListString}"`);
        }

        logger.debug(`Ended scheduled reports extraction process for these platforms: "${filteredPlatformsListString}"`);
    }

    public async getScheduledReports(platforms: string[], dynamo: Dynamo, schedulationPrivateKey: string): Promise<ExtractionMapper> {
        const logger: SessionLoggerService = httpContext.get('logger');

        let reportsMapByPlatform: ExtractionModel[];
        try {
            reportsMapByPlatform = await dynamo.getScheduledReportByPlatform(platforms);
        } catch (dynamoError: any) {
            logger.errorWithStack(`Error while trying to get scheduled reports from dynamo: ${dynamoError.message}. Platforms: ${platforms.join(', ')}`, dynamoError);
            throw Error(`Error while trying to get scheduled reports from dynamo: ${dynamoError}`);
        }

        return this.filterScheduledByPeriod(new Date(), reportsMapByPlatform, schedulationPrivateKey);
    }

    public async filterScheduledByPeriod(today: Date, reports: ExtractionModel[], schedulationPrivateKey: string, timezone?: string, logger?: SessionLoggerService): Promise<ExtractionMapper> {
        logger = logger ?? httpContext.get('logger') as SessionLoggerService;

        const extractionMapper: ExtractionMapper = {};
        const cacheOwnerTimezone = {};
        let ownerTimezone: string;
        for (const report of reports) {
            if (typeof(timezone) !== 'undefined') {
                ownerTimezone = timezone;
            } else {
                const reportSchedulationService = new ReportSchedulationService(report.platform, schedulationPrivateKey);
                if (cacheOwnerTimezone[report.platform] && cacheOwnerTimezone[report.platform][report.author.toString()]) {
                    ownerTimezone = cacheOwnerTimezone[report.platform][report.author.toString()];
                } else {
                    ownerTimezone = await reportSchedulationService.getOwnerReportTimezone(report.author);
                    cacheOwnerTimezone[report.platform] = cacheOwnerTimezone[report.platform] || {};
                    cacheOwnerTimezone[report.platform][report.author.toString()] = ownerTimezone;
                }
            }

            const now = moment(today).tz(ownerTimezone).startOf('d');

            logger.debug(`For the report ${report.idReport}, platform ${report.platform} we calculate now in the timezone: ${ownerTimezone}. In this timezone 'now' is: ${now.format('YYYY-MM-DD HH:mm:ss')}`);

            const planningOption = report.planning.option as PlanningOption;
            const scheduledFrom = moment(planningOption.scheduleFrom).startOf('d');
            const scheduleInformationLog = `${scheduledFrom.format('YYYY-MM-DD HH:mm:ss')}. Every ${planningOption.every}/${planningOption.timeFrame.toString()}`;

            if (now.isBefore(scheduledFrom) && now.format('YYYY-MM-DD HH:mm:ss') !== scheduledFrom.format('YYYY-MM-DD HH:mm:ss')) {
                logger.debug(`Scheduled report ${report.idReport} skipped for platform ${report.platform}. Reason: now is before of "scheduled from" field. Now is: ${now.format('YYYY-MM-DD HH:mm:ss')} and scheduledFrom is: ${scheduleInformationLog}`);
                continue;
            }

            // check if this report needs to be executed
            if (this.hasToBeExecuted(now, planningOption)) {
                logger.debug(`Scheduled report ${report.idReport} ran for platform ${report.platform}. Was scheduled from ${scheduleInformationLog}`);
                this.addReportToMapper(extractionMapper, report);
            } else {
                logger.debug(`Scheduled report ${report.idReport} skipped for platform ${report.platform}. Now is: ${now.format('YYYY-MM-DD HH:mm:ss')} and scheduledFrom is: ${scheduleInformationLog}`);
            }
        }

        return extractionMapper;
    }

    private hasToBeExecuted(now: moment.Moment, planningOption: PlanningOption): boolean {
        if (!planningOption || planningOption.every <= 0) return false;
        const scheduledFrom = moment(planningOption.scheduleFrom).startOf('d');

        if (now.isSame(scheduledFrom, 'd')) return true;

        switch (planningOption.timeFrame) {
            case TimeFrameOptions.days:
                return scheduledFrom.recur().every(planningOption.every).days().matches(now);
            case TimeFrameOptions.weeks:
                return scheduledFrom.recur().every(planningOption.every).week().matches(now);
            case TimeFrameOptions.months:
                return scheduledFrom.recur().every(planningOption.every).month().matches(now);
        }
    }

    private addReportToMapper(extractionMapper: ExtractionMapper, report: ExtractionModel) {
        extractionMapper[report.platform] = extractionMapper[report.platform] || {};
        extractionMapper[report.platform][report.author] = extractionMapper[report.platform][report.author] || [];
        extractionMapper[report.platform][report.author].push(report.idReport);
    }

    private async getPlatformCommonConfig(): Promise<{[key: string]: string}> {
        try {
            return await redisFactory.getRedis().getRedisCommonParams();
        } catch (e: any) {
            const logger: SessionLoggerService = httpContext.get('logger');
            logger.errorWithStack('Error while attempting to read redis main configuration.', e);

            throw new Error(`Error while attempting to read redis main configuration: ${e}`);
        }
    }

    public async updateDataLakeRefreshTimestamp(platforms: string[], dynamo: Dynamo, isRefreshOnDemand: boolean): Promise<void> {
        // prepare the items to write or replace
        // get current situation on datalake_refresh_info dynamodb table
        const res = await this.getDataLakePlatformsRefreshDetails(platforms, dynamo);
        const now = moment.utc().format('YYYY-MM-DD HH:mm:ss');
        const items: DataLakeRefreshItem[] = platforms.reduce(
            (previousValue: DataLakeRefreshItem[], currentValue) => {
                let actualDatalakeRefreshItem = res[currentValue];
                if (!actualDatalakeRefreshItem) {
                    actualDatalakeRefreshItem = {platform: currentValue} as DataLakeRefreshItem;
                }
                const datalakeRefreshInfo = actualDatalakeRefreshItem;
                // update datalake refresh item
                datalakeRefreshInfo.refreshOnDemandLastDateUpdate = now;
                datalakeRefreshInfo.refreshTimezoneLastDateUpdate = now;
                datalakeRefreshInfo.refreshOnDemandStatus = DataLakeRefreshStatus.RefreshSucceeded;
                datalakeRefreshInfo.refreshTimeZoneStatus = DataLakeRefreshStatus.RefreshSucceeded;
                previousValue.push(datalakeRefreshInfo);
                return previousValue;
            }, []);

        await dynamo.batchWriteDataLakeUpdates(items);
    }

    public async startDataLakeRefresh(platforms: string[], isRefreshOnDemand: boolean, session?: SessionManager): Promise<void> {
        const logger: SessionLoggerService = httpContext.get('logger');
        const platformsListString = platforms.join(', ');
        logger.debug(`Data lake refresh requested${isRefreshOnDemand ? ' - Refresh on demand' : ''}  for these platforms: "${platformsListString}"`);

        let dynamo: Dynamo;
        if (session) {
            dynamo = session.getDynamo();
        } else {
            const commonKeys = await this.getPlatformCommonConfig();
            dynamo = new Dynamo(commonKeys.dynamoDbRegion, '', '', new PlatformManager());
        }

        const platformsSettings = await dynamo.getPlatformsSettings(platforms);
        const filteredPlatforms = platformsSettings.filter(platformSetting => !platformSetting.toggleDatalakeV2).map(platformSetting => platformSetting.platform);
        const filteredPlatformsListString = filteredPlatforms.join(', ');
        logger.debug(`Set refresh in progress in dynamo for these platforms: "${filteredPlatformsListString}"`);

        const updatedItems: DataLakeRefreshItem[] = [];
        const now = moment.utc().format('YYYY-MM-DD HH:mm:ss');

        const dataLakeRefreshItems = await this.getDataLakePlatformsRefreshDetails(filteredPlatforms, dynamo);
        for (const platformName of filteredPlatforms) {
            let detail = dataLakeRefreshItems[platformName];
            if (detail) {
                if (isRefreshOnDemand) {
                    detail.refreshOnDemandStatus = DataLakeRefreshStatus.RefreshInProgress;
                    detail.refreshOnDemandLastDateUpdate = now;
                } else {
                    detail.refreshTimeZoneStatus = DataLakeRefreshStatus.RefreshInProgress;
                    detail.refreshTimezoneLastDateUpdate = now;
                    if (session?.platform.isDatalakeV2Active()) {
                        detail.lastRefreshStartDate = now;
                    }

                    // set the ROD to error, so we can kill any possible background job
                    detail.refreshOnDemandStatus = DataLakeRefreshStatus.RefreshError;
                    detail.refreshOnDemandLastDateUpdate = now;
                }
            } else {

                if (isRefreshOnDemand) {
                    detail = {
                        platform: platformName,
                        refreshOnDemandStatus: DataLakeRefreshStatus.RefreshInProgress,
                        refreshOnDemandLastDateUpdate: now,
                    };
                } else {
                    detail = {
                        platform: platformName,
                        refreshTimeZoneStatus: DataLakeRefreshStatus.RefreshInProgress,
                        refreshTimezoneLastDateUpdate: now,
                    };

                    if (session.platform.isDatalakeV2Active()) {
                        detail.lastRefreshStartDate = now;
                    }
                }

            }
            updatedItems.push(detail);
        }

        await dynamo.batchWriteDataLakeUpdates(updatedItems);

    }

    public async getDataLakePlatformsRefreshDetails(platforms: string[], dynamo: Dynamo): Promise<{ [key: string]: DataLakeRefreshItem }> {
        const res: { [key: string]: DataLakeRefreshItem } = {};
        for (const item of await dynamo.getDataLakeRefreshDetails(platforms)) {
            res[item.platform] = item;
        }
        return res;
    }

    public async getDataLakeLastRefresh(session: SessionManager): Promise<LastRefreshDate> {
        const dynamo = session.getDynamo();
        const refreshTime = await dynamo.getLastDataLakeUpdate(session);
        const platformUrl = session.platform.getPlatformBaseUrl();
        const response: LastRefreshDate = {refreshDate: ''};
        const logger: SessionLoggerService = httpContext.get('logger');

        if (session.platform.isDatalakeV3ToggleActive()) {
            // We are calling this one here because in this function there's also an additional check to create the refresh info row on Dynamo if it wasn't present
            const lastRefreshTime = await dynamo.getDatalakeV3LastRefreshTime();

            const snowflakeDriver = session.getSnowflake();
            const refreshDetails = await snowflakeDriver.getLastRefreshDetails();

            if (refreshDetails.status !== 'complete') {
                response.refreshDate = lastRefreshTime;
            } else {
                await dynamo.updateDatalakeV3LastRefreshTime(refreshDetails.lastRefreshStart);
                response.refreshDate = refreshDetails.lastRefreshStart;
            }

            response.refreshStatus = DataLakeRefreshStatus.RefreshSucceeded;

            return response;
        }

        // manage the 'no state yet' of the data lake
        if (!refreshTime || (!refreshTime.refreshOnDemandStatus && !refreshTime.refreshTimeZoneStatus)) {
            response.refreshStatus = undefined;
            if (session.platform.isDatalakeV2Active()) {
                response.isRefreshNeeded = true;
                response.errorCount = 0;
            }
            return response;
        }

        // If pass more the X hours for the nightly refresh return the refresh status on nightly error
        if (typeof refreshTime !== undefined) {
            // Get the timeout limit if set in Redis
            const redisPlatformParams = await redisFactory.getRedis().getRedisPlatformParams(platformUrl);
            const timeoutLimit = parseInt(redisPlatformParams.aamonDatalakeNightlyRefreshTimeout, 10);

            const now = moment().utc();
            const timezoneLastUpdate = refreshTime.refreshTimezoneLastDateUpdate ? refreshTime.refreshTimezoneLastDateUpdate : 0;
            const timeZoneLastUpdateLimit = moment(timezoneLastUpdate).add(timeoutLimit, 'minutes');
            const isRefreshTimezoneInProgress = refreshTime.refreshTimeZoneStatus === DataLakeRefreshStatus.RefreshInProgress;
            const isRefreshOnDemandInProgress = refreshTime.refreshOnDemandStatus === DataLakeRefreshStatus.RefreshInProgress;

            if (!session.platform.isDatalakeV2Active() && moment(now).isAfter(timeZoneLastUpdateLimit) && isRefreshTimezoneInProgress && !isRefreshOnDemandInProgress) {
                logger.error(`Nightly refresh timeout error. Timezone last update: ${timezoneLastUpdate} | Timeout refresh limit: ${timeoutLimit} minutes`);

                // Set error on refreshTimeZoneStatus field in dynamo
                await this.setDataLakeStatusToError([platformUrl]);

                response.refreshStatus = DataLakeRefreshStatus.RefreshError;
                response.refreshDate = refreshTime.refreshOnDemandLastDateUpdate;
                if (session.platform.isDatalakeV2Active()) {
                    response.isRefreshNeeded = true;
                    response.errorCount = refreshTime.errorCount;
                }
                return response;
            }
        }
        let installationType = '';
        if (session.platform.isDatalakeV2Active()) {
            try {
                if (refreshTime.refreshTimeZoneStatus === DataLakeRefreshStatus.RefreshInProgress && refreshTime.stepFunctionExecutionId) {
                    const executionStatus = await session.getStepFunction().getExecutionStatus(refreshTime.stepFunctionExecutionId);
                    if (executionStatus === StepFunctionStatuses.SUCCEEDED) {
                        response.refreshStatus = DataLakeRefreshStatus.RefreshSucceeded;
                        refreshTime.refreshTimeZoneStatus = DataLakeRefreshStatus.RefreshSucceeded;
                        refreshTime.refreshOnDemandStatus = DataLakeRefreshStatus.RefreshSucceeded;
                        await session.getDynamo().forceUpdateDataLakeNightlyRefreshStatus(DataLakeRefreshStatus.RefreshSucceeded);
                    } else if (executionStatus !== StepFunctionStatuses.RUNNING) {
                        response.refreshStatus = DataLakeRefreshStatus.RefreshError;
                        refreshTime.refreshTimeZoneStatus = DataLakeRefreshStatus.RefreshError;
                        refreshTime.refreshOnDemandStatus = DataLakeRefreshStatus.RefreshError;
                        await session.getDynamo().forceUpdateDataLakeNightlyRefreshStatus(DataLakeRefreshStatus.RefreshError);
                    }
                }
            } catch (error: any) {
                logger.errorWithStack(`Error on retrieve Step Function execution status for platform ${platformUrl} and execution ARN ${refreshTime.stepFunctionExecutionId}. Error message: ${error.message}`, error);
            }

            try {
                installationType = (await session.getHydra().getInstallationType()).data.installationType;
            } catch (error: any) {
                logger.errorWithStack(`Error on retrieve installation type for the platform ${platformUrl}. Error message: ${error.message}`, error);
                installationType = '';
            }
        }
        return this.detectDataLakeState(refreshTime, session, installationType);
    }

    public detectDataLakeState(dataLakeState: DataLakeRefreshItem, session: SessionManager, installationType: string): LastRefreshDate {
        const response: LastRefreshDate = {refreshDate: ''};
        // look for a RefreshInProgress state
        if (dataLakeState.refreshOnDemandStatus === DataLakeRefreshStatus.RefreshInProgress || dataLakeState.refreshTimeZoneStatus === DataLakeRefreshStatus.RefreshInProgress) {
            response.refreshStatus = DataLakeRefreshStatus.RefreshInProgress;
            if (session.platform.isDatalakeV2Active()) {
                response.isRefreshNeeded = false;
                response.errorCount = dataLakeState.errorCount;
            }
            return response;
        }

        // prevent the undefined dates - in moment they become like a new Date()
        const timezoneLastUpdate = dataLakeState.refreshTimezoneLastDateUpdate ? dataLakeState.refreshTimezoneLastDateUpdate : 0;
        const onDemandLastUpdate = dataLakeState.refreshOnDemandLastDateUpdate ? dataLakeState.refreshOnDemandLastDateUpdate : 0;

        if (moment(timezoneLastUpdate).isAfter(onDemandLastUpdate)) {
            response.refreshStatus = dataLakeState.refreshTimeZoneStatus;
            response.refreshDate = dataLakeState.refreshTimezoneLastDateUpdate ? dataLakeState.refreshTimezoneLastDateUpdate : '';

            if (session.platform.isDatalakeV2Active()) {
                const now = moment().utc();
                response.isRefreshNeeded = now.diff(moment(timezoneLastUpdate), 'seconds') >= session.platform.getDatalakeV2ExpirationTime(installationType) || dataLakeState.refreshTimeZoneStatus === DataLakeRefreshStatus.RefreshError;
            }
        } else {
            response.refreshStatus = dataLakeState.refreshOnDemandStatus;
            response.refreshDate = dataLakeState.refreshOnDemandLastDateUpdate ? dataLakeState.refreshOnDemandLastDateUpdate : '';
            response.lastRefreshStartDate = dataLakeState.lastRefreshStartDate ? dataLakeState.lastRefreshStartDate : '';

            if (session.platform.isDatalakeV2Active()) {
                const now = moment().utc();
                response.isRefreshNeeded = now.diff(moment(onDemandLastUpdate), 'seconds') >= session.platform.getDatalakeV2ExpirationTime(installationType) || dataLakeState.refreshOnDemandStatus === DataLakeRefreshStatus.RefreshError;
            }
        }
        if (session.platform.isDatalakeV2Active()) {
            response.errorCount = dataLakeState.errorCount;
        }

        return response;

    }

    /**
     * Set the data lake refresh status to error for the specified platforms
     * @param platforms {string[]} The list of platforms to set to error
     * @param isRefreshOnDemand {boolean} Define if it is a refresh on demand that causes the error
     */
    async setDataLakeStatusToError(platforms: string[], isRefreshOnDemand = false) {
        // first of all - get all teh details of the platforms
        const commonKeys = await this.getPlatformCommonConfig();
        const dynamo: Dynamo = new Dynamo(commonKeys.dynamoDbRegion, '', '', new PlatformManager());
        const updatedItems: DataLakeRefreshItem[] = [];

        const platformsSettings = await dynamo.getPlatformsSettings(platforms);
        const filteredPlatforms = platformsSettings.filter(platformSetting => !platformSetting.toggleDatalakeV2).map(platformSetting => platformSetting.platform);

        const dataLakeRefreshItems = await this.getDataLakePlatformsRefreshDetails(filteredPlatforms, dynamo);
        const now = moment.utc().format('YYYY-MM-DD HH:mm:ss');

        // update the status of each platform to error - based on isRefreshOnDemand or not
        for (const platformName in dataLakeRefreshItems) {
            const detail: DataLakeRefreshItem = dataLakeRefreshItems[platformName];
            if (isRefreshOnDemand) {
                detail.refreshOnDemandStatus = DataLakeRefreshStatus.RefreshError;
                detail.refreshOnDemandLastDateUpdate = now;
            } else {
                detail.refreshTimeZoneStatus = DataLakeRefreshStatus.RefreshError;
                detail.refreshTimezoneLastDateUpdate = now;
            }
            updatedItems.push(detail);
        }

        // write the updated items to dynamo
        await dynamo.batchWriteDataLakeUpdates(updatedItems);
    }


    // Restore the refresh tokens after ingestion error
    public async restoreRefreshTokensAfterIngestionError(platforms: string[]): Promise<void> {
        const commonKeys = await this.getPlatformCommonConfig();
        const dynamo: Dynamo = new Dynamo(commonKeys.dynamoDbRegion, '', '', new PlatformManager());
        const updatedItems: RefreshTokenItem[] = [];

        // get the list of refresh token items of each platforms
        const dataLakeRefreshTokensItems = await this.getDataLakePlatformsRefreshTokenItems(platforms, dynamo);
        const now = moment.utc().format('YYYY-MM-DD HH:mm:ss');

        // update the refresh token status of each platform
        for (const platformName in dataLakeRefreshTokensItems) {

            const reportSchedulationService = new ReportSchedulationService(platformName, commonKeys.schedulationPrivateKey);
            await reportSchedulationService.authenticateAgainstPlatformAsSA();

            const refreshTokenItem: RefreshTokenItem = dataLakeRefreshTokensItems[platformName];

            refreshTokenItem.currentDailyTokens = refreshTokenItem.currentDailyTokens + 1;
            refreshTokenItem.currentMonthlyTokens = refreshTokenItem.currentMonthlyTokens + 1;
            refreshTokenItem.lastRequest = now;

            updatedItems.push(refreshTokenItem);
        }

        // write the updated items to dynamo
        dynamo.batchWriteRestoreRefreshTokens(updatedItems);
    }

    public async getDataLakePlatformsRefreshTokenItems(platforms: string[], dynamo: Dynamo): Promise<{ [key: string]: RefreshTokenItem }> {
        const res: { [key: string]: RefreshTokenItem } = {};
        for (const item of await dynamo.getRefreshTokenItems(platforms)) {
            res[item.platform] = item;
        }
        return res;
    }

    /**
     * If the datalake refresh status is equal to "Error", we try to call the datalake step function.
     * @param session
     * @param refreshInfo
     */
    public async startDataLakeRefreshIfStatusIsError(session: SessionManager, refreshInfo: LastRefreshDate) {
        if (session.platform.isDatalakeV2Active() && refreshInfo.refreshStatus === DataLakeRefreshStatus.RefreshError) {
            const sqs = session.getSQS();

            try {
                const isRunning = await sqs.runDataLakeV2Refresh(session, refreshInfo);

                if (isRunning) {
                    refreshInfo.refreshStatus = DataLakeRefreshStatus.RefreshInProgress;
                }
            } catch (e: any) {
                return refreshInfo;
            }
        }
        return refreshInfo;
    }
}
