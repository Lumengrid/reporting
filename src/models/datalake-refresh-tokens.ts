import 'moment-timezone';

import httpContext from 'express-http-context';
import moment from 'moment';

import { PlatformRefreshTokens, RefreshTokenItem } from '../reports/interfaces/tokens.interface';
import { Dynamo } from '../services/dynamo';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import PlatformManager from '../services/session/platform-manager.session';
import SessionManager from '../services/session/session-manager.session';
import { DAILY_REFRESH_TOKENS, MONTHLY_REFRESH_TOKENS } from '../shared/constants';
import { ReportsSettings } from './base';

export class DatalakeRefreshTokens {
    private logger: SessionLoggerService;
    private dynamo: Dynamo;
    private platform: PlatformManager;
    private platformRefreshTokens: PlatformRefreshTokens;
    private currentRefreshTokens: RefreshTokenItem;

    constructor(session: SessionManager) {
        this.logger = httpContext.get('logger');
        this.platform = session.platform;
        this.dynamo = session.getDynamo();
        this.platformRefreshTokens = {} as PlatformRefreshTokens;
        this.currentRefreshTokens = {} as RefreshTokenItem;
    }

    // Return the number of the current refresh tokens available per day
    public async getCurrentRefreshTokens(): Promise<{ [key: string]: number }> {

        // Get the default refresh tokens available for this platform
        this.platformRefreshTokens = this.platform.getRefreshTokens() as PlatformRefreshTokens;

        // Get the current status of the refresh tokens stored in dynamo
        this.currentRefreshTokens = await this.dynamo.getRefreshTokensStatus() as RefreshTokenItem;

        // If first time create new record in dynamo to store tokens and return the platform daily token
        if (!this.currentRefreshTokens) {

            // Create a new record to store in dynamo refresh tokens of this platform
            this.resetMonthlyRefreshTokens();

            return {
                dailyRefreshTokens: this.platformRefreshTokens.platformDailyTokens,
                monthlyRefreshTokens: this.platformRefreshTokens.platformMonthlyTokens,
            };
        }

        // Reset daily or monthly refresh tokens if there are conditions
        this.resetSwitcher();

        return {
            dailyRefreshTokens: this.currentRefreshTokens.currentDailyTokens,
            monthlyRefreshTokens: this.currentRefreshTokens.currentMonthlyTokens,
        };

    }

    // Make a monthly or daily reset
    private resetSwitcher(): void {
        const { lastRequest, lastReset, currentMonthlyTokens } = this.currentRefreshTokens;

        const platformTimezone = this.platform.getDefaultPlatformTimezone();

        const currentDay = moment().utc().tz(platformTimezone).format('YYYY-MM-DD');
        const currentMonth = moment().utc().tz(platformTimezone).format('YYYY-MM');
        const lastRequestDay = moment(lastRequest).tz(platformTimezone).format('YYYY-MM-DD');
        const lastResetMonth = moment(lastReset).tz(platformTimezone).format('YYYY-MM');

        const isDayChanged = moment(currentDay).isAfter(lastRequestDay);
        const isMonthChanged = moment(currentMonth).isAfter(lastResetMonth);

        // If new month reset all and return
        if (isMonthChanged) {
            this.resetMonthlyRefreshTokens();
            return;
        }

        // if Monthly tokens are over set the daily ones to 0
        if (currentMonthlyTokens === 0) {
            this.currentRefreshTokens.currentDailyTokens = 0;
            return;
        }

        // If the day is changed and there are monthly tokens available reset the daily tokens
        if (isDayChanged && (currentMonthlyTokens > 0)) {
            this.resetDailyRefreshTokens();
        }

    }

    public async resetDailyRefreshTokens(): Promise<void> {
        const {currentMonthlyTokens, lastReset} = this.currentRefreshTokens;
        const {platformDailyTokens} = this.platformRefreshTokens;

        // Reset the daily and last request in dynamo
        const today = moment.utc().format('YYYY-MM-DD HH:mm:ss');
        const dailyResetPayload: RefreshTokenItem = {
            currentDailyTokens: platformDailyTokens,
            lastRequest: today,
            currentMonthlyTokens,
            lastReset,
            platform: this.platform.getPlatformBaseUrl()
        };

        try {
            // Update the count of the daily tokens and the status in dynamo
            this.currentRefreshTokens.currentDailyTokens = platformDailyTokens;
            await this.dynamo.updateRefreshTokens(dailyResetPayload);
        } catch (err: any) {
            this.logger.errorWithStack(`Error while while resetting daily refresh tokens on DynamoDB`, err);
            throw new Error(`Error while while resetting daily refresh tokens on DynamoDB: ${err}`);
        }

    }

    // Refresh tokens will be reset every month
    public async resetMonthlyRefreshTokens(): Promise<void> {

        const {platformDailyTokens, platformMonthlyTokens } = this.platformRefreshTokens;

        // Reset all the fields in dynamo
        const today = moment.utc().format('YYYY-MM-DD HH:mm:ss');
        const monthlyResetPayload: RefreshTokenItem = {
            currentMonthlyTokens: platformMonthlyTokens,
            currentDailyTokens: platformDailyTokens,
            lastReset: today,
            lastRequest: today,
            platform: this.platform.getPlatformBaseUrl()
        };

        try {

            // Update the count of the refresh tokens and the status in dynamo
            if (this.currentRefreshTokens) {
                this.currentRefreshTokens.currentDailyTokens = platformDailyTokens;
                this.currentRefreshTokens.currentMonthlyTokens = platformMonthlyTokens;
            }

            await this.dynamo.updateRefreshTokens(monthlyResetPayload);

        } catch (err: any) {
            this.logger.errorWithStack(`Update error with monthly reset tokens on DynamoDB`, err);
            throw new Error(`Update error with monthly reset tokens on DynamoDB: ${err}`);
        }

    }

    // Remove the daily token and recalculate the monthly ones
    public async useRefreshToken(): Promise<boolean> {

        // Get the current token status
        const {currentMonthlyTokens, currentDailyTokens, lastReset} = await this.dynamo.getRefreshTokensStatus() as RefreshTokenItem;

        if (currentDailyTokens === 0 || currentMonthlyTokens === 0) {
            this.logger.debug(`No token available for today. Monthly status: ${currentMonthlyTokens} tokens available`);
            return false;
        }

        const calcMonthTokens = currentMonthlyTokens - 1;
        const calcDailyTokens = currentDailyTokens - 1;

        // Update the refresh tokens except for the lastReset
        const today = moment.utc().format('YYYY-MM-DD HH:mm:ss');
        const refreshTokenPayload: RefreshTokenItem = {
            currentMonthlyTokens: calcMonthTokens,
            currentDailyTokens: calcDailyTokens,
            lastRequest: today,
            lastReset,
            platform: this.platform.getPlatformBaseUrl()
        };

        try {
            await this.dynamo.updateRefreshTokens(refreshTokenPayload);
            return true;
        } catch (err: any) {
            this.logger.errorWithStack(`Error while updating refresh tokens on DynamoDB`, err);
            throw new Error(`Error while updating refresh tokens on DynamoDB ${err}`);
        }
    }

    // Reset tokens after default limit has changed (i.e. putReportSettingsUpdate)
    public async resetTokens(platformSettings: ReportsSettings): Promise<void> {

        // Reset all the fields in dynamo
        const today = moment.utc().format('YYYY-MM-DD HH:mm:ss');
        const tokenResetPayload: RefreshTokenItem = {
            currentMonthlyTokens: platformSettings.monthlyRefreshTokens || MONTHLY_REFRESH_TOKENS,
            currentDailyTokens: platformSettings.dailyRefreshTokens || DAILY_REFRESH_TOKENS,
            lastReset: today,
            lastRequest: today,
            platform: this.platform.getPlatformBaseUrl()
        };

        try {
            await this.dynamo.updateRefreshTokens(tokenResetPayload);

        } catch (err: any) {
            this.logger.errorWithStack(`Update error with monthly reset tokens on DynamoDB`, err);
            throw new Error(`Update error with monthly reset tokens on DynamoDB: ${err}`);
        }
    }

}