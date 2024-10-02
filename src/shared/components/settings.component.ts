import { Request } from 'express';
import { ReportsSettings, UserLevelsGroups } from '../../models/base';
import SessionManager from '../../services/session/session-manager.session';
import { redisFactory } from '../../services/redis/RedisFactory';

export class SettingsComponent {
    req: Request;
    session: SessionManager;

    constructor(req: Request, session: SessionManager) {
        this.req = req;
        this.session = session;
    }

    public async updateSettings(): Promise<ReportsSettings> {
        let settings = await this.session.getDynamo().getSettings() as ReportsSettings;

        if (this.req.body.csvExportLimit || this.req.body.csvExportLimit === 0) {
            if (this.req.body.csvExportLimit === 0 && settings.csvExportLimit) {
                delete settings.csvExportLimit;
            }
            if (this.req.body.csvExportLimit > 0) {
                settings.csvExportLimit = this.req.body.csvExportLimit;
            }
        }
        if (this.req.body.xlxExportLimit || this.req.body.xlxExportLimit === 0) {
            if (this.req.body.xlxExportLimit === 0 && settings.xlxExportLimit) {
                delete settings.xlxExportLimit;
            }
            if (this.req.body.xlxExportLimit > 0) {
                settings.xlxExportLimit = this.req.body.xlxExportLimit;
            }
        }
        if (this.req.body.previewExportLimit || this.req.body.previewExportLimit === 0) {
            if (this.req.body.previewExportLimit === 0 && settings.previewExportLimit) {
                delete settings.previewExportLimit;
            }
            if (this.req.body.previewExportLimit > 0) {
                settings.previewExportLimit = this.req.body.previewExportLimit;
            }
        }
        if (this.req.body.entityUsersLimit || this.req.body.entityUsersLimit === 0) {
            if (this.req.body.entityUsersLimit === 0 && settings.entityUsersLimit) {
                delete settings.entityUsersLimit;
            }
            if (this.req.body.entityUsersLimit > 0) {
                settings.entityUsersLimit = this.req.body.entityUsersLimit;
            }
        }
        if (this.req.body.entityGroupsLimit || this.req.body.entityGroupsLimit === 0) {
            if (this.req.body.entityGroupsLimit === 0 && settings.entityGroupsLimit) {
                delete settings.entityGroupsLimit;
            }
            if (this.req.body.entityGroupsLimit > 0) {
                settings.entityGroupsLimit = this.req.body.entityGroupsLimit;
            }
        }
        if (this.req.body.entityBranchesLimit || this.req.body.entityBranchesLimit === 0) {
            if (this.req.body.entityBranchesLimit === 0 && settings.entityBranchesLimit) {
                delete settings.entityBranchesLimit;
            }
            if (this.req.body.entityBranchesLimit > 0) {
                settings.entityBranchesLimit = this.req.body.entityBranchesLimit;
            }
        }
        if (this.req.body.entityCoursesLimit || this.req.body.entityCoursesLimit === 0) {
            if (this.req.body.entityCoursesLimit === 0 && settings.entityCoursesLimit) {
                delete settings.entityCoursesLimit;
            }
            if (this.req.body.entityCoursesLimit > 0) {
                settings.entityCoursesLimit = this.req.body.entityCoursesLimit;
            }
        }
        if (this.req.body.entityLPLimit || this.req.body.entityLPLimit === 0) {
            if (this.req.body.entityLPLimit === 0 && settings.entityLPLimit) {
                delete settings.entityLPLimit;
            }
            if (this.req.body.entityLPLimit > 0) {
                settings.entityLPLimit = this.req.body.entityLPLimit;
            }
        }
        if (this.req.body.entityClassroomLimit || this.req.body.entityClassroomLimit === 0) {
            if (this.req.body.entityClassroomLimit === 0 && settings.entityClassroomLimit) {
                delete settings.entityClassroomLimit;
            }
            if (this.req.body.entityClassroomLimit > 0) {
                settings.entityClassroomLimit = this.req.body.entityClassroomLimit;
            }
        }
        if (this.req.body.entityWebinarLimit || this.req.body.entityWebinarLimit === 0) {
            if (this.req.body.entityWebinarLimit === 0 && settings.entityWebinarLimit) {
                delete settings.entityWebinarLimit;
            }
            if (this.req.body.entityWebinarLimit > 0) {
                settings.entityWebinarLimit = this.req.body.entityWebinarLimit;
            }
        }
        if (this.req.body.entitySessionsLimit || this.req.body.entitySessionsLimit === 0) {
            if (this.req.body.entitySessionsLimit === 0 && settings.entitySessionsLimit) {
                delete settings.entitySessionsLimit;
            }
            if (this.req.body.entitySessionsLimit > 0) {
                settings.entitySessionsLimit = this.req.body.entitySessionsLimit;
            }
        }
        if (this.req.body.monthlyRefreshTokens || this.req.body.monthlyRefreshTokens === 0) {
            if (this.req.body.monthlyRefreshTokens === 0 && settings.monthlyRefreshTokens) {
                delete settings.monthlyRefreshTokens;
            }
            if (this.req.body.monthlyRefreshTokens > 0) {
                settings.monthlyRefreshTokens = this.req.body.monthlyRefreshTokens;
            }
        }
        if (this.req.body.dailyRefreshTokens || this.req.body.dailyRefreshTokens === 0) {
            if (this.req.body.dailyRefreshTokens === 0 && settings.dailyRefreshTokens) {
                delete settings.dailyRefreshTokens;
            }
            if (this.req.body.dailyRefreshTokens > 0) {
                settings.dailyRefreshTokens = this.req.body.dailyRefreshTokens;
            }
        }

        if (this.req.body.datalakeV2ExpirationTime || this.req.body.datalakeV2ExpirationTime === 0) {
            if (this.req.body.datalakeV2ExpirationTime === 0 && settings.datalakeV2ExpirationTime) {
                delete settings.datalakeV2ExpirationTime;
            }
            if (this.req.body.datalakeV2ExpirationTime > 0) {
                settings.datalakeV2ExpirationTime = this.req.body.datalakeV2ExpirationTime;
            }
        }

        if (this.req.body.extractionTimeLimit || this.req.body.extractionTimeLimit === 0) {
            if (this.req.body.extractionTimeLimit === 0 && settings.extractionTimeLimit) {
                delete settings.extractionTimeLimit;
            }
            if (this.req.body.extractionTimeLimit > 0) {
                settings.extractionTimeLimit = this.req.body.extractionTimeLimit;
            }
        }

        if (this.req.body.snowflakeTimeout || this.req.body.snowflakeTimeout === 0) {
            if (this.req.body.snowflakeTimeout === 0 && settings.snowflakeTimeout) {
                delete settings.snowflakeTimeout;
            }
            if (this.req.body.snowflakeTimeout > 0) {
                settings.snowflakeTimeout = this.req.body.snowflakeTimeout;
            }
        }

        // Adding additional check to the Datalake version toggle in settings to be sure to have it set in the proper way for anonymous calls
        if (this.session.platform.isDatalakeV3ToggleActive()) {
            settings.toggleDatalakeV3 = true;
        } else {
            if (settings.toggleDatalakeV3) {
                delete settings.toggleDatalakeV3;
            }

            if (this.session.platform.isDatalakeV2Active()) {
                settings.toggleDatalakeV2 = true;
            } else if (settings.toggleDatalakeV2) {
                delete settings.toggleDatalakeV2;
            }
        }

        // Additional check to the Hydra minimal version in settings to be sure to have it set in the proper way for anonymous calls
        if (this.session.platform.isHydraMinimalVersionToggleActive()) {
            settings.toggleHydraMinimalVersion = true;
        } else if (settings.toggleHydraMinimalVersion) {
            delete settings.toggleHydraMinimalVersion;
        }

        // Keep this order in order to avoid saving errorCount in settings
        await this.session.getDynamo().createOrEditSettings(settings);

        if (this.req.body.errorCount || this.req.body.errorCount === 0) {
            await this.session.getDynamo().restartDataLakeErrorCount();
            settings = { ...settings, errorCount: 0};
        }

        return settings;
    }

    public async addQueryBuilderAdmin(): Promise<string[]> {
        const idUser = parseInt(this.req.params.id_admin, 10);
        if (isNaN(idUser) || idUser < 0 || idUser > Number.MAX_SAFE_INTEGER) {
            throw(new Error('Not valid id_admin'));
        }
        const currentAdmins = this.session.platform.getQueryBuilderAdmins();
        const hydra = this.session.getHydra();
        const user = await hydra.getUserFullInfo(idUser);
        const username = user.data.user_data.username;
        const levels = await hydra.getLevelGroups();
        if (currentAdmins.includes(username)) {
            return currentAdmins;
        }
        const level = parseInt(user.data.user_data.level, 10);

        if (!(level in levels.data) || (level in levels.data && levels.data[level] !== UserLevelsGroups.GodAdmin)) {
            throw(new Error('The user wasn\'t a God Admin'));
        }

        if (idUser > 0) {
            currentAdmins.push(username);
            await redisFactory.getRedis().saveQueryBuilderAdmins(this.session.platform, currentAdmins);
        }

        return currentAdmins;
    }

    public async delQueryBuilderAdmin(): Promise<string[]> {
        const idUser = parseInt(this.req.params.id_admin, 10);
        if (isNaN(idUser) || idUser < 0 || idUser > Number.MAX_SAFE_INTEGER) {
            throw(new Error('Not valid id_admin'));
        }
        const user = await this.session.getHydra().getUserFullInfo(idUser);
        const username = user.data.user_data.username;
        const currentAdmins = this.session.platform.getQueryBuilderAdmins();

        if (currentAdmins.includes(username)) {
            const index = currentAdmins.indexOf(username);
            currentAdmins.splice(index, 1);
            await redisFactory.getRedis().saveQueryBuilderAdmins(this.session.platform, currentAdmins);
        }

        return currentAdmins;
    }
}
