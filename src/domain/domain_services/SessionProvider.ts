import SessionManager from '../../services/session/session-manager.session';
import CacheService from '../../services/cache/cache';
import { getSession } from '../../routes/permissions';
import { Redis } from '../../services/redis/redis';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import { InvalidAuthorUsernameException } from '../exceptions/InvalidAuthorUsernameException';
import { InvalidTokenException } from '../exceptions/InvalidTokenException';
import { Utils } from '../../reports/utils';
import { HTTPService } from '../../services/http/HTTPService';
import moment from 'moment/moment';
import * as jwt from 'jsonwebtoken';
import Hydra from '../../services/hydra';

export class SessionProvider {
    public constructor(
        private readonly logger: SessionLoggerService,
        private readonly httpService: HTTPService,
        private readonly cache: CacheService,
        private readonly redis: Redis,
        private readonly delayBetweenAttemptsInMilliseconds = 200,
    ) {
    }

    private async performCallWithRetry(callable: () => Promise<any>): Promise<any> {
        let attempts = 0;

        while (true) {
            try {
                return await callable();
            } catch (error: any) {
                this.logger.errorWithStack(error.message, error);

                if (!error.isAxiosError || error.response?.status < 500 || ++attempts >= 10) {
                    throw error;
                }

                await Utils.sleep(this.delayBetweenAttemptsInMilliseconds);
            }
        }
    }

    private generateJWTBasedOnPlatform(platform: string, privateKey: string, user: string): string {
        const clientId = 'aamon';
        const now = moment().valueOf();
        const plusYear = moment().add(1, 'y').valueOf();

        const payload = {
            iss: clientId,
            sub: user,
            aud: platform,
            iat: now,
            exp: plusYear,
        };

        const header = {
            alg: 'RS256',
            typ: 'JWT',
        };

        return jwt.sign(payload, privateKey, {header});
    }

    private async getUserAccessToken(jwt: string, platform: string): Promise<string> {
        const url = `/manage/v1/oauth2/token`;

        const payload = {
            grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            scope: 'api',
            assertion: jwt,
        };

        const reqOptions = {
            method: 'post',
            baseURL: `https://${platform}`,
            url,
            data: payload,
        };

        const response = await this.performCallWithRetry(() => this.httpService.call(reqOptions));

        return response.data.data.access_token;
    }

    public async getToken(platform: string, user: string): Promise<string> {
        const key = `session.provider#access_token#${platform}#${user}`;
        let token = this.cache.get(key);

        if (token) {
            this.logger.debug(`Found token in cache for user "${user}", no need to call Hydra for ${platform}`);
            return token;
        }

        token = '';

        try {
            const commonKeys = await this.redis.getRedisCommonParams(platform);
            const privateKey = commonKeys.schedulationPrivateKey;
            const decodedKey = Buffer.from(privateKey, 'base64');
            const jwt = this.generateJWTBasedOnPlatform(platform, decodedKey.toString(), user);
            token = await this.getUserAccessToken(jwt, platform);
        } catch (ex) {
            this.logger.error(`[SessionProvider] Error on retrieving token of ${user} for ${platform}`, ex);
        }

        if (typeof token !== 'string' || token === '') {
            this.logger.error(`[SessionProvider] Token not valid for ${user} for ${platform}`);
            throw new InvalidTokenException(`${user} token not recovered for ${platform}`);
        }

        token = `Bearer ${token}`;
        this.cache.set(key, token, 30);

        return token;
    }

    private async getUsernameByUserId(platform: string, subfolder: string, userId: number, token: string): Promise<string> {
        try {
            const hydra = new Hydra(platform, token, subfolder, '', '', this.logger);
            const response = await this.performCallWithRetry(() => hydra.getUserFullInfo(userId));

            return response.data.user_data.username;
        } catch (ex) {
            this.logger.error(`[SessionProvider] Error on retrieving Username of ${userId} for ${platform}`, ex);
            throw new InvalidAuthorUsernameException(`Username of Author ${userId} not recovered for ${platform}`);
        }
    }

    public async getTokenForUserId(platform: string, subfolder: string, userId: number): Promise<string> {
        const staffSupportToken = await this.getToken(platform, 'staff.support');
        const username = await this.getUsernameByUserId(platform, subfolder, userId, staffSupportToken);

        return this.getToken(platform, username);
    }

    public async getSession(platform: string, subfolder: string, userId: number): Promise<SessionManager> {
        const userToken = await this.getTokenForUserId(platform, subfolder, userId);

        return this.performCallWithRetry(() => getSession(userToken, platform, subfolder, this.cache, '', '', this.logger));
    }
}
