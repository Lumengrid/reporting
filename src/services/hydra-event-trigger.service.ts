import * as jwt from 'jsonwebtoken';
import moment from 'moment';
import axios, { AxiosRequestConfig } from 'axios';
import { SessionLoggerService } from './logger/session-logger.service';
import httpContext from 'express-http-context';
import { redisFactory } from './redis/RedisFactory';

export default class HydraEventTriggerService {
    private bearerToken = '';
    private logger: SessionLoggerService;
    private privateKey = '';

    constructor(private readonly platform: string) {
        this.logger = httpContext.get('logger');
    }

    /**
     * Call the backend requesting the extraction of the reports
     * @param platformsReport The reports of the users of the platform
     */
    async triggerEventOnPlatform(event: string, eventParameters: HydraEventParameters): Promise<void> {
        try {
            const commonKeys = await this.getPlatformCommonConfig();
            this.privateKey = commonKeys.schedulationPrivateKey;
            this.logger.debug(`Logging into ${this.platform}`);
            await this.authenticateAgainstPlatformAsSA();
            await this.callHydra({event, eventParameters});
            this.logger.debug(`Event ${event} launched for platform ${this.platform}`);
        } catch (e: any) {
            this.logger.errorWithStack(`cannot launch event ${event} for the platform: ${this.platform}.`, e);
        }
    }

    private async getPlatformCommonConfig(): Promise<{[key: string]: string}> {
        const logger: SessionLoggerService = httpContext.get('logger');

        try {
            return await redisFactory.getRedis().getRedisCommonParams(this.platform);
        } catch (e) {
            logger.errorWithStack('Error while attempting to read redis main configuration.', e);
            throw new Error(`Error while attempting to read redis main configuration: ${e}`);
        }
    }

    /**
     * Perform the authentication via JWT against the LMS
     */
    async authenticateAgainstPlatformAsSA(): Promise<void> {
        let jwt;
        let accessToken;
        try {
            const decodedKey = Buffer.from(this.privateKey, 'base64');
            jwt = this.generateJWTBasedOnPlatform(this.platform, decodedKey.toString());
        } catch (jwtError: any) {
            this.logger.errorWithStack(jwtError, `Cannot generate jwt for platform ${this.platform}, error `, jwtError);
            throw new Error('cannot generate jwt');
        }

        try {
            accessToken = await this.getUserAccessToken(jwt, this.platform);
        } catch (authenticationError: any) {
            this.logger.errorWithStack(`cannot authenticate against ${this.platform} with JWT.`, authenticationError);
            throw new Error(`cannot authenticate against ${this.platform} with JWT`);
        }

        this.bearerToken = accessToken;
    }

    /**
     * Generate the JWT token
     * @param platform The target platform
     * @param privateKey The private key of aamon
     */
    private generateJWTBasedOnPlatform(platform: string, privateKey: string): string {
        const user = 'staff.support';
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
        const encodedJwt = jwt.sign(payload, privateKey, {header});
        return encodedJwt;
    }

    /**
     * Call the backend to obtain the access token
     * @param jwt The JWT token
     * @param platform The target platform
     */
    private async getUserAccessToken(jwt: string, platform: string): Promise<string> {
        const url = `/manage/v1/oauth2/token`;
        const payload = {
            grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            scope: 'api',
            assertion: jwt,
        };
        const reqOptions: AxiosRequestConfig = {
            method: 'post',
            baseURL: `https://${platform}`,
            url,
            data: payload,
        };

        const response = await axios.request(reqOptions);

        return response.data.data.access_token;
    }

    /**
     * Call Hydra to start the report extraction for a set of users of the platform
     * @param payload The payload to send to hydra
     */
    private async callHydra(payload: HydraEventPayload): Promise<void> {
        const url = `report/v1/report/event_trigger`;
        const headers = {
            Authorization: `Bearer ${this.bearerToken}`,
        };
        const reqOptions: AxiosRequestConfig = {
            method: 'post',
            baseURL: `https://${this.platform}`,
            url,
            headers,
            data: payload,
        };
        let response;
        try {
            response = await axios.request(reqOptions);
        } catch (httpError: any) {
            this.logger.errorWithStack(`Event trigger api error on platform ${this.platform}, call failed`, httpError);
        }
    }
}



type HydraEventParameters = {
    exampleParam?: string;
};

type HydraEventPayload = {
    event: string,
    eventParameters: HydraEventParameters
};
