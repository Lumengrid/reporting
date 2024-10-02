import * as jwt from 'jsonwebtoken';
import moment from 'moment';
import axios, { AxiosRequestConfig } from 'axios';
import { ExportsPayload, UsersExtractionsMapper } from '../reports/interfaces/extraction.interface';
import { SessionLoggerService } from './logger/session-logger.service';
import httpContext from 'express-http-context';
import Hydra from './hydra';

export class ReportSchedulationService {
    private bearerToken = '';
    private logger: SessionLoggerService;

    constructor(private readonly platform: string, private readonly privateKey: string, logger?: SessionLoggerService) {
        this.logger = logger ?? httpContext.get('logger');
    }

    /**
     * Call the backend requesting the extraction of the reports
     * @param platformsReport The reports of the users of the platform
     */
    async requestScheduledReportsExportByPlatform(platformsReport: UsersExtractionsMapper): Promise<void> {
        try {
            this.logger.debug(`Logging into ${this.platform}`);
            await this.authenticateAgainstPlatformAsSA(this.platform);
            await this.startReportExtractionByPlatform(platformsReport, this.platform);
            this.logger.debug(`Requests launched for platform ${this.platform}`);
        } catch (e: any) {
            this.logger.errorWithStack(`cannot launch the extraction for the platform: ${this.platform}.`, e);
        }
    }

    /**
     * Call the backend to retrieve the timezone of the owner of the report. Fallback: UTC
     * @param userId
     */
    async getOwnerReportTimezone(userId: number): Promise<string> {
        try {
            this.logger.debug(`Logging into ${this.platform} in order to retrieve timezone of the owner report`);
            await this.authenticateAgainstPlatformAsSA();
            const hydra = new Hydra(this.platform, `Bearer ${this.bearerToken}`, '');
            const response = await hydra.getUserProps(userId);
            return response.data.timezone;
        } catch (e: any) {
            this.logger.debug(`Cannot retrieve user timezone for the user ${userId}, platform ${this.platform}.`);
            return 'UTC';
        }
    }



    /**
     * Perform the authentication via JWT against the LMS
     */
    async authenticateAgainstPlatformAsSA(platform?: string): Promise<void> {
        let jwt;
        let accessToken;
        try {
            const decodedKey = Buffer.from(this.privateKey, 'base64');
            jwt = this.generateJWTBasedOnPlatform(platform ?? this.platform, decodedKey.toString());
        } catch (jwtError: any) {
            this.logger.errorWithStack(`Cannot generate jwt for platform ${platform ?? this.platform}, error `, jwtError);
            throw new Error('cannot generate jwt');
        }

        try {
            accessToken = await this.getUserAccessToken(jwt, platform ?? this.platform);
        } catch (authenticationError: any) {
            this.logger.errorWithStack(`cannot authenticate against ${platform ?? this.platform} with JWT, error is: ${authenticationError.message}`, authenticationError);
            throw new Error(`cannot authenticate against ${platform ?? this.platform} with JWT`);
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
     * Prepare the requests and call hydra to start the extraction
     * @param platformsReport The object that describes the reports for the platform
     */
    async startReportExtractionByPlatform(platformsReport: UsersExtractionsMapper, platform?: string): Promise<void> {
        const payload: ExportsPayload = {
            exports: [],
        };
        for (const user in platformsReport) {
            payload.exports.push({
                userId: +user,
                reports: platformsReport[user],
            });
        }

        // we don't want to wait for the response of the backend
        this.callHydraStartExtraction(payload, platform);
    }

    /**
     * Call Hydra to start the report extraction for a set of users of the platform
     * @param payload The payload to send to hydra
     */
    private async callHydraStartExtraction(payload: ExportsPayload, platform?: string): Promise<void> {
        const url = `report/v1/report/extractions`;
        const headers = {
            Authorization: `Bearer ${this.bearerToken}`,
        };
        const reqOptions: AxiosRequestConfig = {
            method: 'post',
            baseURL: `https://${platform ?? this.platform}`,
            url,
            headers,
            data: payload,
        };
        let response;
        try {
            response = await axios.request(reqOptions);
        } catch (httpError: any) {
            this.logger.errorWithStack(`Hydra report api start report extraction error on platform ${platform ?? this.platform}, call failed.`, httpError);
        }
    }
}

export type OauthResponse = {
    data: AuthenticationDetails;
};

export type AuthenticationDetails = {
    access_token: string;
    expires_in: number;
    token_type: string;
    scope: string;
};
