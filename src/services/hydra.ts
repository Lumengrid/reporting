import axios, { AxiosRequestConfig, AxiosResponse, Method, AxiosError } from 'axios';
import httpContext from 'express-http-context';
import Https from 'https';

import { BadRequestException, NotFoundException, ServerErrorException, UnauthorizedException, Exception } from '../exceptions';
import { ErrorCode } from '../exceptions/error-codes.enum';
import { LegacyReportsResponse, MigrateInputPayload } from '../models/migration-component';
import { SessionLoggerService } from './logger/session-logger.service';
import { HTTPFactory } from './http/HTTPFactory';

class HydraCallHeader {
    authorization?: string;
    contentType?: string;
}

interface HydraHeaders {
    Authorization?: string;
    contentType?: string;
    cookie?: string;
    'x-csrf-token'?: string;
}

export class HydraCall implements Https.AgentOptions {
    hostname: string;
    subfolder: string;
    port: number;
    path: string;
    method: string;
    headers: HydraCallHeader;
    token: string;
    body?: string;
    params?: any;
    cookie?: string;
    xCSRFToken?: string;

    constructor(url: string, token: string, subfolder: string, cookie?: string, xCSRFToken?: string) {
        this.hostname = url;
        this.subfolder = subfolder;
        this.port = 443;
        this.path = '/';
        this.method = 'GET';
        this.headers = new HydraCallHeader();
        this.token = token;
        this.cookie = cookie;
        this.xCSRFToken = xCSRFToken;
    }
}

export interface SessionResponse {
    data: SessionResponseData;
}

interface SessionResponseData {
    user?: SessionResponseDataUser;
    platform: SessionResponseDataPlatform;
}

interface SessionResponseDataUser {
    idUser: number;
    username: string;
    eMail: string;
    level: string;
    erpAdmin: boolean;
    lang: string;
    langCode: string;
    permissions: SessionResponseDataUserPermissions;
    groups: number[];
    branches: number[];
    branchesWithParents: number[];
    timezone: string;
}

interface SessionResponseDataUserPermissions {
    viewReport: boolean;
    updateReport: boolean;
    manager: boolean;
    viewEcommerceTransaction:boolean;
}

export interface SessionResponseDataToggles {
    toggleAdminReport: boolean;
    toggleNewContribute: boolean;
    toggleMyTeamUserAddFilter: boolean;
    toggleWebinarsEnableCreation: boolean;
    toggleForceDatalakeV1: boolean;
    toggleAudittrailLegacyArchive: boolean;
    toggleManagerReportXLSXPolling: boolean;
    toggleDatalakeV2ManualRefresh: boolean;
    toggleMultipleEnrollmentCompletions: boolean;
    toggleDatalakeV3: boolean;
    togglePrivacyPolicyDashboardOnAthena: boolean;
    toggleCoursesDashboardOnAthena: boolean;
    toggleBranchesDashboardOnAthena: boolean;
    toggleHydraMinimalVersion: boolean;
    toggleUsersLearningPlansReportEnhancement: boolean;
    toggleLearningPlansStatisticsReport: boolean;
    toggleNewLearningPlanManagement: boolean;
}

interface SessionResponseDataPlatform {
    platformBaseUrl: string;
    defaultLanguage: string;
    defaultLanguageCode: string;
    configs: SessionResponseDataPlatformConfigs;
    plugins: SessionResponseDataPlugins;
    toggles: SessionResponseDataToggles;
}

export interface SessionResponseDataPlatformConfigs {
    showFirstNameFirst: boolean;
    defaultPlatformTimezone: string;
    reportDownloadPermissionLink: boolean;
    isUserAddFieldsFiltersForManager: boolean;
    isLearningplansAssignmentTypeActive: boolean;
    isCoursesAssignmentTypeActive: boolean;
}

export interface SessionResponseDataPlugins {
    certification: boolean;
    classroom: boolean;
    esignature: boolean;
    ecommerce: boolean;
    gamification: boolean;
    transcript: boolean;
    contentPartners: boolean;
    share: boolean;
    flow: boolean;
    flowMsTeams: boolean;
    multiDomain: boolean;
}

export class BackgroundJobParams {
    name = 'Test execution report';
    endpoint = '/analytics/v1/exports/polling/echo';
    method = 'GET';
    type = 'importer';

    // tslint:disable: variable-name
    chunk_size = 1;
    data_source: BackgroundJobParamsDataSource;
    notify = true;
    notify_email?: string|string[];
    notify_type = 'inbox';
    // tslint:enable: variable-name

    public constructor(idReport: string, idExport: string, idUser: number, eMail: string|string[], reportTitle: string, hostname: string, subfolder: string) {
        this.data_source = new BackgroundJobParamsDataSource(idReport, idExport, idUser, reportTitle, hostname, subfolder);
        this.name = `Extraction for report ${reportTitle}`;
        if (eMail) {
            this.notify_email = eMail;
            this.notify_type = 'email_and_inbox';
        }
    }
}

export class BackgroundJobParamsDataSource {
    type = 'report_file_extraction';
    // tslint:disable-next-line: variable-name
    datasource_params: BackgroundJobParamsDataSourceParams;
    public constructor(idReport: string, idExport: string, idUser: number, reportTitle: string, hostname: string, subfolder: string) {
        this.datasource_params = new BackgroundJobParamsDataSourceParams(idReport, idExport, idUser, reportTitle, hostname, subfolder);
    }
}

export class BackgroundJobParamsDataSourceParams {
    reportId: string;
    extractionId: string;
    authorId: number;
    reportTitle: string;
    hostname: string;
    subfolder: string;
    public constructor(idReport: string, idExport: string, idUser: number, reportTitle: string, hostname: string, subfolder: string) {
        this.reportId = idReport;
        this.extractionId = idExport;
        this.authorId = idUser;
        this.reportTitle = reportTitle;
        this.hostname = hostname;
        this.subfolder = subfolder;
    }
}

export class UsersResponse {
    data: { [key: number]: UsersResponseDetails; };
    constructor() {
        this.data = [];
    }
}
export class UsersResponseDetails {
    userid: string;
    firstname: string;
    lastname: string;
    avatar?: string;
    constructor() {
        this.userid = this.firstname = this.lastname = '';
    }
}

export class GroupsResponse {
    data: { [key: number]: string; };
    constructor() {
        this.data = [];
    }
}

export class BranchesResponse extends GroupsResponse {}

export class CoursesResponse {
    data: { [key: number]: CoursesResponseDetails; };
    constructor() {
        this.data = [];
    }
}
export class CoursesResponseDetails {
    code: string;
    title: string;
    constructor() {
        this.code = this.title = '';
    }
}

export class SessionsResponse {
    data: { [key: number]: SessionsResponseDetails; };
    constructor() {
        this.data = [];
    }
}
export class SessionsResponseDetails {
    code: string;
    name: string;
    constructor() {
        this.code = this.name = '';
    }
}
export class SurveysResponse {
    data: { [key: number]: SurveysResponseDetails; };
    constructor() {
        this.data = [];
    }
}
export class SurveysResponseDetails {
    id: number;
    title: string;
    description: string;
    constructor() {
        this.title = this.description = '';
    }
}

export class LearningPlansResponse {
    data: { [key: number]: LearningPlansResponseDetails; };
    constructor() {
        this.data = [];
    }
}

export class CoursesCategoriesResponse {
    data: {id: string, title: string}[];
    constructor() {
        this.data = [];
    }
}

export class CertificationsDetailsResponse {
    data: {id_cert: string, title: string}[];
    constructor() {
        this.data = [];
    }
}
export class BadgesDetailsResponse {
    data: {id_badge: string, name: string}[];
    constructor() {
        this.data = [];
    }
}
export class AssetsDetailsResponse {
    data: {id: string, title: string}[];
    constructor() {
        this.data = [];
    }
}
export class ChannelsDetailsResponse {
    data: {id: string, name: string}[];
    constructor() {
        this.data = [];
    }
}

export class LearningPlansResponseDetails {
    code: string;
    title: string;
    constructor() {
        this.code = this.title = '';
    }
}



export class PuUserAssociationResponse {
    data: { items: number[]; count: number; total_pages: number; page_number: number };
    constructor() {
        this.data = { items: [], count: 0, total_pages: 0, page_number: 0 };
    }
}

export class PuAssociationResponse {
    data: number[];
    constructor() {
        this.data = [];
    }
}

export class PuPermissionResponse {
    data: PuPermissions;
    constructor() {
        this.data = new PuPermissions();
    }
}

export class PuPermissions {
    viewReport: boolean;
    updateReport: boolean;
    public constructor() {
        this.updateReport = this.viewReport = false;
    }
}

export type UserPropsResponse = {
    data: UserProps;
};

export type UserProps = {
    idUser: number;
    firstname: string;
    lastname: string;
    username: string;
    avatar: string;
    timezone?: string;
};

export type FullUserInfoResponse = {
    data: FullUserInfoData;
};

export type FullUserInfoData = {
    user_data: FullUserInfo;
};

export type FullUserInfo = {
    level: string;
    username: string;
};

export type LevelGroupsResponse = {
    data: {[key: number]: string}
};

export type UserExtraFieldsWithDropdownOptionsResponse = {
    data: UserExtraFieldsWithDropdownOptions[];
};

export type UserExtraFieldsWithDropdownOptions = {
    id: string;
    title: string;
    type: string;
    sequence: number;
    options: UserExtraFieldsWithDropdownOptionsOptions[]
};

export type UserExtraFieldsWithDropdownOptionsOptions = {
    id: string,
    label: string
};

export type UserExtraFieldsResponse = {
    data: UserExtraFieldsItems;
};

export type UserExtraFieldsItems = {
    items: UserExtraFields[];
};

export type UserExtraFields = {
    id: string;
    title: string;
    type: string;
};

export type CourseExtraFieldsResponse = {
    data: CourseExtraFieldsItems;
};

export type CourseExtraFieldsItems = {
    items: CourseExtraFields[];
};

export type CourseExtraFields = {
    id: number;
    type: string;
    name: CourseExtraFieldsName;
};

export type CourseExtraFieldsName = {
    value: string;
    values: string[];
};

export type CourseuserExtraFieldsResponse = {
    data: CourseuserExtraFields[];
};

export type LearningPlanExtraFieldsResponse = {
    data: LearningPlanExtraFieldsItems;
};

export type LearningPlanExtraFieldsItems = {
    items: LearningPlanExtraFields[];
};

export type LearningPlanExtraFields = {
    id: number;
    type: string;
    name: LearningPlanExtraFieldsName;
};

export type LearningPlanExtraFieldsName = {
    value: string;
    values: string[];
};

export type CourseuserExtraFields = {
    id: number;
    type: string;
    name: string;
};

export type TranscriptsExtraFieldsResponse = {
    data: TranscriptsExtraFieldsItems;
};

export type TranscriptsExtraFieldsItems = {
    items: TranscriptsExtraFields[];
};

export type ManagerType = {
    manager_type_id: string;
    manager_type_name: string;
};

export type ManagerTypes = {
    data: {
        items: ManagerType[];
    };
};

export type TranscriptsExtraFields = {
    id: number;
    title: string;
    type: string;
};

export type Translations = {
    translations: { [key: string]: TranslationsPhrases; };
    lang_code: string;
};

export type TranslationsPhrases = {
    key: string;
    module: string;
};

export type ReportTranslations = {
    data: { [key: string]: string; };
};

export interface ReportLegacyMigrationResponse {
    data: LegacyReportsResponse;
}

export type LearningObjectTypesResponse = {
    data: string[],
};

export type InstallationTypeResponse = {
    data: { installationType: string | null};
};

export default class Hydra {
    protected token: string;
    protected cookie?: any;
    protected xCSRFToken?: string;
    protected hostname: string;
    protected subfolder: string;
    protected logger: SessionLoggerService;

    constructor(url: string, token: string, subfolder: string, cookie?: string, xCSRFToken?: string, logger?: SessionLoggerService) {
        if (!url) {
            throw new BadRequestException('Missing url while calling hydra', ErrorCode.MISSING_URL);
        }
        if (token === undefined) {
            throw new BadRequestException('Trying to authenticate against hydra but token is missing', ErrorCode.MISSING_TOKEN);
        }
        this.logger = logger ?? httpContext.get('logger');

        this.token = token;
        this.xCSRFToken = xCSRFToken;
        this.cookie = cookie;
        this.hostname = url;
        this.subfolder = subfolder;
    }

    /**
     * Return the current platform hostname
     *
     * @returns {string} The current hostname used for hydra calls
     */
    public getHostname(): string {
        return this.hostname;
    }

    /**
     * Return the current platform subfolder if present
     *
     * @returns {string} The current subfolder, if present, used for hydra calls
     */
    public getSubfolder(): string {
        return this.subfolder;
    }

    /**
     * Perform a call to hydra backend
     *
     * @param {HydraCall} options Connection options
     * @returns {Promise<any>} The response object, type differs per endpoint
     *
     * @throws UnauthorizedException
     * @throws NotFoundException
     * @throws ServerErrorException
     * @throws BadRequestException
     */
    public async call(options: HydraCall): Promise<any> {
        const headers: HydraHeaders = {};
        if (typeof options.headers.authorization === 'undefined' && this.token !== '') {
            headers.Authorization = this.token;
        }

        const methodToUpper = options.method.toUpperCase();
        if (methodToUpper !== 'GET' && methodToUpper !== 'OPTIONS' && methodToUpper !== 'HEAD') {
            headers.cookie = this.cookie ?? '';
            headers['x-csrf-token'] = this.xCSRFToken ?? '';
        }

        const reqOptions = {
            method: options.method as Method,
            baseURL: `https://${options.hostname}`,
            url:  (options.subfolder !== '' ? `/${options.subfolder}` : '') + options.path,
            headers,
            data: options.body,
            params: options.params ? options.params : undefined,
        };

        try {
            const response = await HTTPFactory.getHTTPService().call(reqOptions);
            return response.data;
        } catch (apiError: any) { // Interface AxiosError, attribute response.status is guaranteed to be there
            this.logger.errorWithStack(`Error on hydra request ${JSON.stringify(reqOptions)}, Response: ${JSON.stringify(apiError.response?.data)}`, apiError);
            switch (apiError.response.status) {
                case 401:
                    throw new UnauthorizedException('Unauthorized call to hydra', ErrorCode.HYDRA_AUTH_ERROR);
                case 500:
                    throw new ServerErrorException('Call to hydra has failed', ErrorCode.HYDRA_SERVER_ERROR);
                case 404:
                    throw new NotFoundException('Hydra resource not found', ErrorCode.HYDRA_NOT_FOUND);
                case 400:
                    this.logger.errorWithStack(`Bad hydra request. Request ${JSON.stringify(reqOptions)}, Response: ${JSON.stringify(apiError.response.data)}`, apiError);
                    throw new BadRequestException('Bad hydra request', ErrorCode.HYDRA_BAD_REQUEST);
                default:
                    this.logger.errorWithStack(`Unexpected error while performing a call to Hydra. ${reqOptions.baseURL}${reqOptions.url}. Response status code: ${apiError.response.status}`, apiError);
                    throw new Exception('Unexpected error while performing a call to Hydra', ErrorCode.HYDRA_UNEXPECTED_ERROR);
            }
        }
    }

    /**
     * Get currently logged in user's session details
     */
    public async session(): Promise<SessionResponse> {
        if (!this.token) {
            return {
                data: {
                    platform: {
                        platformBaseUrl: this.hostname,
                        defaultLanguage: 'english',
                        defaultLanguageCode: 'en',
                        configs: {
                            showFirstNameFirst: true,
                            defaultPlatformTimezone: '',
                            reportDownloadPermissionLink: false,
                            isUserAddFieldsFiltersForManager: false,
                            isLearningplansAssignmentTypeActive: false,
                            isCoursesAssignmentTypeActive: false,
                        },
                        plugins: {
                            certification: true,
                            classroom: true,
                            esignature: true,
                            ecommerce: true,
                            gamification: true,
                            transcript: true,
                            contentPartners: true,
                            share: true,
                            flow: true,
                            flowMsTeams: true,
                            multiDomain: true,
                        },
                        toggles: {
                            toggleAdminReport: false,
                            toggleNewContribute: false,
                            toggleMyTeamUserAddFilter: false,
                            toggleWebinarsEnableCreation: false,
                            toggleForceDatalakeV1: false,
                            toggleAudittrailLegacyArchive: false,
                            toggleManagerReportXLSXPolling: false,
                            toggleDatalakeV2ManualRefresh: false,
                            toggleMultipleEnrollmentCompletions: false,
                            toggleDatalakeV3: false,
                            togglePrivacyPolicyDashboardOnAthena: false,
                            toggleCoursesDashboardOnAthena: false,
                            toggleBranchesDashboardOnAthena: false,
                            toggleHydraMinimalVersion: false,
                            toggleUsersLearningPlansReportEnhancement: false,
                            toggleLearningPlansStatisticsReport: false,
                            toggleNewLearningPlanManagement: false,
                        }
                    }
                }
            };
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/session';

        return await this.call(options);
    }

    public async getUsers(users: number[]): Promise<UsersResponse> {

        if (users && users.length === 0) {
            return new UsersResponse();
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/users_details';
        options.method = 'POST';
        options.body = JSON.stringify({
            id_users: users
        });

        const call = await this.call(options);

        return call as UsersResponse;
    }

    public async getGroups(groups: number[]): Promise<GroupsResponse> {

        if (groups && groups.length === 0) {
            return new GroupsResponse();
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/groups_details';
        options.method = 'POST';
        options.body = JSON.stringify({
            id_groups: groups
        });

        return await this.call(options);
    }

    public async getBranches(branches: number[]): Promise<BranchesResponse> {

        if (branches && branches.length === 0) {
            return new BranchesResponse();
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/branches_details';
        options.method = 'POST';
        options.body = JSON.stringify({
            id_branches: branches
        });

        return await this.call(options);
    }

    public async getCourses(courses: number[]): Promise<CoursesResponse> {

        if (courses && courses.length === 0) {
            return new CoursesResponse();
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/courses_details';
        options.method = 'POST';
        options.body = JSON.stringify({
            id_courses: courses
        });

        return await this.call(options);
    }

    public async getSessions(sessions: number[]): Promise<SessionsResponse> {

        if (sessions && sessions.length === 0) {
            return new SessionsResponse();
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/sessions-details';
        options.method = 'POST';
        options.body = JSON.stringify({
            sessionsIds: sessions
        });

        return await this.call(options);
    }

    public async getSurveys(surveys: number[]): Promise<SurveysResponse> {

        if (surveys && surveys.length === 0) {
            return new SurveysResponse();
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder);
        options.path = '/report/v1/report/surveys-details';
        options.method = 'POST';
        options.body = JSON.stringify({
            surveysIds: surveys
        });

        return await this.call(options);
    }

    public async getLearningPlans(learningPlans: number[]): Promise<LearningPlansResponse> {

        if (learningPlans && learningPlans.length === 0) {
            return new LearningPlansResponse();
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/learningplans_details';
        options.method = 'POST';
        options.body = JSON.stringify({
            id_learningPlans: learningPlans
        });

        return await this.call(options);
    }

    public async getCourseCategories(categoriesIds: number[]): Promise<CoursesCategoriesResponse> {

        if (categoriesIds && categoriesIds.length === 0) {
            return {
                data: []
            };
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/categories-details';
        options.method = 'POST';
        options.body = JSON.stringify({
            categoriesIds
        });

        return await this.call(options);
    }

    public async getCertificationsDetail(certificationsIds: number[]): Promise<CertificationsDetailsResponse> {

        if (certificationsIds && certificationsIds.length === 0) {
            return {
                data: []
            };
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/certifications-details';
        options.method = 'POST';
        options.body = JSON.stringify({
            certificationIds: certificationsIds
        });

        return await this.call(options);
    }

    public async getBadgesDetail(badgesIds: number[]): Promise<BadgesDetailsResponse> {

        if (badgesIds && badgesIds.length === 0) {
            return {
                data: []
            };
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/badges-details';
        options.method = 'POST';
        options.body = JSON.stringify({
            badgesIds
        });

        return await this.call(options);
    }

    public async getAssetsDetail(assetsIds: number[]): Promise<AssetsDetailsResponse> {

        if (assetsIds && assetsIds.length === 0) {
            return {
                data: []
            };
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/assets-details';
        options.method = 'POST';
        options.body = JSON.stringify({
            assetsIds
        });

        return await this.call(options);
    }

    public async getChannelsDetail(channelsIds: number[]): Promise<ChannelsDetailsResponse> {

        if (channelsIds && channelsIds.length === 0) {
            return {
                data: []
            };
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/channels-details';
        options.method = 'POST';
        options.body = JSON.stringify({
            channelsIds
        });

        return await this.call(options);
    }

    public async createExtractionBackgroundJob(idReport: string, idExport: string, idUser: number, eMail: string|string[], reportTitle: string, hostname?: string, subfolder?: string): Promise<void> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        let hostnameBJ = this.hostname;
        let subfolderBJ = this.subfolder;

        if (typeof hostname !== 'undefined' && hostname !== '') {
            hostnameBJ = hostname;
        }

        if (typeof subfolder !== 'undefined' && subfolder !== '') {
            subfolderBJ = subfolder;
        }

        options.body = JSON.stringify(new BackgroundJobParams(idReport, idExport, idUser, eMail, reportTitle, hostnameBJ, subfolderBJ));
        options.path = '/manage/v1/job';
        options.method = 'POST';
        await this.call(options);
    }

    public async createBackgroundJob(payload: any): Promise<void> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.body = JSON.stringify(payload);
        options.path = '/manage/v1/job';
        options.method = 'POST';
        await this.call(options);
    }

    public async* getPuUsers(pageDimension?): AsyncGenerator<PuUserAssociationResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/pu_users';
        options.method = 'GET';
        options.params = {};
        if (pageDimension) {
            options.params.page_dimension = pageDimension;
        }
        const response = await this.call(options);
        yield response;
        if (response.data.total_pages > 1) {
            for (let pageNumber = 2; pageNumber <= response.data.total_pages; pageNumber++) {
                options.params.page_number = pageNumber;
                yield await this.call(options);
            }
        }
    }

    public async getPuCourses(): Promise<PuAssociationResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/pu_courses';
        options.method = 'GET';
        return await this.call(options);
    }

    public async getPuLPs(): Promise<PuAssociationResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/pu_lps';
        options.method = 'GET';
        return await this.call(options);
    }

    public async getUserProps(userId: number): Promise<UserPropsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = `/report/v1/report/user/${userId}`;
        options.method = 'GET';
        return await this.call(options);
    }

    public async getUserFullInfo(userId: number): Promise<FullUserInfoResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = `/manage/v1/user/${userId}`;
        options.method = 'GET';
        return await this.call(options);
    }

    public async getLevelGroups(): Promise<LevelGroupsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = `/report/v1/report/level_groups`;
        options.method = 'GET';
        return await this.call(options);
    }

    public async getUserExtraFields(): Promise<UserExtraFieldsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/manage/v1/user_fields?no_pagination=1';
        options.method = 'GET';
        return await this.call(options);
    }

    public async getUserExtraFieldsWithDropdownOptions(userId?: number): Promise<UserExtraFieldsWithDropdownOptionsResponse> {
        let endpointWithQueryString = '/manage/v1/user_fields?no_pagination=1&as_array=1';

        // Used for the report manager visibility in My Team Page
        if (userId) {
            endpointWithQueryString = `${endpointWithQueryString}&user_id=${userId}`;
        }

        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = endpointWithQueryString;
        options.method = 'GET';
        return await this.call(options);
    }

    // TODO: Remove the course extra fields API with a new one with a better payload
    public async getCourseExtraFields(): Promise<CourseExtraFieldsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/learn/v1/courses/field?no_pagination=1&association=course&show_field=1';
        options.method = 'GET';
        const response = await this.call(options);
        for (const [index, field] of response.data.items.entries()) {
            if (field.name.value === null) {
                response.data.items[index].name.value = 'course_extrafield_' + field.id;
            }
        }
        return response;
    }

    public async getWebinarExtraFields(): Promise<CourseExtraFieldsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/learn/v1/courses/field?no_pagination=1&association=webinar&show_field=1';
        options.method = 'GET';
        return await this.call(options);
    }

    public async getILTExtraFields(): Promise<CourseExtraFieldsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/learn/v1/courses/field?no_pagination=1&association=ilt&show_field=1';
        options.method = 'GET';
        return await this.call(options);
    }

    public async getLearningPlanExtraFields(): Promise<LearningPlanExtraFieldsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/learn/v1/courses/field?no_pagination=1&association=coursepath&show_field=1';
        options.method = 'GET';
        return await this.call(options);
    }

    public async getCourseuserExtraFields(): Promise<CourseuserExtraFieldsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/courseuser_fields';
        options.method = 'GET';
        const response = await this.call(options);
        for (const [index, field] of response.data.entries()) {
            if (field.name === null) {
                response.data[index].name = 'courseuser_extrafield_' + field.id;
            }
        }
        return response;
    }

    public async getTranscriptExtraFields(): Promise<TranscriptsExtraFieldsResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/report/v1/report/transcript_fields';
        options.method = 'GET';
        return await this.call(options);
    }

    public async getTranslations(translations: Translations): Promise<ReportTranslations> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.body = JSON.stringify(translations);
        options.path = '/report/v1/report/translations';
        options.method = 'POST';
        return await this.call(options);
    }

    public async getManagerTypes(ids?: number[]): Promise<ManagerTypes> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = '/manage/v1/managers/types';
        if (ids.length > 0) {
            options.path += `?manager_type_ids[]=` + ids.join('&manager_type_ids[]=');
        }
        options.method = 'GET';
        return await this.call(options);
    }

    public async getOldReportsFromHydra(payload: MigrateInputPayload): Promise<ReportLegacyMigrationResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = `/report/v1/report/migration`;
        options.method = 'POST';
        options.body = JSON.stringify(payload);
        return await this.call(options);
    }

    public async getAllLOTypes(): Promise<LearningObjectTypesResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = `/report/v1/report/learning-object-types`;
        options.method = 'GET';
        return await this.call(options);
    }

    async getInstallationType(): Promise<InstallationTypeResponse> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.path = `/report/v1/report/installation-type`;
        options.method = 'GET';
        return await this.call(options);
    }

    public async generateEventOnEventBus(payload: any): Promise<void> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        options.body = JSON.stringify(payload);
        options.path = '/report/v1/report/generate-event-on-eventbus';
        options.method = 'POST';
        await this.call(options);
    }

    /**
     * List of userId subordinate (directly and indirectly) at the current userId logged
     * filtered, if is request, by managerTypes ids
     *
     * @param userId current userId logged
     * @param managerTypes If passed, filter subordinate based on the manager type
     */
    public async getUserIdsByManager(userId: number, managerTypes: number[]): Promise<number[]> {
        const options = new HydraCall(this.hostname, this.token, this.subfolder, this.cookie, this.xCSRFToken);
        const userIds = [];
        options.method = 'GET';
        options.path = `/skill/v1/managers/${userId}/subordinates`;

        if (managerTypes.length > 0) {
            options.path += `?manager_type_id[]=` + managerTypes.join('&manager_type_id[]=');
        }

        const response = await this.call(options);

        for (const item of response.data.items) {
            JSON.stringify(item, (key, value) => {
                if (key === 'id') userIds.push(value);
                return value;
            });
        }

        return userIds;
    }
}
