import express, { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';

import { NotFoundException, UnauthorizedException, DisabledReportTypeException } from '../exceptions';
import { ErrorCode } from '../exceptions/error-codes.enum';
import { BaseResponse, DataLakeRefreshStatus, ErrorsCode, ReportsSettings } from '../models/base';
import { ReportManagerSwitcher } from '../models/report-switcher';
import Hydra from '../services/hydra';
import { SessionLoggerService } from '../services/logger/session-logger.service';
import SessionManager from '../services/session/session-manager.session';
import { LastRefreshDate } from '../reports/interfaces/extraction.interface';
import { ExtractionComponent } from '../models/extraction.component';
import CacheService from '../services/cache/cache';
import { Utils } from '../reports/utils';
import { BaseReportManager } from '../models/base-report-manager';
import { Dynamo } from '../services/dynamo';
import PlatformManager from '../services/session/platform-manager.session';
import { redisFactory } from '../services/redis/RedisFactory';

const router = express.Router();

/**
 * This route will load a base session for an anonymous user or the real session for a logged in user
 * @param req Express Request (standard router parameter)
 * @param res Express Response (standard router parameter)
 * @param next Express NextFunction (standard router parameter)
 */
export const loadAnonymousSession = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');

    const utils = new  Utils();

    if (req.headers?.authorization || req.cookies?.hydra_access_token) {
        return await sessionOnlyLoading(req, res, next);
    } else {
        let session: SessionManager;
        try {

            // get the hostname of the platform, we need to check in redis db 10
            let hostname = req.hostname;
            const redis = redisFactory.getRedis();
            let platformParams = await redis.getRedisPlatformParams(hostname);
            if (!platformParams.originalDomain) {
                logger.debug(`No original domain in Redis for the hash ${hostname}`);
                // lets give it a try with a purified hostname - remove www and https from the hostname
                hostname = utils.purifyUrl(hostname);
                platformParams = await redis.getRedisPlatformParams(hostname);
                if (!platformParams.originalDomain) {
                    logger.debug(`No original domain in Redis also for ${hostname}, let's keep using ${req.hostname} as a hostname`);
                    hostname = req.hostname;
                } else {
                    hostname = platformParams.originalDomain;
                }
            } else {
                hostname = platformParams.originalDomain;
            }

            logger.debug(`Using Hostname -> ${hostname}`);
            const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';

            const hydra = new Hydra(hostname, '', subfolder);
            session = await SessionManager.init(hydra, req.app.locals.cache, true);
        } catch (sessionError: any) {
            logger.errorWithStack('Error while creating anonymous session:', sessionError);
            res.sendStatus(500);
            return;
        }
        res.locals.session = session;
        next();
    }
};

export const getRequestInfos = (req: Request) => {
    const methodToUpper = req.method.toUpperCase();
    const isMethodGetOptionsOrHead = methodToUpper === 'GET' || methodToUpper === 'OPTIONS' || methodToUpper === 'HEAD';
    const token = req.headers?.authorization ? req.headers?.authorization : ('Bearer ' + req.cookies?.hydra_access_token);

    return {
        token,
        xCSRFToken: isMethodGetOptionsOrHead ? req.cookies._csrf : req.headers['x-csrf-token'],
        cookie: req.headers.cookie
    };
};

/**
 * This will check for the view permission of the currently logged user
 */
export const checkViewReportPermission = async (req: Request, res: Response, next: NextFunction) => {
    checkReportsPermission(req, res, next);
};

/**
 * This will check for the edit permission of the currently logged user
 */
export const checkUpdateReportPermission = async (req: Request, res: Response, next: NextFunction) => {
    checkReportsPermission(req, res, next, true);
};

/**
 * Check if the user level is super admin
 * @param req [Request] request
 * @param res [Response] response
 * @param next [NextFunction] the next middleware
 */
export const superAdminPermission = (req: Request, res: Response, next: NextFunction) => {

    const session: SessionManager = res.locals.session;
    if (!session || !session.user.isGodAdmin()) {
        const response: BaseResponse = {
            success: false,
            error: 'Unauthorized',
        };

        return res.status(401).json(response);
    }

    next();
};

export const checkRODToggleActivation = (req: Request, res: Response, next: NextFunction) => {

    const session: SessionManager = res.locals.session;
    if (!session || session.platform.isDatalakeV2Active()) {
        const response: BaseResponse = {
            success: false,
            error: 'Not Found',
        };

        return res.status(404).json(response);
    }

    next();
};

export const checkDatalakeV2ToggleActivation = async (req: Request, res: Response, next: NextFunction) => {

    let session: SessionManager = res.locals.session;
    if (!session) {
        const logger: SessionLoggerService = httpContext.get('logger');
        const reqInfos = getRequestInfos(req);
        try {
            const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
            session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
        } catch (sessionError: any) {
            logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
            if (sessionError instanceof NotFoundException) {
                res.sendStatus(404);
            } else if (sessionError instanceof UnauthorizedException) {
                res.sendStatus(401);
            } else {
                res.sendStatus(500);
            }
            return;
        }

    }
    if (!session || !session.platform.isDatalakeV2Active()) {
        return res.sendStatus(404);
    }
    res.locals.session = session;

    next();
};

export const checkDatalakeV2ManualRefreshToggleActivation = async (req: Request, res: Response, next: NextFunction) => {

    let session: SessionManager = res.locals.session;
    if (!session) {
        const logger: SessionLoggerService = httpContext.get('logger');
        const reqInfos = getRequestInfos(req);
        try {
            const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
            session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
        } catch (sessionError: any) {
            logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
            if (sessionError instanceof NotFoundException) {
                res.sendStatus(404);
            } else if (sessionError instanceof UnauthorizedException) {
                res.sendStatus(401);
            } else {
                res.sendStatus(500);
            }
            return;
        }
    }

    if (!session || !session.platform.isDatalakeV2ManualRefreshToggleActive()) {
        return res.sendStatus(404);
    }
    res.locals.session = session;

    next();
};

export const checkPrivacyPolicyOnSnowflake = async (req: Request, res: Response, next: NextFunction) => {

    let session: SessionManager = res.locals.session;
    if (!session) {
        const logger: SessionLoggerService = httpContext.get('logger');
        const reqInfos = getRequestInfos(req);
        try {
            const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
            session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
        } catch (sessionError: any) {
            logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
            if (sessionError instanceof NotFoundException) {
                res.sendStatus(404);
            } else if (sessionError instanceof UnauthorizedException) {
                res.sendStatus(401);
            } else {
                res.sendStatus(500);
            }
            return;
        }

    }
    if (!session || !(session.platform.isDatalakeV3ToggleActive() && session.platform.isPrivacyPolicyDashboardOnAthenaActive())) {
        return res.sendStatus(404);
    }
    if (!session.user.isGodAdmin()) {
        return res.sendStatus(403);
    }
    res.locals.session = session;

    next();
};

export const checkCoursesOnSnowflake = async (req: Request, res: Response, next: NextFunction) => {

    let session: SessionManager = res.locals.session;
    if (!session) {
        const logger: SessionLoggerService = httpContext.get('logger');
        const reqInfos = getRequestInfos(req);
        try {
            const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
            session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
        } catch (sessionError: any) {
            logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
            if (sessionError instanceof NotFoundException) {
                res.sendStatus(404);
            } else if (sessionError instanceof UnauthorizedException) {
                res.sendStatus(401);
            } else {
                res.sendStatus(500);
            }
            return;
        }

    }
    if (!session || !(session.platform.isDatalakeV3ToggleActive() && session.platform.isCoursesDashboardOnAthenaActive())) {
        return res.sendStatus(404);
    }
    res.locals.session = session;

    next();
};

export const checkBranchesOnSnowflake = async (req: Request, res: Response, next: NextFunction) => {
    let session: SessionManager = res.locals.session;

    if (!session) {
        const logger: SessionLoggerService = httpContext.get('logger');
        const reqInfos = getRequestInfos(req);
        try {
            const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
            session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
        } catch (sessionError: any) {
            logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
            if (sessionError instanceof NotFoundException) {
                res.sendStatus(404);
            } else if (sessionError instanceof UnauthorizedException) {
                res.sendStatus(401);
            } else {
                res.sendStatus(500);
            }
            return;
        }
    }
    if (!session || !(session.platform.isDatalakeV3ToggleActive() && session.platform.isBranchesDashboardOnAthenaActive())) {
        return res.sendStatus(404);
    }
    res.locals.session = session;

    next();
};

const checkReportsPermission = async (req: Request, res: Response, next: NextFunction, update = false) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    let session: SessionManager;
    const reqInfos = getRequestInfos(req);
    try {
        const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
        session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
    } catch (sessionError: any) {
        logger.errorWithStack(`Error while trying to fetch session from hydra: ${sessionError.message}`, sessionError);
        if (sessionError instanceof NotFoundException) {
            res.sendStatus(404);
        } else if (sessionError instanceof UnauthorizedException) {
            res.sendStatus(401);
        } else {
            res.sendStatus(500);
        }
        return;
    }

    if (
        (update && !session.permission.canUpdateReportPermission(session.user))
        || (!update && !session.permission.canViewReportPermission(session.user))
    ) {
        res.sendStatus(403);
        return;
    }

    res.locals.session = session;
    next();
};

export const hasQueryBuilderPermission = async(session: SessionManager): Promise<boolean> => {
    if (!session.user.isERPAdmin() && !session.user.isGodAdmin()) {
        return false;
    }
    const queryBuilderAdmins = session.platform.getQueryBuilderAdmins();

    return queryBuilderAdmins.includes(session.user.getUsername());
};

export const checkQueryBuilderPermission = async (req: Request, res: Response, next: NextFunction, update = false) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    let session: SessionManager;
    const reqInfos = getRequestInfos(req);
    try {
        const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
        session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
    } catch (sessionError: any) {
        logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
        if (sessionError instanceof NotFoundException) {
            res.sendStatus(404);
        } else if (sessionError instanceof UnauthorizedException) {
            res.sendStatus(401);
        } else {
            res.sendStatus(500);
        }
        return;
    }

    if (!(await hasQueryBuilderPermission(session))) {
        res.sendStatus(403);
        return;
    }

    res.locals.session = session;
    next();
};

export const checkReportsManagerPermission = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    let session: SessionManager;
    const reqInfos = getRequestInfos(req);
    try {
        const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
        session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
    } catch (sessionError: any) {
        logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
        if (sessionError instanceof NotFoundException) {
            res.sendStatus(404);
        } else if (sessionError instanceof UnauthorizedException) {
            res.sendStatus(401);
        } else {
            res.sendStatus(500);
        }
        return;
    }

    if (!session.permission.canBeManagerPermission(session.user)) {
        res.sendStatus(403);
        return;
    }

    res.locals.session = session;
    next();
};

// Only ERP and SUPER ADMIN can see custom report types
export const checkCustomReportTypePermission = async (req: Request, res: Response, next: NextFunction, update = false) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    let session: SessionManager;
    const reqInfos = getRequestInfos(req);
    try {
        const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
        session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
    } catch (sessionError: any) {
        logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
        if (sessionError instanceof NotFoundException) {
            res.sendStatus(404);
        } else if (sessionError instanceof UnauthorizedException) {
            res.sendStatus(401);
        } else {
            res.sendStatus(500);
        }
        return;
    }

    if (!session.user.isGodAdmin()) {
        res.sendStatus(403);
        return;
    }

    res.locals.session = session;
    next();
};

// Only SA
export const checkArchivedAuditTrailPermission = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    let session: SessionManager;
    const reqInfos = getRequestInfos(req);
    try {
        const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
        session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
    } catch (sessionError: any) {
        logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
        if (sessionError instanceof NotFoundException) {
            res.sendStatus(404);
        } else if (sessionError instanceof UnauthorizedException) {
            res.sendStatus(401);
        } else {
            res.sendStatus(500);
        }
        return;
    }

    if (!session.platform.isAudittrailLegacyArchiveToggleActive()) {
        const response: BaseResponse = {
            success: false,
            error: 'Not Found',
        };
        return res.status(404).json(response);
    }

    if (!session.user.isGodAdmin()) {
        res.sendStatus(403);
        return;
    }

    res.locals.session = session;
    next();
};

/**
 * Gets a user session from hydra based upon current token
 * @throws NotFoundException
 * @throws ServerErrorException
 * @throws UnauthorizedException
 */
export const getSession = async (token: string | undefined, hostname: string, subfolder: string, cache: CacheService, cookie: string, xCSRFToken: string, logger?: SessionLoggerService): Promise<SessionManager> => {
    if (!token) {
        throw new UnauthorizedException('Missing token while trying to fetch session details', ErrorCode.MISSING_TOKEN);
    }

    return await SessionManager.init(new Hydra(hostname, token, subfolder, cookie, xCSRFToken, logger), cache, false, logger);
};

/**
 * This function will check if the current logged user was the creator of the report, for god admin it will allways return true because they have no ownership restriction
 * @param req Express Request (standard router parameter)
 * @param res Express Response (standard router parameter)
 * @param next Express NextFunction (standard router parameter)
 */
export const checkReportOwnership = router.all('/:id_report', async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;

    // God admin have the ownership on every report
    if (session.user.isGodAdmin()) {
        next();
    }

    let reportHandler: BaseReportManager;
    try {
        reportHandler = await ReportManagerSwitcher(session, req.params.id_report);
    } catch (error: any) {
        logger.errorWithStack(error.toString(), error);
        res.type('application/json');
        const response: BaseResponse = {
            success: false
        };
        if (error.message === 'Report type not found!') {
            res.status(400);
            response.error = 'Invalid report type';
        } else if (error instanceof NotFoundException || error instanceof DisabledReportTypeException) {
            res.status(404);
            response.error = 'Report not found!';
            response.errorCode = error.getCode();
        } else {
            res.status(500);
            response.error = error.message;
        }
        res.json(response);
        return;
    }

    // In case of a Power User we have to check the ownership for some specific actions
    if (session.user.isPowerUser() && reportHandler.info.author !== session.user.getIdUser()) {
        res.sendStatus(403);
        return;
    }
});

/**
 * Check if the process that refresh the data lake is in execution. If it's in execution,
 * it returns a DatLakeRefreshInProgress error code
 * @param req {Request} Request
 * @param res {Response} Response
 * @param next {NextFunction} Next function
 */
export const checkDataLakeRefreshStatusMandatory = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;

    const extractionComponent = new ExtractionComponent();

    let refreshInfo: LastRefreshDate;
    try {
        refreshInfo = await extractionComponent.getDataLakeLastRefresh(session);
    } catch (e: any) {
        // no info about the refresh for the platform loaded in this session
        // just for now, we can go on, waiting for a DOC to manage this situation
        logger.debug('We can go to the next middleware because we have no info about the data lake');
        next();
        return;
    }

    if (refreshInfo.refreshStatus === DataLakeRefreshStatus.RefreshInProgress) {
        logger.debug('Stop the execution, refresh in progress and we cannot execute operations on the data lake');
        const response: BaseResponse = {
            error: 'Data lake refresh in progress for this platform',
            success: false,
            errorCode: ErrorsCode.DataLakeRefreshInProgress,
        };
        res.type('application/json');
        return res.json(response);
    }

    next();
};

export const checkExistDataFresher = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const dynamo: Dynamo = session.getDynamo();
    let completeTime = '';

    try {
        completeTime = await dynamo.getDatalakeV2LastCompleteTime();
    } catch (err: any) {
        const response: BaseResponse = {
            error: 'Complete time not found',
            success: false,
            errorCode: ErrorsCode.CompleteTimeNotFound,
        };
        res.type('application/json');
        return res.json(response);
    }

    const platformRefreshDetail = await dynamo.getLastDataLakeUpdate(session);
    if (!platformRefreshDetail) {
        throw new Error('No detail for the refresh status of the platform');
    }

    if (completeTime === platformRefreshDetail.refreshTimezoneLastDateUpdateV2) {
        const response: BaseResponse = {
            error: 'No Data Fresher',
            success: false,
            errorCode: ErrorsCode.NotDataFresher,
        };
        res.type('application/json');
        return res.json(response);
    }

    next();
};
/**
 * Check if the process that refresh the data lake is in execution. If it's in execution and the V2 is not enabled,
 * it returns a DatLakeRefreshInProgress error code otherwise it will go foward with execution of the operations
 * @param req {Request} Request
 * @param res {Response} Response
 * @param next {NextFunction} Next function
 */
export const checkDataLakeRefreshStatus = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;

    const extractionComponent = new ExtractionComponent();

    let refreshInfo: LastRefreshDate;
    try {
        refreshInfo = await extractionComponent.getDataLakeLastRefresh(session);
    } catch (e: any) {
        // no info about the refresh for the platform loaded in this session
        // just for now, we can go on, waiting for a DOC to manage this situation
        logger.debug('We can go to the next middleware because we have no info about the data lake');
        next();
        return;
    }

    if (refreshInfo.refreshStatus === DataLakeRefreshStatus.RefreshInProgress && !session.platform.isDatalakeV2Active()) {
        logger.debug('Stop the execution, refresh in progress and we cannot execute operations on the data lake');
        const response: BaseResponse = {
            error: 'Data lake refresh in progress for this platform',
            success: false,
            errorCode: ErrorsCode.DataLakeRefreshInProgress,
        };
        res.type('application/json');
        return res.json(response);
    }

    next();
};

const sessionOnlyLoading = async (req: Request, res: Response, next: NextFunction, update = false) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    let session: SessionManager;
    const reqInfos = getRequestInfos(req);
    try {
        const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
        session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
    } catch (sessionError: any) {
        logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
        if (sessionError instanceof NotFoundException) {
            res.sendStatus(404);
        } else if (sessionError instanceof UnauthorizedException) {
            res.sendStatus(401);
        } else {
            res.sendStatus(500);
        }
        return;
    }

    res.locals.session = session;
    next();
};

/**
 * Check if the user is an ERP admin
 * @param req [Request] request
 * @param res [Response] response
 * @param next [NextFunction] the next middleware
 */
export const checkERPAdminUser = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    let session: SessionManager;
    const reqInfos = getRequestInfos(req);

    try {
        const subfolder = req.params.subfolder ? req.params.subfolder.replace(/[^a-zA-Z0-9-]/gi, '') : '';
        session = await getSession(reqInfos.token, req.hostname, subfolder, req.app.locals.cache, reqInfos.cookie, reqInfos.xCSRFToken);
    } catch (sessionError: any) {
        logger.errorWithStack('Error while trying to fetch session from hydra: ', sessionError);
        if (sessionError instanceof NotFoundException) {
            res.sendStatus(404);
        } else if (sessionError instanceof UnauthorizedException) {
            res.sendStatus(401);
        } else {
            res.sendStatus(500);
        }
        return;
    }

    if (!session.user.isERPAdmin()) {
        logger.error('Only ERP admins can access this specific API');
        res.sendStatus(403);
        return;
    }

    res.locals.session = session;
    next();
};

export const checkAPIWhitelist = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const utils = new  Utils();
    try {
        // get the hostname of the platform, we need to check in redis db 10
        let hostname = req.hostname;
        const redis = redisFactory.getRedis();
        let platformParams = await redis.getRedisPlatformParams(hostname);
        if (!platformParams.originalDomain) {
            logger.debug(`No original domain in Redis for the hash ${hostname}`);
            // lets give it a try with a purified hostname - remove www and https from the hostname
            hostname = utils.purifyUrl(hostname);
            platformParams = await redis.getRedisPlatformParams(hostname);
            if (!platformParams.originalDomain) {
                logger.debug(`No original domain in Redis also for ${hostname}, let's keep using ${req.hostname} as a hostname`);
                hostname = req.hostname;
            } else {
                hostname = platformParams.originalDomain;
            }
        } else {
            hostname = platformParams.originalDomain;
        }

        const commonKeys = await redis.getRedisCommonParams(hostname);
        const dynamo = new Dynamo(commonKeys.dynamoDbRegion, hostname, '', new PlatformManager());

        const platformSetting = await dynamo.getSettings() as ReportsSettings;

        if (platformSetting.toggleHydraMinimalVersion && !await redis.checkWhitelistedAPI(hostname, req.route.path, req.method)) {
            throw new NotFoundException();
        }
    } catch (sessionError: any) {
        if (sessionError instanceof NotFoundException) {
            res.sendStatus(404);
        } else {
            logger.errorWithStack('Error while creating anonymous session:', sessionError);
            res.sendStatus(500);
        }

        return;
    }

    next();
};
