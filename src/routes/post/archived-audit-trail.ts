import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import moment from 'moment';
import { BaseResponse, ErrorsCode, GeneralErrorResponse } from '../../models/base';
import { OLD_AUDITTRAIL_CONTEXT } from '../../shared/constants';
import { redisFactory } from '../../services/redis/RedisFactory';

export const postArchivedAuditTrail = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;

    const startTime = req.body.startTime ? req.body.startTime : undefined;
    let endTime = req.body.endTime ? req.body.endTime : undefined;
    const eventName = req.body.eventName ? req.body.eventName : [];
    const responseOptions: BaseResponse = {success: true};
    const dateTimeFormat = 'YYYY-MM-DD HH:mm:ss';

    // Body validation
    if (!startTime) {
        res.type('application/json');
        res.status(400);
        res.json(new GeneralErrorResponse('Missing required parameter "startTime"', ErrorsCode.WrongParameter));
        return;
    }

    if (!endTime) {
        endTime = new Date();
        endTime = moment(endTime).format(dateTimeFormat);
    }

    if (startTime === '' || !moment(startTime, dateTimeFormat, true).isValid()) {
        res.type('application/json');
        res.status(400);
        res.json(new GeneralErrorResponse(`Property 'startTime' is not valid. Format accepted '${dateTimeFormat}'`, ErrorsCode.WrongParameter));
        return;
    }

    if (endTime === '' || !moment(endTime, dateTimeFormat, true).isValid()) {
        res.type('application/json');
        res.status(400);
        res.json(new GeneralErrorResponse(`Property 'endTime' is not valid. Format accepted '${dateTimeFormat}'`, ErrorsCode.WrongParameter));
        return;
    }

    try {
        const partitionStart = moment(startTime).format('YYYY/MM/DD');
        const partitionEnd = moment(endTime).format('YYYY/MM/DD');

        const athena = session.getAthena();
        const redis = redisFactory.getRedis();

        const legacyAuditTrailDBName = await redis.getLegacyAuditTrailLogsDBName();
        const legacyAuditTrailLogsTableName = await redis.getLegacyAuditTrailLogsTableName();

        const sql = `SELECT id, iduser, action, targetcourse, idtarget, targetuser, jsondata, ip, timestamp
            FROM ${legacyAuditTrailLogsTableName}
            WHERE platform = '${session.platform.getPlatformBaseUrlPath()}'
            AND datehour >= '${partitionStart}' AND datehour <= '${partitionEnd}'
            ${eventName.length > 0 ? `AND action IN ('${eventName.join('\', \'')}')` : ''}
            ORDER BY timestamp DESC`;

        const data = await athena.runCSVExport(sql, false, legacyAuditTrailDBName);
        const queryExecutionId = data.QueryExecutionId;

        await redis.saveQueryExecutionIdOnRedis(session.user.getIdUser(), queryExecutionId, OLD_AUDITTRAIL_CONTEXT);

        res.type('application/json');
        res.status(200);
        responseOptions.data = {QueryExecutionId: queryExecutionId};
    } catch (err: any) {
        logger.errorWithStack('Error on retrieve archived audit trail logs', err);
        res.type('application/json');
        res.status(500);
        responseOptions.success = false;
        responseOptions.error = 'Generic Error. See the log';
    }
    res.json(responseOptions);
};
