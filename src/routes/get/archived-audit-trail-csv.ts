import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { BaseResponse } from '../../models/base';
import moment from 'moment';
import { v4 } from 'uuid';
import { ArchivedAuditTrailManager } from '../../archived-audit-trail/manager/archived-audit-trail-manager';
import { BadRequestException } from '../../exceptions/bad-request.exception';
import { redisFactory } from '../../services/redis/RedisFactory';

export const archivedAuditTrailCsv = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const queryExecutionId = req.params.query_execution_id;
    const responseOptions: BaseResponse = {success: true};

    res.type('application/json');

    try {
        const redis = redisFactory.getRedis();
        await ArchivedAuditTrailManager.existsQueryExecutionIdOnRedis(redis, session.user.getIdUser(), queryExecutionId);
        await ArchivedAuditTrailManager.isQuerySucceeded(session.getAthena(), queryExecutionId);

        const s3 = session.getS3();
        const stream = await s3.getReportExtractionDownloadStream(queryExecutionId, 'csv');
        const extractionDateToFormat = moment(new Date()).format('YYYYMMDD');
        const randomId = v4();
        const fileName = `${extractionDateToFormat}_${randomId}.csv`;

        res.setHeader(
            'Content-disposition',
            `attachment; filename=${fileName}`
        );
        res.setHeader('Content-type', `text/csv`);


        res.setHeader('Access-Control-Expose-Headers', 'Content-disposition');
        stream.pipe(res, {end: true});
        res.status(200);
    } catch (e: any) {
        logger.errorWithStack(`Error on get csv for QueryExecutionId ${queryExecutionId}`, e);
        responseOptions.success = false;
        if (e instanceof BadRequestException) {
            res.status(400);
            responseOptions.error = e.message;
            responseOptions.errorCode = e.getCode();
        } else {
            responseOptions.error = 'Generic error. See the log';
            res.status(500);
        }
        res.json(responseOptions);
    }
};
