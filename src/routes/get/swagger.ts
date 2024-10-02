import { NextFunction, Request, Response } from 'express';
import httpContext from 'express-http-context';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import SessionManager from '../../services/session/session-manager.session';
import { redisFactory } from '../../services/redis/RedisFactory';

/**
 * @category Report
 * @summary Get swagger file
 * @internal
 * @method GET
 * @url /analytics/v1/swagger
 *
 * @response string [string, required] swagger json string
 */
export const getSwagger = async (req: Request, res: Response, next: NextFunction) => {
    const fs = require('fs');
    const SWAGGER_DOC_PATH = '/../..';
    const path = require('path');
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;

    try {
        const swaggerPath = __dirname + SWAGGER_DOC_PATH + '/swagger.json';
        const fileContent = fs.readFileSync(path.resolve(swaggerPath), 'utf8');
        const swaggerDoc = JSON.parse(fileContent);
        const enableInternalApi = await redisFactory.getRedis().getEnableInternalApi(session.platform.getPlatformBaseUrl());

        if (enableInternalApi === false) {
            for (const field of Object.keys(swaggerDoc.paths)) {
                const method = Object.keys(swaggerDoc.paths[field])[0];
                if (swaggerDoc.paths[field][method].hasOwnProperty('internal')) {
                    const schemaResponse = swaggerDoc.paths[field][method]?.responses['200']?.schema?.$ref ?? '';
                    const schemaResponseName = swaggerDoc.definitions[schemaResponse.split('/').pop()]?.properties?.data?.$ref ?? '';
                    const schemaInputName = swaggerDoc.paths[field][method]?.parameters[0]?.schema?.$ref ?? '';

                    if (schemaResponseName !== '') {
                        delete swaggerDoc.definitions[schemaResponseName.split('/').pop()];

                    }
                    if (schemaResponse !== '') {
                        delete swaggerDoc.definitions[schemaResponse.split('/').pop()];

                    }
                    if (schemaInputName !== '') {
                        delete swaggerDoc.definitions[schemaInputName.split('/').pop()];

                    }
                    delete swaggerDoc.paths[field][method];

                    if (Object.keys(swaggerDoc.paths[field]).length === 0) {
                        delete swaggerDoc.paths[field];

                    }
                }
            }
        }
        res.status(200);
        res.send(swaggerDoc);
    } catch (e: any) {
        logger.errorWithStack('Error on genertion Swagger documentation', e);
        res.sendStatus(500);
    }
};
