import { Pool } from 'generic-pool';
import { DBConnection } from './services/snowflake/interfaces/snowflake.interface';
import { DeclareExpressRoutes } from './express-app';
import http from 'http';
import https from 'https';
import fs from 'fs';
import express, { Request } from 'express';
import cookieParser from 'cookie-parser';
import compression from 'compression';
import httpContext from 'express-http-context';
import Config from './config';
import httpLogger from 'morgan';
import { Logger, LogLevel } from './services/logger/logger';
import CacheService from './services/cache/cache';
import { SessionLoggerService } from './services/logger/session-logger.service';
import { BaseResponse } from './models/base';

type AppCallback = () => Promise<void>;
const Noop: AppCallback = () => undefined;

export class Application {
  public static async start(
    config: Config,
    pool: Pool<DBConnection>,
    onStart: AppCallback = Noop,
    onEnd: AppCallback = Noop,
  ): Promise<Application> {
    global.snowflakePool = pool;
    const app: express.Application = express();

    app.use(express.json());
    app.use(express.urlencoded({extended: false}));
    app.use(cookieParser());
    app.use(compression());

    // Health-check
    const urlPrefix = config.urlPrefix;
    app.get(`/:subfolder?${urlPrefix}/health`, (req, res) => {
      res.status(200);
      res.send('OK');
      res.end();
    });

    app.use(httpContext.middleware);

    // set morgan to log using the logger instance
    const morganOptions = {
      stream: {
        write(logMsg: string): void {
          const splittedLogMsg = logMsg.trim().split(' ');
          const hostname = splittedLogMsg.pop();
          Logger.log(splittedLogMsg.join(' '), LogLevel.info, hostname + '');
        }
      }
    };

    httpLogger.token('hostname', (req: Request) => req.hostname);
    app.use(httpLogger(':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent" :hostname', morganOptions));

    // set the logger
    await Logger.createLoggerInstance(config, '');

    // Set global app
    const cache = new CacheService(0);
    app.locals.cache = cache;

    // instance of logger for each request
    app.use((req: Request, res, next) => {
      const logger = new SessionLoggerService(req.hostname);
      httpContext.set('logger', logger);
      next();
    });

    await DeclareExpressRoutes(app, config);

    // catch JSON error in body
    app.use((err, req, res, next) => {
      if ('type' in err && err.type === 'entity.parse.failed' && 'status' in err && err.status === 400 && err instanceof SyntaxError) {
        res.status(400);

        const response: BaseResponse = {
          success: false,
          error: 'Wrong JSON in body request'
        };

        res.json(response);
      }
    });

    process.on('unhandledRejection', (reason, p) => {
      const logger = new SessionLoggerService('unhandledRejection');
      logger.errorWithStack('Unhandled Rejection at Promise: ', reason);
      return;
    }).on('uncaughtException', (err) => {
      const logger = new SessionLoggerService('uncaughtException');
      logger.errorWithStack('Uncaught Exception: ', err);
      return;
    });

    let server: http.Server | https.Server;

    if (config.secureConnection) {
        const options = {
            key: fs.readFileSync(config.sslKeyFile),
            cert: fs.readFileSync(config.sslCertFile),
        };
        server = https.createServer(options, app);
    } else {
        server = http.createServer({}, app);
    }

    server.keepAliveTimeout = 301 * 1000;

    server.listen(
      config.port,
      async () => {
        console.debug(`Express started on port ${config.port}`);
        await onStart();
      }
    );

    return new Application(
      server,
      cache,
      pool,
      onEnd,
    );
  }

  private constructor(
    private readonly server: http.Server | https.Server,
    private readonly cache: CacheService,
    private readonly snowflakePool: Pool<DBConnection>,
    private readonly onEndCallback: AppCallback,
  ) {
  }

  public async stop(): Promise<void> {
    this.server.keepAliveTimeout = 0;
    this.server.close();

    await this.onEndCallback();

    await this.snowflakePool.drain();
    await this.snowflakePool.clear();

    this.cache.close();
  }
}
