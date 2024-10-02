import { Application } from './Application';
import { CreateSnowflakeConnectionsPool } from './connections-pool';
import { HTTPFactory } from './services/http/HTTPFactory';
import { AxiosHTTPService } from './services/http/AxiosHTTPService';
import { redisFactory } from './services/redis/RedisFactory';
import { MessageHandlerFactory } from './handlers/message-handler-factory';
import Config from './config';
import customEnv from 'dotenv';
import {
  ReportExtractionApplicationServiceFactory
} from './domain/factories/ReportExtractionApplicationServiceFactory';
import { loggerFactory } from './services/logger/logger-factory';
import { queueConsumerFactory } from './components/queue-consumer-factory';
import { HTTPServiceLogger } from './services/http/HTTPServiceLogger';

HTTPFactory.setHTTPService(
  new HTTPServiceLogger(
    new AxiosHTTPService(),
    loggerFactory.buildLogger('[HTTPService]')
  )
);

const mainLogger = loggerFactory.buildLogger('[main]');
mainLogger.debug({ message: `Creating the Snowflake connections pool` });

CreateSnowflakeConnectionsPool().then(async (connectionsPool) => {
  mainLogger.debug({ message: `Connections pool started, starting the server` });

  customEnv.config();
  const config = new Config();

  const appServiceFactory = new ReportExtractionApplicationServiceFactory(config, redisFactory.getRedis());
  const sidekiq = await redisFactory.getSidekiqClient();
  const messageHandlerFactory = new MessageHandlerFactory(sidekiq);
  const messageHandler = messageHandlerFactory.getMessageHandler(appServiceFactory);

  const numberOfWorkers = config.getDatalakeV3MaxNumberOfWorkers();
  mainLogger.debug({ message: `Creating ${numberOfWorkers} workers to handle SQS messages`});

  const queueConsumer = queueConsumerFactory.buildQueueConsumer(numberOfWorkers);

  const appLogger = loggerFactory.buildLogger('[App]');

  const app = await Application.start(
    config,
    connectionsPool,
    async () => {
      appLogger.debug({ message: `Application started on port ${config.port}` });

      appLogger.debug({ message: 'Starting to consume the messaging queue' });
      queueConsumer.startConsumingQueue(messageHandler);
    },
    async () => {
      appLogger.debug({ message: `Application is being closed` });

      appLogger.debug({ message: `Stopping queue consumer` });
      await queueConsumer.stop();
      appLogger.debug({ message: `Queue consumer stopped` });

      appLogger.debug({ message: `Draining redis and sidekiq connections pool` });
      await redisFactory.drainPools();
      appLogger.debug({ message: `Redis and sidekiq connections pool drained` });
    },
  );

  process.on('SIGTERM', async (signal) => {
    mainLogger.debug({ message: `Received ${signal}, stopping the application` });
    await app.stop();
    mainLogger.debug({ message: `Application stopped` });
  });
});
