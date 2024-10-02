import { Application } from '../../src/Application';
import { DBConnection } from '../../src/services/snowflake/interfaces/snowflake.interface';
import { CreateSnowflakeConnectionsPool } from '../../src/connections-pool';
import axios, { AxiosRequestConfig, AxiosResponse, Method } from 'axios';
import { Pool } from 'generic-pool';
import Config from '../../src/config';
import { createHandyClient } from 'handy-redis';
import { v4 } from 'uuid';
import { loadTestEnv } from '../utils';
import { redisFactory } from '../../src/services/redis/RedisFactory';

export type BodyParams = {
  readonly [key: string]: unknown;
};

export type QueryStringParams = {
  readonly [key: string]: string;
};

export type Headers = {
  readonly [key: string]: string;
};

export class TestApp {
  static async createApp(
    logOnExit = false,
  ): Promise<TestApp> {

    loadTestEnv();
    const connectionsPool = await CreateSnowflakeConnectionsPool({
      minSize: 5,
      maxSize: 5,
      acquireTimeoutMillis: 10000,
      idleTimeoutMillis: 1000,
    });

    await connectionsPool.ready();

    const config = new Config();
    config.port = 3000;

    const app = await Application.start(
      config,
      connectionsPool,
      async () => {},
      async () => {}
    );

    return new TestApp(app, connectionsPool, config);
  }

  private constructor(
    private readonly app: Application,
    private readonly pool: Pool<DBConnection>,
    private readonly config: Config,
  ) {}

  private generateRandomBearerToken(): string {
    return v4().replace(/-/g, '');
  }

  public async stop(): Promise<void> {
    await this.app.stop();
  }

  private async doCall(
    method: Method,
    path: string,
    queryStringParams?: QueryStringParams,
    headers?: Headers,
    body?: BodyParams,
  ): Promise<AxiosResponse> {
    if (path.startsWith('/')) {
      path = path.substring(1);
    }

    let url = `http://localhost:${this.config.port}/${path}`;

    if (queryStringParams) {
      url += '?' + new URLSearchParams(queryStringParams);
    }

    if (!headers) {
      headers = {};
    }

    const requestOptions: AxiosRequestConfig = {
      method,
      url,
      timeout: 15000,
    };

    if (body) {
      const encodedData = JSON.stringify(body);
      requestOptions.data = encodedData;

      headers = {
        ...headers,
        'Content-Type': 'application/json; encoding=UTF-8',
        'Content-Length': `${encodedData.length}`,
      };
    }

    requestOptions.headers = headers;

    return axios(requestOptions);
  }

  public async doGET(path: string, queryString?: QueryStringParams, headers?: Headers): Promise<AxiosResponse> {
    return this.doCall('GET', path, queryString, headers);
  }

  public async doPOST(
    path: string,
    body?: BodyParams,
    queryString?: QueryStringParams,
  ): Promise<AxiosResponse> {
    return this.doCall('POST', path, queryString, {}, body);
  }

  public async generateBearerTokenForUser(host: string, userId: number): Promise<string> {
    const randomToken = this.generateRandomBearerToken();

    const redisClient = createHandyClient({
      host: this.config.getRedisHost(),
      port: this.config.getRedisPort(),
      db: 5,
    });

    try {
      await redisClient.set(
        `${host}:oauth_access_tokens:${randomToken}`,
        JSON.stringify({
          access_token: randomToken,
          client_id: 'hydra_frontend',
          user_id: userId,
          expires: Math.floor(Date.now() / 1000) + 300,
          scope: 'api',
          generated_by: undefined,
        })
      );

      return randomToken;
    } finally {
      await redisClient.quit();
    }
  }
}
