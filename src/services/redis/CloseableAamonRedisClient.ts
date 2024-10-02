import { AamonRedisClient } from './AamonRedisClient';

export interface CloseableAamonRedisClient extends AamonRedisClient {
  close(): Promise<void>;
}
