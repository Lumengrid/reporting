import { Exception } from '../../exceptions/exception';

// Thrown when Redis connection goes down or cannot be instantiated
export class RedisConnectionException extends Exception {}

export class RedisMissingMandatoryKeyException extends Exception {}
