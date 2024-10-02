export abstract class AbstractCache {
  public abstract set(key: string, value: any, ttl: number): void;

  public abstract get(key: string): any;

  public abstract del(key: string): void;

  public abstract flush(): void;

  public abstract exists(key: string): boolean;

  public abstract close(): void;
}
