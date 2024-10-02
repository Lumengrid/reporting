import { AbstractCache } from './AbstractCache';

export class NullCache extends AbstractCache {

  public set(key: string, value: any, ttl = 0): void {}

  public get(key: string): any {
    return undefined;
  }

  public del(key: string): void {

  }

  public flush(): void {
  }

  public exists(key: string): boolean {
    return false;
  }

  public close(): void {
  }
}
