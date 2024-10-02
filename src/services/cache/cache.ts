import NodeCache from 'node-cache';
import { AbstractCache } from './AbstractCache';

export default class Cache implements AbstractCache {
    private cache: NodeCache;
    public constructor(ttl = 0) {
        this.cache = new NodeCache({stdTTL: ttl});
    }

    /**
     * Stores a value in cache, optionally expiring it after some time
     *
     * @param key
     * @param value
     * @param ttl The TTL in seconds, use 0 to make the item never expire
     */
    public set(key: string, value: any, ttl = 0): void {
        this.cache.set(key, value, ttl);
    }

    public get(key: string): any {
        return this.cache.get(key);
    }

    public del(key: string): void {
        this.cache.del(key);
    }

    public flush(): void {
        this.cache.flushAll();
    }

    public exists(key: string): boolean {
        return this.cache.has(key);
    }

    public close(): void {
        this.cache.close();
    }
}
