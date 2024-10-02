export interface AamonRedisClient {
  sendCommand(command: string, params: readonly any[], db: number): Promise<any>;
}
