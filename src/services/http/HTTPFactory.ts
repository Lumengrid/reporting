import { HTTPService } from './HTTPService';

export class HTTPFactory {
  private static service: HTTPService;

  public static setHTTPService(service: HTTPService): void {
    this.service = service;
  }

  public static getHTTPService(): HTTPService {
    return this.service;
  }
}