export interface HTTPResponse {
  readonly data: any;
}

export interface HTTPService {
  call(options: any): Promise<HTTPResponse>;
}
