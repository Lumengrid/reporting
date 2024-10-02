import { HTTPService, HTTPResponse } from './HTTPService';
import axios, { AxiosRequestConfig } from 'axios';

export class AxiosHTTPService implements HTTPService {
    public async call(options: AxiosRequestConfig): Promise<HTTPResponse> {
        const ret = await axios.request(options);

        return {
            ...ret,
            data: ret.data,
        };
    }

}
