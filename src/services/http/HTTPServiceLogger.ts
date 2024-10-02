import { HTTPResponse, HTTPService } from './HTTPService';
import { LoggerInterface } from '../logger/logger-interface';

export class HTTPServiceLogger implements HTTPService {
	public constructor(
		private readonly innerHttpService: HTTPService,
		private readonly logger: LoggerInterface,
	) {}

	public async call(options: any): Promise<HTTPResponse> {
		const t0 = Date.now();
		let response: any;
		let errorCaught: any;

		try {
			response = await this.innerHttpService.call(options);
			return response;
		} catch (error: any) {
			errorCaught = error;
			throw error;
		} finally {
			const dt = (Date.now() - t0);

			this.logger.debug({
				message: `Call to ${options.method} ${options.baseURL}${options.url} performed in ${dt} ms (status: ${response?.statusText ?? errorCaught?.code ?? '(?)'})`,
			});
		}
	}
}
