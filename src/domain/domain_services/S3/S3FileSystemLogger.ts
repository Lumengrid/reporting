import { S3FileSystem } from './S3FileSystem';
import { LoggerInterface } from '../../../services/logger/logger-interface';

export class S3FileSystemLogger implements S3FileSystem {
	public constructor(
		private readonly innerFileSystem: S3FileSystem,
		private readonly logger: LoggerInterface,
	) {
	}

	private logDebug(message: string): void {
		this.logger.debug({ message });
	}

	private async takeTime(callable: () => Promise<any>, message: string): Promise<any> {
		const t0 = Date.now();

		try {
			return await callable();
		} finally {
			const dt = Date.now() - t0;
			this.logDebug(`${message} (${dt.toFixed(3)} ms)`);
		}
	}

	public async fileIsEmpty(bucketName: string, filePath: string): Promise<boolean> {
		this.logDebug(`Checking if file s3://${bucketName}/${filePath} is empty`);

		return this.takeTime(
			() => this.innerFileSystem.fileIsEmpty(bucketName, filePath),
			'Checked if file was empty',
		);
	}

	public async createFile(bucketName: string, filePath: string, fileContent: string, contentType?: string): Promise<void> {
		this.logDebug(`Creating file s3://${bucketName}/${filePath}`);

		return this.takeTime(
			() => this.innerFileSystem.createFile(bucketName, filePath, fileContent, contentType),
			'File created',
		);
	}

	public async compressFile(bucketName: string, sourceFilePath: string, sourceFileName: string, targetFileName: string, contentFileName: string): Promise<void> {
		this.logDebug(`Compressing file s3://${bucketName}/${sourceFilePath}/${sourceFileName}`);

		return this.takeTime(
			() => this.innerFileSystem.compressFile(bucketName, sourceFilePath, sourceFileName, targetFileName, contentFileName),
			'File compressed',
		);
	}

	public async deleteFile(bucketName: string, filePath: string): Promise<void> {
		this.logDebug(`Deleting file s3://${bucketName}/${filePath}`);

		return this.takeTime(
			() => this.innerFileSystem.deleteFile(bucketName, filePath),
			'File deleted',
		);
	}
}
