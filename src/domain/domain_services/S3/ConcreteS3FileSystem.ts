import {
	DeleteObjectCommand,
	GetObjectCommand,
	HeadObjectCommand,
	NotFound,
	PutObjectCommand,
	S3Client
} from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { Upload } from '@aws-sdk/lib-storage';
import { Stream } from 'stream';
import archiver from 'archiver';
import { S3FileSystem } from './S3FileSystem';

function trimSlashes(s: string): string {
	if (s.startsWith('/')) {
		s = s.substring(1);
	}

	if (s.endsWith('/')) {
		s = s.substring(0, s.length - 1);
	}

	return s;
}

export class ConcreteS3FileSystem implements S3FileSystem {
	public constructor(
		private readonly s3Client: S3Client,
		private readonly basePath = '/',
	) {
		this.basePath = trimSlashes(basePath);
	}


	private normalizePath(path: string): string {
		if (this.basePath === '') {
			return trimSlashes(path);
		}

		return `${this.basePath}/${trimSlashes(path)}`;
	}

	public async fileIsEmpty(bucketName: string, filePath: string): Promise<boolean> {
		try {
			const response = await this.s3Client.send(new HeadObjectCommand({
				Bucket: bucketName,
				Key: this.normalizePath(filePath),
			}));

			return response.ContentLength === 0;
		} catch (error: any) {
			if (error instanceof NotFound) {
				return true;
			}

			throw error;
		}
	}

	public async createFile(bucketName: string, filePath: string, fileContent: string, contentType?: string): Promise<void> {
		const normalizedFilePath = this.normalizePath(filePath);

		await this.s3Client.send(new PutObjectCommand({
			Bucket: bucketName,
			Key: normalizedFilePath,
			ContentType: contentType ?? 'text/plain',
			Body: fileContent,
		}));
	}

	private async startCompression(
		bucketName: string,
		sourceFilePath: string,
		sourceFileName: string,
		targetFileName: string,
		contentFileName: string,
	): Promise<void> {
		const sourceFile = await this.s3Client.send(new GetObjectCommand({
			Bucket: bucketName,
			Key: this.normalizePath(`${sourceFilePath}/${sourceFileName}`),
		}));

		const sourceStream = Stream.PassThrough.from(sourceFile.Body);
		const passThroughStream = new Stream.PassThrough();
		const zipper = archiver('zip');

		zipper.once('error', (error) => {
			throw error;
		});

		zipper.pipe(passThroughStream);

		zipper.append(
			<Readable>sourceStream,
			{
				name: contentFileName,
			}
		);

		const upload = new Upload({
			client: this.s3Client,
			params: {
				Bucket: bucketName,
				Key: this.normalizePath(`${sourceFilePath}/${targetFileName}`),
				Body: <Readable>passThroughStream,
				ContentType: 'application/zip',
			},
			queueSize: 1,
		});

		zipper.finalize();

		await upload.done();
	}

	public async compressFile(
		bucketName: string,
		sourceFilePath: string,
		sourceFileName: string,
		targetFileName: string,
		contentFileName: string,
	): Promise<void> {
		await this.startCompression(bucketName, sourceFilePath, sourceFileName, targetFileName, contentFileName);
	}

	public async deleteFile(bucketName: string, filePath: string): Promise<void> {
		await this.s3Client.send(new DeleteObjectCommand({
			Bucket: bucketName,
			Key: this.normalizePath(filePath),
		}));
	}
}
