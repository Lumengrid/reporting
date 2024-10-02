export interface S3FileSystem {
	fileIsEmpty(bucketName: string, filePath: string): Promise<boolean>;
	createFile(bucketName: string, filePath: string, fileContent: string, contentType?: string): Promise<void>;
	compressFile(
		bucketName: string,
		sourceFilePath: string,
		sourceFileName: string,
		targetFileName: string,
		contentFileName: string,
	): Promise<void>;
	deleteFile(bucketName: string, filePath: string): Promise<void>;
}
