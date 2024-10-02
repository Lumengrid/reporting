import AWS from 'aws-sdk';
import stream from 'stream';
import archiver from 'archiver';
import exceljs from 'exceljs';
import csv from 'csv-parser';
import { SessionLoggerService } from './logger/session-logger.service';
import httpContext from 'express-http-context';
import fs from 'fs';
import { Utils } from '../reports/utils';

export class S3 {
    protected region: string;
    protected bucket: string;
    protected s3Path: string;
    protected s3ExportPath: string;
    protected logger: SessionLoggerService;

    protected bucketV3: string;

    protected s3: AWS.S3;

    public constructor(region: string, bucket: string, s3Path: string, s3ExportPath: string, bucketV3: string) {
        this.region = region;
        this.bucket = bucket;
        this.s3Path = s3Path;
        this.s3ExportPath = s3ExportPath;
        this.bucketV3 = bucketV3;

        const awsCredentials = {
            region: this.region
        };
        AWS.config.update(awsCredentials);
        // AWS.config.logger = console; // Uncomment this to have a full log in console for the AWS sdk
        this.s3 = new AWS.S3();
        this.logger = httpContext.get('logger');
    }

    protected streamTo(folder: string, file: string, extension: string, v3 = false, forcedBucket?: string): stream.PassThrough {
        try {
            const pass = new stream.PassThrough();
            const bucket = forcedBucket ?? (v3 ? this.bucketV3 : this.bucket);
            this.s3.upload({
                Bucket: bucket,
                Key: folder + '/' + file + '.' + extension,
                Body: pass
            }, (error: Error, data: AWS.S3.ManagedUpload.SendData) => {
                if (error) {
                    throw error;
                }
            });
            return pass;
        } catch (error: any) {
            throw error;
        }
    }

    protected async streamFrom(folder: string, file: string, extension: string, v3 = false, forcedBucket?: string): Promise<stream.Readable> {
        try {
            const bucket = forcedBucket ?? (v3 ? this.bucketV3 : this.bucket);
            // check the file exists with the headObject call
            await this.s3.headObject({Bucket: bucket, Key: folder + '/' + file + '.' + extension}).promise();
            return this.s3.getObject({
                Bucket: bucket,
                Key: folder + '/' + file + '.' + extension
            }).createReadStream();
        } catch (error: any) {
            throw(error);
        }
    }

    public async getFileSize(file: string, extension: string, v3 = false, forcedBucket?: string): Promise<number> {
        try {
            const bucket = forcedBucket ?? (v3 ? this.bucketV3 : this.bucket);
            const folder = v3 ? 'snowflake-exports' : this.s3ExportPath.split(/[/]+/).pop();
            const params: AWS.S3.Types.HeadObjectRequest = {
                Bucket: bucket,
                Key: folder + '/' + file + '.' + extension
            };
            const response = await this.s3.headObject(params).promise();

            return response.ContentLength ?? -1;
        } catch (error: any) {
            if (error.code === 'NotFound') {
                return -1;
            }
            throw error;
        }
    }

    protected async deleteFile(folder: string, file: string, extension: string) {
        try {
            return await this.s3.deleteObject({
                Bucket: this.bucket,
                Key: folder + '/' + file + '.' + extension
            }).promise();
        } catch (error: any) {
            throw(error);
        }
    }

    public async updateFile(file: string, extension: string, content: string, v3 = true, forcedBucket?: string): Promise<void> {
        const bucket = forcedBucket ?? (v3 ? this.bucketV3 : this.bucket);
        const folder = v3 ? 'snowflake-exports' : this.s3ExportPath.split(/[/]+/).pop();

        const putParams = {
            Bucket: bucket,
            Key: folder + '/' + file + '.' + extension,
            Body: content,
            ContentType: 'text/csv',
        };

        await this.s3.putObject(putParams).promise();
    }

    protected getDownloadUrl(folder: string, file: string, extension: string, v3 = false, expires = 24 * 60 * 60): Promise<string> {
        let bucket = this.bucket;
        if (v3) {
            bucket = this.bucketV3;
        }
        return new Promise(async (resolve, reject) => {
            this.s3.getSignedUrl('getObject', {
                Bucket: bucket,
                Key: folder + '/' + file + '.' + extension,
                Expires: expires
            }, (error: Error, url: string) => {
                if (error) {
                    reject(error);
                }
                resolve(url);
            });
        });
    }

    public async getExtractionDownloadUrl(idExtraction: string, extension = 'zip', v3 = false, expires = 24 * 60 * 60): Promise<string> {
        let folder = this.s3ExportPath.split(/[/]+/).pop();
        if (v3) {
            folder = 'snowflake-exports';
        }
        try {
            return await this.getDownloadUrl('' + folder, idExtraction, extension, v3, expires);
        } catch (error: any) {
            throw(error);
        }
    }

    public async getReportExtractionDownloadStream(idExtraction: string, extension: string, v3 = false): Promise<stream.Readable> {
        let folder = this.s3ExportPath.split(/[/]+/).pop();
        if (v3) {
            folder = 'snowflake-exports';
        }
        try {
            return await this.streamFrom('' + folder, idExtraction, extension, v3);
        } catch (error: any) {
            throw(error);
        }
    }

    public async compressFile(file: string, extension: string, fileTo: string, v3 = false, forcedBucket?: string): Promise<void> {
        return new Promise(async (resolve, reject) => {
            try {
                const folder = v3
                    ? 'snowflake-exports'
                    : this.s3ExportPath.split(/[/]+/).pop();

                const streamFrom = await this.streamFrom('' + folder, file, extension, v3, forcedBucket);

                const streamTo = this.streamTo('' + folder, file, 'zip', v3, forcedBucket);
                streamTo.once('error', reject);

                streamTo.once('end', () => {
                    streamTo.removeListener('error', reject);
                    resolve();
                });

                const zipper = archiver('zip');
                zipper.once('error', reject);

                zipper.once('end', () => {
                    zipper.removeListener('error', reject);
                });

                zipper.pipe(streamTo);

                zipper.append(
                  streamFrom,
                  { name: `${fileTo}.${extension}` },
                );

                await zipper.finalize();
            } catch (error: any) {
                reject(error);
            }
        });
    }

    public async convertCsvToXlsx(file: string, spreadsheetName: string, v3 = false) {
        return new Promise(async (resolve, reject) => {
            let folder = this.s3ExportPath.split(/[/]+/).pop();
            if (v3) {
                folder = 'snowflake-exports';
            }
            try {
                const streamFrom = await this.streamFrom('' + folder, file, 'csv', v3);
                const streamTo = this.streamTo('' + folder, file, 'xlsx', v3);

                streamTo.on('error', reject);

                const options = {
                    stream: streamTo
                };
                const workbook = new exceljs.stream.xlsx.WorkbookWriter(options);
                const worksheet = workbook.addWorksheet(spreadsheetName.substr(0, 30));

                let first = true;
                streamFrom.pipe(csv())
                    .on('data', async (data) => {
                        if (first) {
                            first = false;
                            const keys = Object.keys(data);
                            worksheet.addRow(keys).commit();
                        }
                        const values = Object.values(data);
                        if (values.length > 0) {
                            worksheet.addRow(values).commit();
                        }
                    })
                    .on('error', reject)
                    .on('end', async () => {
                        workbook.commit().then(async () => {
                            let bucket = this.bucket;
                            if (v3) {
                                bucket = this.bucketV3;
                            }
                            this.waitFileUpload(bucket, folder + '/' + file + '.xlsx').then(resolve);
                        });
                    });
            } catch (e: any) {
                reject(e);
            }
        });
    }

    public async waitFileUpload(bucket: string, file: string) {
        while (true) {
            try {
                await this.s3.headObject({Bucket: bucket, Key: file}).promise();
            } catch (e: any) {
                if (e.code === 'NotFound') {
                    await Utils.sleep(500);
                    continue;
                }
            }
            return;
        }
    }

    public async uploadTempTableFile(name: string): Promise<string> {
        return new Promise((resolve, reject) => {
            const path = `./tmp/${name}.csv`;
            const streamFrom = fs.createReadStream(path);

            this.s3.putObject({
                Bucket: this.bucket,
                Key: `tmp_tables/${name}/${name}.csv`,
                Body: streamFrom
            }, (err) => {
                if (err) {
                    reject(err);
                }
            });

            resolve(`s3://${this.bucket}/tmp_tables/${name}/`);
        });
    }

    public async deleteTempTableFile(name: string) {
        return new Promise(async (resolve, reject) => {
            await this.deleteFile(`tmp_tables/${name}`, name, 'csv');
            resolve(undefined);
        });
    }

    /**
     * Check if the xlsx file already exists in the export bucket
     * Returns true: if exists, false: otherwise
     * @param nameFile
     */
    public async checkIfXlsxFileExists(nameFile: string, v3 = false): Promise<boolean> {
        let folder = this.s3ExportPath.split(/[/]+/).pop();
        if (v3) {
            folder = 'snowflake-exports';
        }

        let bucket = this.bucket;
        if (v3) {
            bucket = this.bucketV3;
        }

        try {
            await this.s3.headObject({Bucket: bucket, Key: folder + '/' + nameFile + '.xlsx'}).promise();
            return true;
        } catch (error: any) {
            return false;
        }
    }

    /**
     * Check if the csv file already exists in the export bucket
     * Returns true: if exists, false: otherwise
     * @param nameFile
     */
    public async checkIfCsvFileExists(nameFile: string, v3 = false): Promise<boolean> {
        let folder = this.s3ExportPath.split(/[/]+/).pop();
        if (v3) {
            folder = 'snowflake-exports';
        }

        let bucket = this.bucket;
        if (v3) {
            bucket = this.bucketV3;
        }

        try {
            await this.s3.headObject({Bucket: bucket, Key: folder + '/' + nameFile + '.csv'}).promise();
            return true;
        } catch (error: any) {
            return false;
        }
    }
}
