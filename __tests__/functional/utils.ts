import { Exception } from '../../src/exceptions';
import AWS, { AWSError, DynamoDB } from 'aws-sdk';
import { AttributeMap, BatchWriteItemInput, BatchWriteItemOutput, } from 'aws-sdk/clients/dynamodb';
import { PromiseResult } from 'aws-sdk/lib/request';
import * as dotenv from 'dotenv';
import * as fs from 'fs';
import { ReportManagerInfo } from '../../src/models/report-manager';

export class FunctionalTestUtils {
  private static dynamo: AWS.DynamoDB;
  private static awsCredentials = {
    region: 'us-east-1'
  };

  static init() {
    AWS.config.update(this.awsCredentials);
    this.dynamo = new AWS.DynamoDB();
  }

  static loadTestEnv(): void {
    const pathEnvTest = '.env-test';

    if (fs.existsSync(pathEnvTest)) {
      dotenv.config({ path: `${pathEnvTest}` });
    }
  }


  static async truncateTable(targetTable: string, params?: Object): Promise<void> {
    if (!this.dynamo) throw new Error('Dynamo is null, please call init()');
    const items: AttributeMap[] = await this.getAllReportsFromTable(targetTable);
    const deletionPromises: Promise<PromiseResult<BatchWriteItemOutput, AWSError>>[] = [];
    const batchDeleteRequest: BatchWriteItemInput = {
      RequestItems: {},
      ReturnConsumedCapacity: 'NONE',
      ReturnItemCollectionMetrics: 'NONE'
    };
    batchDeleteRequest.RequestItems[targetTable] = [];
    for (let startIndex = 0; startIndex < items.length; startIndex = startIndex + 25) {
      const thisBatchPutRequest = Object.assign(batchDeleteRequest, {});
      let keyValue = {};
      for (let index = +startIndex; index < startIndex + 25; index++) {
        if (!items[index]) continue;
        if (targetTable === process.env.REPORTS_TABLE) {
          keyValue = {idReport: items[index].idReport, platform: items[index].platform};
        } else {
          keyValue = {id: items[index].id, platform: items[index].platform};
        }
        thisBatchPutRequest.RequestItems[targetTable].push({
          DeleteRequest: {
            Key: keyValue
          }
        });
      }

      deletionPromises.push(this.dynamo.batchWriteItem(thisBatchPutRequest).promise());
    }
    await Promise.all(deletionPromises);
  }

  static async putItemsToTable(targetTable: string, items: AttributeMap[]): Promise<void> {
    if (!this.dynamo) throw new Error('Dynamo is null, please call init()');
    const putPromises: Promise<PromiseResult<BatchWriteItemOutput, AWSError>>[] = [];
    const batchPutRequest: BatchWriteItemInput = {
      RequestItems: {},
      ReturnConsumedCapacity: 'NONE',
      ReturnItemCollectionMetrics: 'NONE'
    };
    batchPutRequest.RequestItems[targetTable] = [];
    for (let startIndex = 0; startIndex < items.length; startIndex = startIndex + 25) {
      const thisBatchPutRequest = Object.assign(batchPutRequest, {});

      for (let index = +startIndex; index < startIndex + 25; index++) {
        if (!items[index]) continue;
        thisBatchPutRequest.RequestItems[targetTable].push({
          PutRequest: {
            Item: items[index]
          }
        });
      }

      putPromises.push(this.dynamo.batchWriteItem(thisBatchPutRequest).promise());
    }
    await Promise.all(putPromises);
  }

  static async getAllReportsFromTable(targetTable: string, params?: Object): Promise<AttributeMap[]> {
    if (!this.dynamo) throw new Error('Dynamo is null, please call init()');
    const scan: DynamoDB.Types.ScanOutput = await this.dynamo.scan(
      Object.assign({
        TableName: targetTable,
      }, params)
    ).promise();
    if (!scan.Items || scan.Items.length === 0) return [];
    if (typeof scan.LastEvaluatedKey !== 'undefined') {
      return [...scan.Items, ...await this.getAllReportsFromTable(targetTable, {ExclusiveStartKey: scan.LastEvaluatedKey})];
    }
    return scan.Items;
  }


  // copy current json file content to a table
  static async loadFixtureFromFile(targetTable: string, filePath: string): Promise<DynamoDB.AttributeMap[]> {
    await this.truncateTable(targetTable);
    const fixtureFileContent = fs.readFileSync(filePath, 'utf8');
    const fixtureFileContentParsed: ReportManagerInfo[] = JSON.parse(fixtureFileContent);
    const items = fixtureFileContentParsed.map(item => AWS.DynamoDB.Converter.marshall(item));
    await this.putItemsToTable(targetTable, items);

    return items;
  }

  // copy custom_reports_test current content to another table
  static async loadFixture(targetTable: string): Promise<void> {
    await this.truncateTable(targetTable);
    const items: AttributeMap[] = await this.getAllReportsFromTable(targetTable);
    this.putItemsToTable(targetTable, items);
  }

  // We made a backup of the table custom_reports_test
  // With this function, you can restore that backup to another table
  // ALERT: Calling this function, you launch the restore, but you can't know how much time it will take
  // you can check it on aws console
  static async createTestTableFromBackup(targetTable: string): Promise<void> {
    if (!this.dynamo) throw new Error('Dynamo is null, please call init()');
    try {
      await this.dynamo.deleteTable({
        TableName: targetTable
      }).promise();
    } catch (error) {
      // table not existing, it's ok
    }

    const listBackups: DynamoDB.ListBackupsOutput = await this.dynamo.listBackups({
      TableName: targetTable
    }).promise();

    if (!listBackups.BackupSummaries) {
      throw new Exception('No Backups');
    }
    const backupSummary = listBackups.BackupSummaries[listBackups.BackupSummaries.length - 1];

    if (!backupSummary.BackupArn) {
      throw new Exception('No Backups');
    }

    // Here you are only launching the restore! No way to know when it's finished.
    this.dynamo.restoreTableFromBackup({
      TargetTableName: targetTable,
      BackupArn: backupSummary.BackupArn
    });
  }
}
