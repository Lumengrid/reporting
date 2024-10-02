import Hydra from '../../services/hydra';
import {
    AuthorName,
    CustomReportType,
    DynamoCustomReportType
} from '../../query-builder/interfaces/query-builder.interface';
import { Dynamo } from '../../services/dynamo';

/**
 * This component take the values stored in dynamo and returs the final data for the custom report type
 */
export class CustomReportTypesComponent {

    constructor(
        private readonly dynamoService: Dynamo,
        private readonly hydraService: Hydra) { }

    async getCustomReportTypes(): Promise<CustomReportType[]> {

        // get getCustomReportTypes from Dynamo DB
        const dynamoCustomReportTypes: DynamoCustomReportType[] = await this.dynamoService.getCustomReportTypes() as DynamoCustomReportType[];

        // convert the data to aamon format
        return await this.convertDynamoCustomReportTypesMetadataToAamonReportsMetadata(dynamoCustomReportTypes) as CustomReportType[];
    }

    /**
     * Retrieve the array of actives custom report types
     */
    async getActiveCustomReportTypes(): Promise<object[]> {
        // get active custom report type from Dynamo DB
        const dynamoCustomReportTypes: DynamoCustomReportType[] = await this.dynamoService.getActiveCustomReportTypes() as DynamoCustomReportType[];
        const activeList = [];
        for (const dynamoCustomReportType of dynamoCustomReportTypes) {
            const customReportType: any = {};

            customReportType.id = dynamoCustomReportType.id;
            customReportType.name = dynamoCustomReportType.name;
            customReportType.description = dynamoCustomReportType.description;

            activeList.push(customReportType);
        }

        return activeList;
    }

    private async convertDynamoCustomReportTypesMetadataToAamonReportsMetadata(dynamoCustomReportTypes: DynamoCustomReportType[]): Promise<CustomReportType[]> {

        // List of enriched reports
        const aamonReports: CustomReportType[] = [];

        // List of authors enriched objects
        const aamonCustomReportTypeAuthors: Map<number, AuthorName> =
            new Map<number, AuthorName>();

        for (const dynamoCustomReportType of dynamoCustomReportTypes) {
            const aamonReport = this.convertToAamonCustomReportTypesMetadata(dynamoCustomReportType);

            aamonReports.push(aamonReport);
            aamonCustomReportTypeAuthors.set(aamonReport.authorId as number, { firstname: '', lastname: '', username: '' });
        }

        await this.getUserInfoDetailFromHydra(aamonCustomReportTypeAuthors);

        // Add the createdBy field to the final custom report type
        this.addAuthorNameToCustoReportType(aamonReports, aamonCustomReportTypeAuthors);

        return aamonReports;
    }

    private convertToAamonCustomReportTypesMetadata(dynamoCustomReportType: DynamoCustomReportType): CustomReportType {
        const aamonCustomReportType: CustomReportType = {} as CustomReportType;

        aamonCustomReportType.id = dynamoCustomReportType.id;
        aamonCustomReportType.name = dynamoCustomReportType.name;
        aamonCustomReportType.description = dynamoCustomReportType.description;
        aamonCustomReportType.authorId = dynamoCustomReportType.authorId !== 0 ? dynamoCustomReportType.authorId : '';
        aamonCustomReportType.creationDate = dynamoCustomReportType.creationDate;
        aamonCustomReportType.status = dynamoCustomReportType.status;

        return aamonCustomReportType;
    }

    // Associate the user info like username, firstname and lastname to the corresponding ids passed as parameters
    private async getUserInfoDetailFromHydra(aamonCustomReportTypeAuthors: Map<number, AuthorName>): Promise<void> {
        const usersData = await this.hydraService.getUsers([...aamonCustomReportTypeAuthors.keys()]);

        // For each userId match the corresponding firstname and lastname
        for (const author in usersData.data) {
            aamonCustomReportTypeAuthors.set(parseInt(author, 10), {
                firstname: usersData.data[author].firstname,
                lastname: usersData.data[author].lastname,
                username: usersData.data[author].userid,
            });
        }

    }

    // Add User info like firstname, lastname and username to the createdBy field
    private addAuthorNameToCustoReportType(aamonCustomReportType: CustomReportType[], aamonCustomReportTypeAuthors: Map<number, AuthorName>): void {
        for (const customReportType of aamonCustomReportType) {
            const customReportTypeAuthorInfo = aamonCustomReportTypeAuthors.get(customReportType.authorId as number);

            if (customReportTypeAuthorInfo) {
                customReportType.createdBy = {
                    firstname: customReportTypeAuthorInfo.firstname,
                    lastname: customReportTypeAuthorInfo.lastname,
                    username: customReportTypeAuthorInfo.username,
                };
            }
        }
    }

}
