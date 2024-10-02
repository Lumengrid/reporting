import SessionManager from '../../services/session/session-manager.session';
import { UserAdditionalFieldType } from '../../models/custom-report';
import { UserExtraFieldsWithDropdownOptionsResponse } from '../../services/hydra';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';

export class UserAdditionalFieldsComponent {
    session: SessionManager;
    private logger: SessionLoggerService;
    constructor(session: SessionManager) {
        this.session = session;
        this.logger = httpContext.get('logger');
    }

    async getUserAdditionalFields(userId?: number): Promise<any> {

        let dropdownFields: UserAdditionalFieldType[];
        try {
            dropdownFields = await this.retrieveDropdownUserAdditionalFields(userId);
        } catch (e: any) {
            this.logger.errorWithStack(`Cannot retrieve user additional fields.`, e);
            throw e;
        }
        return dropdownFields;
    }

    private async retrieveDropdownUserAdditionalFields(userId?: number): Promise<UserAdditionalFieldType[]> {
        let response: UserExtraFieldsWithDropdownOptionsResponse = {
            data: []
        };
        try {
            response = await this.session.getHydra().getUserExtraFieldsWithDropdownOptions(userId);
        } catch (e: any) {
            console.log(e);
        }
        const fields: UserAdditionalFieldType[] = [];
        for (const field of response.data) {
            if (field.type === 'dropdown') {
                const tmp: UserAdditionalFieldType = {
                    id: parseInt(field.id, 10),
                    title: field.title,
                    sequence: field.sequence,
                    options: []
                };

                for (const option of field.options) {
                    tmp.options.push({
                        value: parseInt(option.id, 10),
                        label: option.label
                    });
                }

                fields.push(tmp);
            }
        }
        return fields;
    }
}

