export class DoceboSwaggerDocumentator {

    private static REGEX_DOCBLOCK = /\/\*{2}[\s\S]+?\*\//;
    private static REGEX_DOCBLOCK_PARAMS = /@(\w+)[ \t]*(.*)/g;
    private static REGEX_METHODS = /@methods*\s*(\w*)/;
    private static REGEX_STATUS = /(\w+)[ \t]*(.*)$/;
    private static REGEX_WHOLE_ROW = /(\w+[-]?\w+)[ \t]*\[(\w+(?:\(.+\))?),[ \t]*(\w+)\]\s*(.*)/;
    private static REGEX_TYPED_ARRAY = /array\((\w+)\)/;
    private static REGEX_ENUM = /(enum(?:_multiple)?)\(([\w(?:\/\w)\s_\-,]+)\)/;

    private static REGEX_ENUM_RES_TYPE_SINGLE = 1;
    private static REGEX_ENUM_RES_TYPE_MULTIPLE = 2;

    private static MODEL_LOCALIZABLE = 'localizable';

    private globalModel: any = {};
    private guestActions: any = {};
    private apiTable: any = {};

    private objectName = '';
    private mainObjectName = '';
    private static USE_CUSTOM_CATEGORY = true;
    private htmlspecialchars = require('htmlspecialchars');
    private level: string[];
    private isInput = false;


    /**
     * Compile a JSON for Swagger Open Api V2
     */
    public resourceListing() {
        // This is the default relative url
        const tokenUrl = '/oauth2/token';

        let jsonObject: any;
        jsonObject = {
            swagger: '2.0',
            host: false,
            tags: [],
            info: {
                title: 'aamon',
                version: '1.0',
                description: ''
            },
            paths: {},
            securityDefinitions: {
                docebo_oauth: {
                    type: 'oauth2',
                    flow: 'implicit',
                    tokenUrl: `${tokenUrl}`,
                    authorizationUrl: '/oauth2/authorize',
                    scopes: {
                        api: 'Common scope, used by the whole API'
                    },
                },
            },
            security: [{
                docebo_oauth: ['api'],
            }],
        };

        // Build the api module/controllers/actions table
        this.buildModuleTable();

        this.globalModel.definitions = {};
        this.initCommonGlobalModel();

        let category = '';
        let name = '';

        if (DoceboSwaggerDocumentator.USE_CUSTOM_CATEGORY) {
            category = 'Report';
            name = 'Report';
        }

        for (const controllerKey in this.apiTable) {
            if (this.apiTable.hasOwnProperty(controllerKey)) {
                const moduleActionsObject = this.getControllerPaths(this.apiTable[controllerKey], category);
                if (!moduleActionsObject.hasOwnProperty(undefined) && Object.keys(moduleActionsObject).length > 0) {

                    if (!DoceboSwaggerDocumentator.USE_CUSTOM_CATEGORY) {
                        name = this.apiTable[controllerKey].name;
                    }

                    for (const action in moduleActionsObject) {
                        if (jsonObject.paths.hasOwnProperty(action)) {
                            jsonObject.paths[action] = {...jsonObject.paths[action], ...moduleActionsObject[action]};
                        } else {
                            jsonObject.paths[action] = moduleActionsObject[action];
                        }
                    }

                    jsonObject.tags = [{
                        name: `${name}`,
                        description: this.apiTable[controllerKey].description,
                    }];
                }
            }
        }

        jsonObject.definitions = this.globalModel.definitions;
        this.globalModel.definitions = {};


        return JSON.stringify(jsonObject);
    }

    /**
     * Get the controller actions reading alla the action files and parsing the doc block
     * @param controllerData
     * @param category
     * @returns {any}
     */
    private getControllerPaths(controllerData: any, category: string): any {

        const pathInfo: any = {};

        this.getControllerActions(controllerData);

        // Nothing to do here if controller has no actions
        if (this.apiTable[controllerData.name].actions === undefined) {
            return pathInfo;
        }

        let tags = '';

        // Loop through all actions of this app
        // tslint:disable-next-line:prefer-for-of
        for (let index = 0; index < this.apiTable[controllerData.name].actions.length; ++index) {
            const action = this.apiTable[controllerData.name].actions[index];
            const method = (action.method) ? action.method.toString().toLowerCase() : 'post';
            if (action.path !== undefined) {
                tags = controllerData.name;

                if (action.category && DoceboSwaggerDocumentator.USE_CUSTOM_CATEGORY === true) {
                    tags = action.category;
                }

                let operationId = controllerData.name;
                if (controllerData.name.includes('/')) {
                    const lastIndex = controllerData.name.lastIndexOf('/');
                    operationId = controllerData.name.slice(lastIndex + 1);
                }
                pathInfo[action.path] = {};
                pathInfo[action.path][method] = {
                    tags: [tags],
                    operationId: operationId + '.' + action.path,
                    parameters: action.parameters,
                    summary: action.summary,
                    internal: action.internal,
                    description: action.notes,
                    responses: action.error_responses,
                };
            }

        }
        return pathInfo;
    }

    /**
     * Global common model definition
     */
    private initCommonGlobalModel() {

        this.globalModel.definitions[DoceboSwaggerDocumentator.MODEL_LOCALIZABLE] = {
            type: 'object',
            required: ['type'],
            properties: {
                type: {
                    type: 'string',
                    description: 'The type of the localizable object. This controls whether or not the value or values property below will be populated.',
                    collectionFormat: 'brackets',
                    enum: [
                        'single_value',
                        'multi_lang']
                },
                value: {
                    type: 'string',
                    description: 'The scalar value of this localizable object, used for all languages. Only populated when "type=single_value".'
                },
                values: {
                    type: 'object',
                    description: 'A lang_code => value structured object with the different localized names of the item, in each language, available within the platform.',
                }
            },
        };
    }

    /**
     * Common error definition + guest actions errors
     * @param actionObject
     * @param actionName
     */
    private initCommonErrors(actionObject: any, actionName: string) {

        // every action can have err 500
        actionObject.error_responses[500] = {
            description: 'Internal server error',
        };

        // any action that is not a guest action has these statuses
        if (!this.guestActions.hasOwnProperty(actionName)) {
            actionObject.error_responses[401] = {
                description: 'You are requesting with an invalid credential.',
            };

            actionObject.error_responses[403] = {
                description: 'Permission denied',
            };
        }
    }

    /**
     * Module controller listing
     */
    private buildModuleTable() {
        // Build the table of APIs to be parsed
        // Phase 1. Discover core api modules

        const path = require('path');
        const actionFolder: string = path.resolve(__dirname, '../../src/routes/');
        const actionFolderModels = path.resolve(__dirname, '../../src/models/');
        const fs = require('fs');
        const actionPathsGlobal = {};
        actionPathsGlobal[actionFolder] = fs.readdirSync(actionFolder);
        actionPathsGlobal[actionFolderModels] = fs.readdirSync(actionFolderModels);
        for (const [actionFolder, actionPaths] of Object.entries(actionPathsGlobal)) {
            // @ts-ignore
            // tslint:disable-next-line:prefer-for-of
            for (let indexPaths = 0; indexPaths < actionPaths.length; ++indexPaths) {
                if (fs.statSync(path.resolve(actionFolder, actionPaths[indexPaths])).isDirectory()) {
                    const actionFiles: string[] = fs.readdirSync(actionFolder + '/' + actionPaths[indexPaths]);
                    // tslint:disable-next-line:prefer-for-of
                    for (let indexFiles = 0; indexFiles < actionFiles.length; ++indexFiles) {
                        this.buildApiControllerStructure('aamon', actionPaths[indexPaths], actionFolder + '/' + actionPaths[indexPaths], actionFiles[indexFiles]);
                    }
                } else {
                    this.buildApiControllerStructure('aamon', actionFolder, actionFolder, actionPaths[indexPaths]);
                }
            }
        }


    }

    /**
     * Api controller listing
     * @param {string} moduleName
     * @param {string} controllerName
     * @param {string} actionPath
     * @param {string} actionFile
     */
    private buildApiControllerStructure(moduleName: string, controllerName: string, actionPath: string, actionFile: string) {
        if (!this.apiTable[controllerName]) {
            this.apiTable[controllerName] = {
                module: moduleName,
                name: controllerName,
                description: '',
                actions: []
            };
        }

        this.apiTable[controllerName].actions.push({
            file: actionFile,
            file_path: actionPath + '/'
        });
    }

    /**
     * Parse the controllers and extract comment API documentation
     * @param {string} controllerData
     * @returns {string} JSON Swagger OpenAPI
     */
    private getControllerActions(controllerData: any) {
        // tslint:disable-next-line:prefer-for-of
        for (let index = 0; index < this.apiTable[controllerData.name].actions.length; ++index) {
            const action: any = this.apiTable[controllerData.name].actions[index];
            this.parseActionComments(controllerData.module, controllerData.name, action);
        }
    }

    /**
     * Reads the file and searches for the Swagger API doc block, if found generates a
     * JSON Swagger OpenAPI compliant object
     * @param moduleName
     * @param controllerName
     * @param action
     * @returns {Object}
     */
    private parseActionComments(moduleName: string, controllerName: string, action: any) {
        let param: any;
        let modelsStack: any = [];
        let actionObject: any = {};

        // Reading comment from actual source file
        const fs = require('fs');
        const fileContent = fs.readFileSync(action.file_path + action.file, 'utf8');

        let actionName = '';
        let returnObjectModel = '';
        let schemaName = '';

        this.guestActions = {};

        this.level = [];
        const regexpDocblock: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_DOCBLOCK);
        if (regexpDocblock.test(fileContent)) {
            const docBlock: string = regexpDocblock.exec(fileContent).toString();

            // Determining if comment is for Swagger API
            if (docBlock.indexOf('@summary') > 0 ||
                docBlock.indexOf('@url') > 0) {
                const file = action.file;
                let controllerNameModified = controllerName;
                if (controllerName.includes('/')) {
                    const lastIndex = controllerName.lastIndexOf('/');
                    controllerNameModified = controllerName.slice(lastIndex + 1);
                }
                returnObjectModel = this.ucFirst(moduleName) + this.ucFirst(controllerNameModified) + this.ucFirst(file.split('.')[0]) + 'Response';
                actionName =  action.file.replace(/-/gi, '_').split('.')[0];
                actionObject = {};
                actionObject.error_responses = {
                    200: {
                        description: 'Operation Successful',
                        schema: {
                            $ref: '#/definitions/' + returnObjectModel,
                        }
                    }
                };
                actionObject.parameters = [];
                // Parsing comment for JSON Swagger API generation
                // Determining action method
                const actionMethod = this.resolveActionMethod(docBlock);

                // Parsing parameters if any
                if (actionMethod !== 'GET' && this.resolveHasParameters(docBlock)) {
                    const paramData: any = {
                        in: 'body',
                        name: 'body',
                        description: 'Raw Body',
                        required: false,
                        schema: []
                    };

                    let controllerNameModified = controllerName;
                    if (controllerName.includes('/')) {
                        const lastIndex = controllerName.lastIndexOf('/');
                        controllerNameModified = controllerName.slice(lastIndex + 1);
                    }
                    schemaName = this.ucFirst(moduleName) + this.ucFirst(controllerNameModified) + this.ucFirst(file.split('.')[0]) + 'InputSchema';
                    this.objectName = schemaName;

                    this.globalModel.definitions[schemaName] = {type: 'object'};
                    paramData.schema = {$ref: '#/definitions/' + schemaName};
                    actionObject.parameters.push(paramData);
                }

                modelsStack = [];
                const regexpDocBlockParams: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_DOCBLOCK_PARAMS);
                while ((param = regexpDocBlockParams.exec(docBlock)) !== null) {

                    const paramKey = param[1];
                    let paramLine = '';
                    switch (paramKey) {
                        case 'consumer':
                            const paramDataConsumer: any = {in: 'header'};
                            const regexpConsumer: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_WHOLE_ROW);
                            if (regexpConsumer.test(param[2])) {
                                const rowList = regexpConsumer.exec(param[2]);
                                let type = rowList[2];
                                const enumType = this.checkForEnums(type, DoceboSwaggerDocumentator.REGEX_ENUM_RES_TYPE_MULTIPLE);

                                if (enumType.length > 0) {
                                    paramDataConsumer.enum = enumType;
                                    // determine if the data in enum is integer or string
                                    type = this.getEnumElementsType(paramDataConsumer.enum);
                                }

                                paramDataConsumer.name = rowList[1];
                                paramDataConsumer.type = type;
                                paramDataConsumer.required = rowList[3] === 'required';
                                paramDataConsumer.description = this.htmlspecialchars(rowList[4]);

                                actionObject.parameters.push(paramDataConsumer);
                            }

                            break;
                        case 'summary': // Short summary of the api action
                            modelsStack = [];
                            actionObject[param[1]] = param[2];
                            break;
                        case 'internal':
                            modelsStack = [];
                            actionObject[param[1]] = true;
                            break;
                        case 'notes': // Extended notes on the action (can be multiline)
                            modelsStack = [];
                            if (typeof actionObject[param[1]] !== 'undefined') {
                                actionObject[param[1]] = actionObject[param[1]] + param[2];
                            } else {
                                actionObject[param[1]] = param[2];
                            }
                            break;

                        case 'status': // Status code responses
                            modelsStack = [];
                            modelsStack[1] = '';
                            const statusLine: string = param[2];

                            const regexpStatus: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_STATUS);
                            if (regexpStatus.test(statusLine)) {
                                const statusList = regexpStatus.exec(statusLine);
                                const errorCode = statusList[1];
                                const errorMessage = statusList[2];

                                // if it isn't a standard code. The second condition interval can be changed
                                if (parseInt(errorCode) > 600 || (parseInt(errorCode) >= 15 && parseInt(errorCode) <= 35)) {
                                    if (actionObject.error_responses[400] === undefined) {
                                        actionObject.error_responses[400] = {description: 'General Error'};
                                    }

                                    if (actionObject.error_responses[400].error_codes === undefined) {
                                        actionObject.error_responses[400].error_codes = {};
                                    }

                                    actionObject.error_responses[400].error_codes[errorCode] = statusList[2];
                                }
                                 else {
                                    actionObject.error_responses[errorCode] = {description: errorMessage};
                                }
                            }
                            break;

                        case 'url': // API url
                            actionObject.path = param[2];
                            break;

                        case 'get':
                            // Nothing to do here, if no url is provided.
                            // This will happen only if @get is used before @url in the doc block
                            if (actionObject.path !== undefined) {
                                break;
                            }

                            const paramData: any = {in: 'path'};
                            const regexp: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_WHOLE_ROW);
                            if (regexp.test(param[2])) {
                                const rowList = regexp.exec(param[2]);
                                let type = rowList[2];

                                const enumType = this.checkForEnums(type, DoceboSwaggerDocumentator.REGEX_ENUM_RES_TYPE_MULTIPLE);
                                if (enumType.length > 0) {
                                    paramData.enum = enumType;
                                    // determine if the data in enum is integer or string
                                    type = this.getEnumElementsType(paramData.enum);
                                }

                                paramData.name = rowList[1];
                                paramData.type = type;
                                paramData.required = rowList[3] === 'required';
                                paramData.description = this.htmlspecialchars(rowList[4]);

                                actionObject.parameters.push(paramData);
                            }
                            break;

                        case 'response':
                            paramLine = param[2];
                            modelsStack = [];
                            this.isInput = false;
                            this.objectName = returnObjectModel.replace('/-/ig', '_') + 'Schema';
                            this.buildModel(modelsStack, returnObjectModel, paramLine, moduleName, controllerName, actionName);
                            break;

                        case 'parameter':
                            paramLine = param[2];
                            this.isInput = true;
                            if (actionMethod === 'GET') {
                                // when the method is GET, we use normal get parameters
                                this.buildGETParameters(actionObject, paramLine);
                            } else {
                                // build body parameter
                                modelsStack = [];
                                this.buildModel(modelsStack, schemaName, paramLine, moduleName, controllerName, actionName);
                            }
                            break;

                        case 'method':
                            modelsStack = [];
                            actionObject[param[1]] = param[2];
                            break;

                        case 'category':
                            modelsStack = [];
                            actionObject.category = param[2];
                            break;

                        case 'item':
                            if (modelsStack !== 'undefined' &&
                                modelsStack.length > 0) {
                                paramLine = param[2];
                                const currentModel = this.ucFirst(moduleName) + this.ucFirst(controllerName) + this.ucFirst(file.split('.')[0]) + 'Schema';
                                this.buildModel(modelsStack, currentModel.replace('/-/ig', '_'), paramLine, moduleName, controllerName, actionName);
                            } else {
                                // TODO Yii::warning("Can't use @end on an empty model stack. Please check the PHPDoc of action " . $actionClassReflector->name);
                            }
                            break;

                        case 'end':
                            this.level.pop();
                            modelsStack.pop();
                            break;
                    }
                }

                if (actionObject.summary) {
                    this.initCommonErrors(actionObject, actionName);
                    action = Object.assign(action, actionObject);
                }
            } else {
                action = {};
            }
        }
    }

    /**
     * Builds the GET parameter list
     * @param action
     * @param {string} paramLine
     */
    private buildGETParameters(action: any, paramLine: string) {

        const paramData: any = {
            in: 'query',
        };

        const regexpEnum: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_WHOLE_ROW);
        if (regexpEnum.test(paramLine)) {
            const rowList: string[] = regexpEnum.exec(paramLine);
            let type = rowList[2];
            const regexp: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_TYPED_ARRAY);
            if (regexp.test(type)) {
                const typeList = regexp.exec(type)[1];
                paramData.collectionFormat = 'brackets';
                paramData.items = {type: typeList};
            }

            const enumType = this.checkForEnums(type, DoceboSwaggerDocumentator.REGEX_ENUM_RES_TYPE_MULTIPLE);
            if (enumType.length > 0) {
                paramData.enum = this.checkForEnums(type, DoceboSwaggerDocumentator.REGEX_ENUM_RES_TYPE_MULTIPLE);

                // determine if the data in enum is integer or string
                const enumElementsType = this.getEnumElementsType(paramData.enum);

                // single select
                if (enumType === 'enum') {
                    type = enumElementsType;
                } else { // multi select (REGEX_ENUM_RES_TYPE_MULTIPLE)
                    type = 'array';
                    paramData.collectionFormat = 'brackets';
                    paramData.items = {type: enumElementsType};
                }
                paramData.enum = enumType;
                // determine if the data in enum is integer or string
                type = this.getEnumElementsType(paramData.enum);
            }

            paramData.name = rowList[1];
            paramData.type = type;
            paramData.required = rowList[3] === 'required';
            paramData.description = this.htmlspecialchars(rowList[4]);

            action.parameters.push(paramData);
        }
    }

    /**
     * Method is used to build both body '@parameter', '@item' and '@response' Objects/Models
     *
     * *** Method not suitable for formData Parameter, only for BodyParameter & Response because they share the same structure ***
     *
     * @param modelsStack
     * @param object
     * @param paramLine
     * @param moduleName
     * @param controllerName
     * @param actionName
     */
    private buildModel(modelsStack: any[], object: string, paramLine: string, moduleName: string, controllerName: string, actionName: string) {
        const regexpEnum: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_WHOLE_ROW);
        let currentObject = object;

        if (this.isInput === true) {
            controllerName = controllerName + 'Input';
        }
        if (regexpEnum.test(paramLine)) {
            const rowList: string[] = regexpEnum.exec(paramLine);
            const fieldName: string = rowList[1];
            const type: string = this.parseType(rowList[2]);
            const required: string = rowList[3];
            const description: string = this.htmlspecialchars(rowList[4].trim());
            let modelName: string = undefined;
            this.initGlobalDefinitionsObject(object);

            // Array type handler
            const regexp: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_TYPED_ARRAY);
            if (regexp.test(type)) {
                const typeList = regexp.exec(type)[1];

                if (this.level.length > 0) {
                    currentObject = this.level[this.level.length - 1 ];
                }

                this.initGlobalDefinitionsObject(currentObject);
                if (required === 'required') {
                    this.setFieldAsRequired(currentObject, fieldName);
                }

                this.globalModel.definitions[currentObject].properties[fieldName] = {
                    type: 'array',
                    description: `${description}`,
                    items: {
                        type: typeList
                    }
                };

                return;
            }

            // Enum type handler
            const enumType: string = this.checkForEnums(type, DoceboSwaggerDocumentator.REGEX_ENUM_RES_TYPE_SINGLE);
            if (enumType !== undefined &&
                enumType.length > 0) {
                const fieldData: any = {
                    description: `${description}`,
                    enum: this.checkForEnums(type, DoceboSwaggerDocumentator.REGEX_ENUM_RES_TYPE_MULTIPLE),
                };

                // determine if the data in enum is INT or String
                const enumElementsType = this.getEnumElementsType(fieldData.enum);

                // single select
                if (enumType === 'enum') {
                    fieldData.type = enumElementsType;
                } else { // multi select (REGEX_ENUM_RES_TYPE_MULTIPLE)
                    fieldData.type = 'array';
                    fieldData.collectionFormat = 'brackets';
                    fieldData.items = {type: enumElementsType};
                }

                if (required === 'required') {
                    this.setFieldAsRequired(object, fieldName);
                }
                this.globalModel.definitions[object].properties[fieldName] = fieldData;

                return;
            }
            let objectStructure = '';
            // Other types handlers
            switch (type.toLowerCase()) {

                case 'array':
                    modelName = this.ucFirst(moduleName) + this.ucFirst(object) + this.ucFirst(controllerName) + this.ucFirst(fieldName) + this.ucFirst(actionName);
                    this.objectName = modelName;
                    const arrayStructure = object.replace('Response', 'Schema');
                    objectStructure = modelName + 'Schema';

                    if (this.level.length > 0) {
                        currentObject = this.level[this.level.length - 1 ];
                    }
                   // objectStructure = arrayStructure;
                    this.level.push(objectStructure);

                    this.initGlobalDefinitionsObject(currentObject);

                    if (required === 'required') {
                        this.setFieldAsRequired(currentObject, fieldName);
                    }
                    this.globalModel.definitions[modelName] = [];
                    this.globalModel.definitions[currentObject].properties[fieldName] = {
                        type: 'array',
                        description: `${description}`,
                        items: {
                            $ref: '#/definitions/' + objectStructure,
                        },
                    };

                    modelsStack.push(modelName);
                    break;

                case 'object':
                    modelName = this.ucFirst(moduleName) + this.ucFirst(controllerName) + this.ucFirst(fieldName) + this.ucFirst(actionName);
                    this.objectName = modelName;
                    if (this.level.length > 0) {
                        currentObject = this.level[this.level.length - 1 ];
                    }

                    objectStructure = modelName + 'Schema';
                    this.level.push(objectStructure);

                    this.globalModel.definitions[modelName] = [];

                    this.initGlobalDefinitionsObject(currentObject);
                    if (required === 'required') {
                        this.setFieldAsRequired(currentObject, fieldName);
                    }

                    this.globalModel.definitions[currentObject].properties[fieldName] = {
                        $ref: '#/definitions/' + objectStructure,
                        description: `${description}`,
                    };


                    modelsStack.push(modelName);
                    break;

                case 'date':
                case 'datetime':
                    // datetime can have its own case, but the format is not the same as the one we use, its format is: "2016-05-19T07:40:40.656Z"
                    // based on RFC3339 ( http://xml2rfc.ietf.org/public/rfc/html/rfc3339.html#anchor14 )
                    if (required === 'required') {
                        this.setFieldAsRequired(currentObject, fieldName);
                    }
                    this.globalModel.definitions[object].properties[fieldName] = {
                        type: 'string',
                        format: 'date',
                        description: `${description}`
                    };
                    break;

                case 'float':
                    if (required === 'required') {
                        this.setFieldAsRequired(currentObject, fieldName);
                    }
                    this.globalModel.definitions[object].properties[fieldName] = {
                        type: 'number',
                        format: 'float',
                        description: `${description}`
                    };
                    break;

                case 'double':
                    if (required === 'required') {
                        this.setFieldAsRequired(currentObject, fieldName);
                    }
                    this.globalModel.definitions[object].properties[fieldName] = {
                        type: 'number',
                        format: 'double',
                        description: `${description}`
                    };
                    break;

                case 'localizable':
                    if (required === 'required') {
                        this.setFieldAsRequired(currentObject, fieldName);
                    }
                    this.globalModel.definitions[object].properties[fieldName] = {
                        $ref: '#/definitions/' + DoceboSwaggerDocumentator.MODEL_LOCALIZABLE,
                        description: `${description}`
                    };
                    break;

                default:
                    if (this.level[this.level.length - 1] !== undefined) {
                        object = this.objectName + 'Schema';
                    }

                    this.initGlobalDefinitionsObject(object);
                    if (required === 'required') {
                        this.setFieldAsRequired(object, fieldName);
                    }
                    this.globalModel.definitions[object].properties[fieldName] = {
                        type: `${type}`,
                        description: `${description}`
                    };
                    break;
            }
        }
    }


    /**
     * Finds the action method define indesed the doc block
     * @param docBlock
     * @returns {string} action method
     */
    private resolveActionMethod(docBlock: string): string {

        const regexp: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_METHODS);
        if (docBlock !== undefined && regexp.test(docBlock)) {
            return regexp.exec(docBlock)[1].toString().toUpperCase();
        } else {
            return 'POST';
        }
    }

    /**
     * Determines if the doc block has parameters
     * @param docBlock
     * @returns {boolean} true has parameters / false no parameters found
     */
    private resolveHasParameters(docBlock: string): boolean {

        return (docBlock !== undefined && docBlock.indexOf('@parameter') > 0);
    }

    /**
     * Given a string parses it for enumerators and returns the enumerators in Array<string>
     * @param {string} input
     * @param {number} resType
     * @returns {Array<string>}
     */
    private checkForEnums(input: string, resType: number): any {
        if (resType === DoceboSwaggerDocumentator.REGEX_ENUM_RES_TYPE_SINGLE ||
            resType === DoceboSwaggerDocumentator.REGEX_ENUM_RES_TYPE_MULTIPLE) {
            const regexpEnum: RegExp = new RegExp(DoceboSwaggerDocumentator.REGEX_ENUM);
            if (regexpEnum.test(input)) {
                const enumList = regexpEnum.exec(input);
                return this.parseEnums(enumList[resType]);
            }
        }

        return [];
    }

    /**
     * Parse enum type inside a doc block param
     * @param {string} enumString
     * @returns {Array}
     */
    private parseEnums(enumString: string): string[] {

        const enumList = enumString.split(',');
        if (enumList instanceof Array) {
            return enumList.map(str => {
                return str.trim();
            });
        } else {
            return [];
        }
    }

    /**
     * Given Array enum list check all elements against int, if so returns type integer otherwise return string
     * @param {Array<string>} enumList
     * @returns {string}
     */
    private getEnumElementsType(enumList: string[]): string {
        const values = enumList.filter((value) => {
            // If any one of the elements is not integer, then we need to use string as type
            if (!(/^\d+$/.test(value))) {
                return value;
            }
        });
        return values.length === 0 ? 'integer' : 'string';
    }

    /**
     * Given a string returns it's defined type or self if no match found
     * @param {string} input
     * @returns {string}
     */
    private parseType(input: string): string {

        switch (input.toLowerCase()) {
            case 'url':
            case 'time':
            case 'mixed':
                return 'string';

            case 'decimal':
                return 'double';

            case 'bool':
                return 'boolean';

            case 'int':
                return 'integer';

            case 'file':
                return 'string';
        }

        return input;
    }

    /**
     * Like Php ucfist
     * @param {string} input
     * @returns {string}
     */
    private ucFirst(input: string): string {
        if (input) {
            input = input.charAt(0).toUpperCase() + input.slice(1);
        }

        return input;
    }

    private initGlobalDefinitionsObject(object: string) {
        if (this.globalModel.definitions[object] === undefined) {
            this.globalModel.definitions[object] = {};
        }
        if (this.globalModel.definitions[object].properties === undefined) {
            this.globalModel.definitions[object] = {properties: {}};
        }
    }

    private setFieldAsRequired(object: string, fieldName: string) {
        this.globalModel.definitions[object].required = this.globalModel.definitions[object].required || [];
        this.globalModel.definitions[object].required.push(fieldName);
    }
}
