#!/usr/bin/env node_modules/.bin/ts-node

import { DoceboSwaggerDocumentator } from './src/docebo-swagger-documentator';
// tslint:disable-next-line:no-implicit-dependencies
import { Command } from 'commander';


const mainFunction = () => {
    const path = require('path');
    const swaggerJsonPath: string = path.resolve(__dirname, '../src/');

    const doceboDocGenerator = new DoceboSwaggerDocumentator();
    const response: string = doceboDocGenerator.resourceListing();

    const fs = require('fs');

    if (!fs.existsSync(swaggerJsonPath)) {
        fs.mkdirSync(swaggerJsonPath, 0o744);
    }

    fs.writeFile(swaggerJsonPath + '/swagger.json', response, error => {
        if (error) {
            return console.log(error);
        }
    });

    console.log('Swagger doc generated!');
};

const program = new Command();

program
    .version('1.0')
    .command('generate')
    .description('Generate Swagger JSON OpenAPI 2.0 documentation for the given aamon module')
    .action(mainFunction);

program.parse(process.argv);
