import { NextFunction, Request, Response } from 'express';
import SessionManager from '../../services/session/session-manager.session';
import { SessionLoggerService } from '../../services/logger/session-logger.service';
import httpContext from 'express-http-context';
import { Parser } from 'node-sql-parser';

const parserExceptionRegex = [
    /\s+CROSS\s+JOIN/gmi,
    /{|\}/gmi,
    /GROUP_CONCAT/gmi,
];

export const postConvertQb = async (req: Request, res: Response, next: NextFunction) => {
    const logger: SessionLoggerService = httpContext.get('logger');
    const session: SessionManager = res.locals.session;
    const athena = session.getAthena();
    let athenaSql = '';
    try {
        let oldSql = req.body.sql;
        if (!parserExceptionRegex.some((expr): boolean => !!expr.exec(oldSql))) {
            const parser = new Parser();
            const ast = parser.astify(oldSql, {database: 'mysql'}); // mysql sql grammar parsed by default
            oldSql = parser.sqlify(ast, {database: 'db2'});
        }
        else {
           console.log('Not applying node-sql-parser transformation');
        }

        const TIMEZONE_AND_DATE_FORMAT_REGEX = /(DATE_FORMAT\(CONVERT_TZ\()(\w+\.?\w+), ('\w+'), '(\w+\/?\w+)'\), '(%\w\/%\w\/%\w ?%?\w?:?%?\w?)'\)/gm;
        const TIMEZONE_AND_DATE_FORMAT_SUBSTR = `DATE_FORMAT($2 AT TIME ZONE '$4', '$5') `;
        oldSql = oldSql.replace(TIMEZONE_AND_DATE_FORMAT_REGEX, TIMEZONE_AND_DATE_FORMAT_SUBSTR);

        const CONVERT_TIMEZONE_REGEX = /(CONVERT_TZ\()(\w+\.?\w+), ('\w+'), '(\w+\/?\w+)'\)/gm;
        const CONVERT_TIMEZONE_SUBSTR = `AT_TIMEZONE($2, '$4')`;
        oldSql = oldSql.replace(CONVERT_TIMEZONE_REGEX, CONVERT_TIMEZONE_SUBSTR);

        const DATESUB_OF_CURRENT_DATE_REGEX  = /DATE_SUB\(CURDATE\(\), INTERVAL (\d+) (\w+)\)/gm;
        const DATESUB_OF_CURRENT_DATE_SUBSTR = `current_timestamp - interval '$1' $2`;
        oldSql = oldSql.replace(DATESUB_OF_CURRENT_DATE_REGEX, DATESUB_OF_CURRENT_DATE_SUBSTR);

        const CURRENT_DATE_REGEX = /curdate\(\)|CURRENT_DATE\(\)/gmi;
        const CURRENT_DATE_SUBSTR = `current_date`;
        oldSql = oldSql.replace(CURRENT_DATE_REGEX, CURRENT_DATE_SUBSTR);

        const DATE_DIFF_REGEX = /TIMESTAMPDIFF\((\w+), (\w?\.?\w+), (\w?\.?\w+)\)/gmi;
        const DATE_DIFF_SUBSTR = `date_diff('$1', $2, $3)`;
        oldSql = oldSql.replace(DATE_DIFF_REGEX, DATE_DIFF_SUBSTR);

        const BETWEEN_TIMESTAMP_REGEX = /(\w+?\.?\w+) (BETWEEN) '(\d{4}-\d{2}-\d{2}\s?\d{2}?:\d{2}?:\d{2}?)' AND '(\d{4}-\d{2}-\d{2}\s?\d{2}?:\d{2}?:\d{2}?)'/gm;
        const BETWEEN_TIMESTAMP_SUBSTR = `$1 BETWEEN TIMESTAMP '$3' AND '$4'`;
        oldSql = oldSql.replace(BETWEEN_TIMESTAMP_REGEX, BETWEEN_TIMESTAMP_SUBSTR);

        const LEADING_REGEX = /LEADING '\/' FROM (\w+?\.?userid)/gm;
        const LEADING_SUBSTR = `SUBSTR($1, 2)`;
        oldSql = oldSql.replace(LEADING_REGEX, LEADING_SUBSTR);

        const GROUP_CONCAT_REGEX = /GROUP_CONCAT\(/gm;
        const GROUP_CONCAT_SUBSTR = 'ARRAY_JOIN(ARRAY_AGG(';
        oldSql = oldSql.replace(GROUP_CONCAT_REGEX, GROUP_CONCAT_SUBSTR);
        const SEPARATOR_GROUP_CONCAT_REGEX  = / SEPARATOR /gm;
        const SEPARATOR_GROUP_CONCAT_SUBSTR = `),`;
        oldSql = oldSql.replace(SEPARATOR_GROUP_CONCAT_REGEX, SEPARATOR_GROUP_CONCAT_SUBSTR);

        const LANG_REGEX = /(\w+?\.?lang_code)\s?=\"(\w+)\"/gm;
        const LANG_SUBSTR = `$1 = '$2'`;
        oldSql = oldSql.replace(LANG_REGEX, LANG_SUBSTR);

        const DATE_SUB_REGEX = /DATE_SUB\s*\(([^,]*),\s*INTERVAL\s+(\d+)\s+(\w+)\)/gmi;
        const DATE_SUB_SUBSTR = 'DATE_ADD(\'$3\', -$2, $1)';
        oldSql = oldSql.replace(DATE_SUB_REGEX, DATE_SUB_SUBSTR);

        const DATE_ADD_REGEX = /DATE_ADD\s*\(([^,]*),\s*INTERVAL\s+(\d+)\s+(\w+)\)/gmi;
        const DATE_ADD_SUBSTR = 'DATE_ADD(\'$3\', $2, $1)';
        oldSql = oldSql.replace(DATE_ADD_REGEX, DATE_ADD_SUBSTR);

        const TIMESTAMPDIFF_REGEX = /TIMESTAMPDIFF\s*\((\w+),\s*(\w*\.?\w+)\s*,\s*(\w*\.?\w+)\)/gmi;
        const TIMESTAMPDIFF_SUBSTR = 'DATE_DIFF(\'$1\', $2, $3)';
        oldSql = oldSql.replace(TIMESTAMPDIFF_REGEX, TIMESTAMPDIFF_SUBSTR);

        const INTERVAL_REGEX = /INTERVAL\s+(\d+)/gmi;
        const INTERVAL_SUBSTR = `INTERVAL '$1'`;
        oldSql = oldSql.replace(INTERVAL_REGEX, INTERVAL_SUBSTR);

        const COUNT_STAR_REGEX = /count\(\*\)/gmi;
        const COUNT_STAR_SUBSTR = `COUNT(0)`;
        oldSql = oldSql.replace(COUNT_STAR_REGEX, COUNT_STAR_SUBSTR);

        const STR_TO_DATE_REGEX = /STR_TO_DATE\s*\(([^\)]*)\)/gmi;
        const STR_TO_DATE_SUBSTR = `DATE_PARSE($1)`;
        oldSql = oldSql.replace(STR_TO_DATE_REGEX, STR_TO_DATE_SUBSTR);

        const TIME_FORMAT_REGEX = /TIME_FORMAT\s*\(/gmi;
        const TIME_FORMAT_SUBSTR = `DATE_FORMAT(`;
        oldSql = oldSql.replace(TIME_FORMAT_REGEX, TIME_FORMAT_SUBSTR);

        const IF_NULL_REGEX = /IFNULL\s*\(/gmi;
        const IF_NULL_SUBSTR = `COALESCE(`;
        oldSql = oldSql.replace(IF_NULL_REGEX, IF_NULL_SUBSTR);

        oldSql = oldSql.replace(/(\r\n|\n|\r|\t)/gm, ' ');

        const LIKE_THEN_REGEX  = /(\w+\s+)[as]*(?<![like|then] ) ?\s*('(?!"))(.*?)(')/gmi;
        const LIKE_THEN_SUBSTR = `$1 as "$3"`;
        oldSql = oldSql.replace(LIKE_THEN_REGEX, LIKE_THEN_SUBSTR);
        const LIKE_THEN_AS_REGEX = /\bAS\b\s+\bas\b /gi;
        const LIKE_THEN_AS_SUBSTR = ` AS `;
        // Last replacement
        athenaSql = oldSql.replace(LIKE_THEN_AS_REGEX, LIKE_THEN_AS_SUBSTR);

        athenaSql = oldSql ;
        console.log(athenaSql);

        await athena.connection.query(athenaSql);

        res.type('application/json');
        res.status(200);
        res.json({sql: athenaSql, success: true});
    } catch (err: any) {
        let errorMessage = err.message;
        for (let numberOfTries = 1; numberOfTries <= 10; numberOfTries++) {
            const result = await handleException(errorMessage, athenaSql, res);
            if (result.matched) {
                try {
                    athenaSql = result.newQuery;
                    await athena.connection.query(athenaSql);
                    res.type('application/json');
                    res.status(200);
                    res.json({sql: athenaSql, success: true});
                    return;
                } catch (e: any) {
                    logger.errorWithStack(`Try number ${numberOfTries + 1} failed. Reason: `, e);
                    errorMessage = e.message;
                }
            }
            else {
                break;
            }
        }
        logger.errorWithStack('Error on query builder report conversion', err);
        res.type('application/json');
        res.status(500);
        res.json({success: false, error: errorMessage, sql: athenaSql});
        return;
    }
};




const handleException = async (errorMessage: string, athenaSql: string, res: Response): Promise<{newQuery: string, matched: boolean}> => {

    /**
     * Try to fix type mismatch in comparisons applying CAST
     */
    const errorTypeRegex = /\d+:(\d+): \'(.*)\' cannot be applied to (.*),/gm;
    let split = -1;
    let symbol = '';
    let typeLeft = '';
    let newQuery = '';
    let m = errorTypeRegex.exec(errorMessage);
    if (m) {
        symbol = m[2] ?? '';
        if (symbol === '<>') {
            symbol = '!=';
        }
        split = parseInt(m[1]) - symbol.length;
        typeLeft = m[3] ?? '';
        const query = athenaSql.slice(split);
        // Regex definition --> Match attribute name | match varchar | match date format
        const regex = new RegExp(`${symbol}\\s*((\\w*\\.?\\w+)|(\\'\\w+\\')|(\\'\\d{4}-\\d{2}-\\d{2}\\'))`, 'i');
        const subst = `${symbol} CAST($1 as ${typeLeft})`;
        const result = query.replace(regex, subst);
        newQuery = athenaSql.replace(query, result);
        return {newQuery, matched: true};
    }

    /**
     * Try to fix attribute missing in group by clause applying ARBITRARY operator
     */
    const errorAggregateRegex = /.*\d+:(\d+): \'(.*)\' must be an aggregate expression or appear in GROUP BY .*/gm;
    m = errorAggregateRegex.exec(errorMessage);
    if (m) {
        symbol = m[2] ?? '';
        split = parseInt(m[1]) - symbol.length;
        const query = athenaSql.slice(split);
        const regexAlias = new RegExp(`(${symbol})(\\s*as\\s\\w+)`, 'im');
        const regex = new RegExp(`(${symbol})`, 'im');

        let withAlias;
        let subst;

        // Check if selected attribute has an alias
        if (withAlias = regexAlias.exec(query)) {
            console.log('withAlias ------>', withAlias);
            subst = `ARBITRARY($1)`;
        }
        else {
            // add an alias otherwise, athena would use positional names for columns (like col_1, col_2 etc.)
            subst = `ARBITRARY($1) as $1`;
        }

        // The substituted value will be contained in the result variable
        const result = query.replace(regex, subst);

        let newQuery = '';
        newQuery = athenaSql.replace(query, result);
        return {newQuery, matched: true};
    }
    return {newQuery: undefined, matched: false};
};
