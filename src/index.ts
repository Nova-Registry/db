import fs = require("node:fs");
import path = require("node:path");
import dotenv = require("dotenv");
import util = require("node:util");
import pg = require("pg");
import type { StringMap } from "@nova-registry/types";

function getPGString(): string {
    dotenv.config();

    const connectionStringFormat = "postgresql://%s:%s@%s/%s?sslmode=%s&channel_binding=%s";

    const string = util.format(
        connectionStringFormat,
        ...[
            process.env.PGUSER,
            process.env.PGPASSWORD,
            process.env.PGHOST,
            process.env.PGDATABASE,
            process.env.PGSSLMODE,
            process.env.PGCHANNELBINDING
        ]
    );

    return string;
}

/**
 * The Database Namespace. Provides abstraction to interacting with the postgres database.
 */
namespace db {
    // The connection pool for the PG DB.
    const ServerPool: pg.Pool = new pg.Pool({
        connectionString: getPGString()
    });

    export async function selectFrom(table: string, parameters: "*" | string[]): Promise<any[]>;
    export async function selectFrom(table: { schema: string, table: string }, parameters: "*" | string[]): Promise<any[]>;
    export async function selectFrom(valA: unknown, parameters: "*" | string[]): Promise<any[]> {
        if (typeof valA == "string") {
            // public schema

            const queryString = "SELECT " + (parameters == "*" ? parameters : parameters.join(", ")) + " from \"public\".\"" + valA + "\"";

            const q = await ServerPool.query(queryString);

            return q.rows;
        } else if (typeof valA == "object") {
            const { table, schema } = (valA as any);

            const queryString = "SELECT " + (parameters == '*' ? parameters : parameters.join(", ")) + " from \"" + schema + "\".\"" + table + "\"";

            const q = await ServerPool.query(queryString);

            return q.rows;
        }
        
        return [];
    }

    type KeyInObject<T extends Array<any>> = {
        [P in keyof T]: any;
    }

    export async function insertInto<T extends StringMap<any[]>>(table: string, additions: T): Promise<void>;
    export async function insertInto<T extends StringMap<any[]>>(table: { schema: string, table: string }, additions: T): Promise<void>;
    export async function insertInto<T extends StringMap<any[]>>(valA: unknown, additions: T): Promise<void> {
        if (typeof valA == "string")
            return await insertInto({ schema: "public", table: valA }, additions);
        else {
            const { schema, table } = (valA as { schema: string, table: string });

            const paramKeys = new Array<string>();
            const additionValues = new Array<Array<any>>();

            for (const [key, value] of Object.entries(additions)) {
                paramKeys.push(key);

                for (let i = 0; i < value.length; i++)
                {
                    if (!additionValues[i])
                        additionValues[i] = new Array<any>();

                    additionValues[i]?.push(value[i]);
                }
            }

            const queryStringA = "INSERT INTO \"" + schema + "\".\"" + table + "\" (" + paramKeys.join(", ") + ")";
            const queryStringB = "VALUES ";

            const valuesEnd: string[] = [];

            for (let i = 0; i < additionValues.length; i++)
            {
                const arr = additionValues[i];

                if (!arr)
                    continue;

                const sArray = new Array<string>();

                for (const val of arr) {
                    if (typeof val == "string")
                        sArray.push(`'${val}'`);
                    else
                        sArray.push(String(val));
                }

                const string = `(${sArray.join(", ")})`;

                valuesEnd.push(string);
            }

            const queryString = [
                queryStringA,
                queryStringB + (valuesEnd.length == 1 ? valuesEnd[0] : ""),
                ...(valuesEnd.length > 1 ? valuesEnd : [])
            ].join("\n");

            await ServerPool.query(queryString);
        }
    }
}

export = db;