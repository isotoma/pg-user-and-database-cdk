// Pure SQL helpers, kept free of any database or AWS dependency so they can be
// tested directly. Anything needing a connection lives in ./postgres.

// Relevant error codes taken from
// https://www.postgresql.org/docs/current/errcodes-appendix.html
export enum PostgresErrorCodes {
    DUPLICATE_DATABASE = '42P04',
    DUPLICATE_OBJECT = '42710',
    INSUFFICIENT_PRIVILEGE = '42501',
    AUTHENTICATION_FAILED = '28P01',
}

export interface PostgresError extends Error {
    code: string;
}

export const isPostgresError = (e: unknown): e is PostgresError => {
    if (typeof e !== 'object' || e === null) {
        return false;
    }

    // Check extends Error
    if (!('message' in e && 'name' in e)) {
        return false;
    }

    // Check has code
    if (!('code' in e)) {
        return false;
    }

    return typeof e.code === 'string';
};

/**
 * Postgres has no parameterisation for identifiers, so any name reaching a
 * statement has to be quoted. These names come from CDK config rather than
 * end-user input, but an unquoted identifier still breaks on mixed case or a
 * reserved word, and quoting keeps the statements honest either way.
 */
export const quoteIdentifier = (name: string): string => `"${name.replace(/"/g, '""')}"`;

export const quoteLiteral = (value: string): string => `'${value.replace(/'/g, "''")}'`;
