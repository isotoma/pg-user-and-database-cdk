import { quoteIdentifier, quoteLiteral, isPostgresError, PostgresErrorCodes } from '../shared/sql';

describe('quoteIdentifier', () => {
    test('wraps a plain name in double quotes', () => {
        expect(quoteIdentifier('core_user')).toBe('"core_user"');
    });

    test('preserves case, which is the point of quoting', () => {
        expect(quoteIdentifier('CoreUser')).toBe('"CoreUser"');
    });

    test('quotes a name that would otherwise be a reserved word', () => {
        expect(quoteIdentifier('user')).toBe('"user"');
    });

    test('escapes an embedded double quote by doubling it', () => {
        expect(quoteIdentifier('we"ird')).toBe('"we""ird"');
    });

    test('leaves no way to break out of the identifier', () => {
        expect(quoteIdentifier('a"; DROP TABLE x; --')).toBe('"a""; DROP TABLE x; --"');
    });
});

describe('quoteLiteral', () => {
    test('wraps a value in single quotes', () => {
        expect(quoteLiteral('hunter2')).toBe("'hunter2'");
    });

    test('escapes an embedded single quote by doubling it', () => {
        expect(quoteLiteral("pa'ss")).toBe("'pa''ss'");
    });

    test('leaves no way to break out of the literal', () => {
        // Generated passwords exclude quotes, but a caller-supplied secret need not.
        expect(quoteLiteral("x'; DROP ROLE y; --")).toBe("'x''; DROP ROLE y; --'");
    });
});

describe('isPostgresError', () => {
    test('accepts an error carrying a string code', () => {
        const error = Object.assign(new Error('role already exists'), { code: PostgresErrorCodes.DUPLICATE_OBJECT });
        expect(isPostgresError(error)).toBe(true);
    });

    test('rejects an error with no code', () => {
        expect(isPostgresError(new Error('boom'))).toBe(false);
    });

    test('rejects an error whose code is not a string', () => {
        expect(isPostgresError(Object.assign(new Error('boom'), { code: 42710 }))).toBe(false);
    });

    test('rejects a bare object carrying a code but no error shape', () => {
        expect(isPostgresError({ code: '42710' })).toBe(false);
    });

    test('rejects non-objects', () => {
        expect(isPostgresError(null)).toBe(false);
        expect(isPostgresError('42710')).toBe(false);
        expect(isPostgresError(undefined)).toBe(false);
    });
});

describe('PostgresErrorCodes', () => {
    // The handler adopts an existing role on this code specifically, so a
    // change here would silently turn a normal update into a failure.
    test('DUPLICATE_OBJECT is the SQLSTATE Postgres raises for an existing role', () => {
        expect(PostgresErrorCodes.DUPLICATE_OBJECT).toBe('42710');
    });
});
