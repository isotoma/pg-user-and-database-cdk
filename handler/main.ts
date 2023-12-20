// A handler for the custom resource. Is called with the following properties from Cloudformation:
// - dbClusterHostname: hostname of the RDS cluster
// - dbClusterPort: port of the RDS cluster
// - dbSecretArn: ARN of the RDS cluster secret
// - userSecretArn: ARN of the user secret
// - username: username of the user
// - databaseName: name of the database
//
// With those, should connect to the RDS cluster and create the user
// and database. The user should be granted CREATEDB and LOGIN access,
// and that user should then create the database.

import * as pg from 'pg';
import * as secretsmanager from '@aws-sdk/client-secrets-manager';
import { z } from 'zod';

interface Response {
    PhysicalResourceId: string;
}

// Relevant error codes taken from
// https://www.postgresql.org/docs/current/errcodes-appendix.html
enum PostgresErrorCodes {
    DUPLICATE_DATABASE = '42P04',
    DUPLICATE_OBJECT = '42710',
    INSUFFICIENT_PRIVILEGE = '42501',
    AUTHENTICATION_FAILED = '28P01',
}

interface PostgresError extends Error {
    code: string;
}

const isPostgresError = (e: unknown): e is PostgresError => {
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

// Class that lazily creates a Postgres client from a secret ARN,
// dbClusterHostname, dbClusterPort and databaseName, and caches the client
interface LazyPostgresClientFromSecretsManagerProps {
    dbSecretArn: string;
    dbClusterHostname: string;
    dbClusterPort: number;
    databaseName: string;
}

class LazyPostgresClientFromSecretsManager {
    private props: LazyPostgresClientFromSecretsManagerProps;
    private client?: pg.Client;
    private credentials?: DbCredentials;

    constructor(props: LazyPostgresClientFromSecretsManagerProps) {
        this.props = props;
    }

    // Getters for the props
    get dbSecretArn(): string {
        return this.props.dbSecretArn;
    }

    get dbClusterHostname(): string {
        return this.props.dbClusterHostname;
    }

    get dbClusterPort(): number {
        return this.props.dbClusterPort;
    }

    get databaseName(): string {
        return this.props.databaseName;
    }

    async getCredentials(): Promise<DbCredentials> {
        if (this.credentials) {
            return this.credentials;
        }

        const secretsManagerClient = new secretsmanager.SecretsManagerClient({
            region: process.env['AWS_REGION'],
        });

        const dbSecret = await secretsManagerClient.send(
            new secretsmanager.GetSecretValueCommand({
                SecretId: this.props.dbSecretArn,
            }),
        );

        const dbSecretJson: unknown = JSON.parse(dbSecret.SecretString ?? '{}');

        const dbSecretJsonSchema = z.object({
            username: z.string(),
            password: z.string(),
        });

        const validatedDbSecretJson = dbSecretJsonSchema.parse(dbSecretJson);

        this.credentials = {
            username: validatedDbSecretJson.username,
            password: validatedDbSecretJson.password,
        };

        return this.credentials;
    }

    async getClient(): Promise<pg.Client> {
        if (this.client) {
            return this.client;
        }

        if (!this.credentials) {
            this.credentials = await this.getCredentials();
        }

        this.client = new pg.Client({
            host: this.props.dbClusterHostname,
            port: this.props.dbClusterPort,
            user: this.credentials.username,
            password: this.credentials.password,
            ssl: {
                rejectUnauthorized: false,
            },
            database: this.props.databaseName,
        });

        await this.client.connect();
        return this.client;
    }

    async end(): Promise<void> {
        if (this.client) {
            await this.client.end();
        }
    }
}

// Log function that takes a message and optionally additional data, writes logs as JSON
const log = (message: string, data?: unknown): void => {
    console.log(JSON.stringify({ message, data }));
};

const customResourcePropertiesSchema = z.object({
    dbClusterHostname: z.string(),
    dbClusterPort: z.string().regex(/^\d+$/).transform(Number),
    dbSecretArn: z.string(),
    userSecretArn: z.string(),
    databaseName: z.string(),
    onDelete: z.enum(['Delete', 'Retain']),
    onCreateIfExists: z.enum(['Fail', 'Adopt', 'DeleteAndRecreate']),
    onUpdateIfUserDoesNotExist: z.enum(['Ignore', 'Create']),
    onUpdateIfDatabaseDoesNotExist: z.enum(['Ignore', 'Create']),
    onUpdateSetUserPassword: z.enum(['Always', 'Never']),
    onUpdateSetUserPermissions: z.enum(['Always', 'Never']),
    onUpdateSetDatabaseOwnership: z.enum(['Always', 'Never']),
});

type CustomResourceProperties = z.infer<typeof customResourcePropertiesSchema>;

interface CreateEvent {
    RequestType: 'Create';
    ResourceProperties: CustomResourceProperties;
}

interface UpdateEvent {
    RequestType: 'Update';
    PhysicalResourceId: string;
    ResourceProperties: CustomResourceProperties;
}

interface DeleteEvent {
    RequestType: 'Delete';
    PhysicalResourceId: string;
    ResourceProperties: CustomResourceProperties;
}

type Event = CreateEvent | UpdateEvent | DeleteEvent;

interface DbCredentials {
    username: string;
    password: string;
}

const decodeEvent = (event: unknown): Event => {
    const eventSchema = z.object({
        RequestType: z.enum(['Create', 'Update', 'Delete']),
    });

    const validatedEvent = eventSchema.parse(event);

    if (validatedEvent.RequestType === 'Create') {
        const createEventSchema = eventSchema.extend({
            ResourceProperties: customResourcePropertiesSchema,
        });

        return {
            ...createEventSchema.parse(event),
            RequestType: 'Create',
        };
    } else if (validatedEvent.RequestType === 'Update') {
        const updateEventSchema = eventSchema.extend({
            PhysicalResourceId: z.string(),
            ResourceProperties: customResourcePropertiesSchema,
        });
        return {
            ...updateEventSchema.parse(event),
            RequestType: 'Update',
        };
    } else if (validatedEvent.RequestType === 'Delete') {
        const deleteEventSchema = eventSchema.extend({
            PhysicalResourceId: z.string(),
            ResourceProperties: customResourcePropertiesSchema,
        });
        return {
            ...deleteEventSchema.parse(event),
            RequestType: 'Delete',
        };
    }
    throw new Error('Invalid event type');
};

const handleCreate = async (event: CreateEvent): Promise<Response> => {
    log('Handling create');
    const adminClientManager = new LazyPostgresClientFromSecretsManager({
        dbSecretArn: event.ResourceProperties.dbSecretArn,
        dbClusterHostname: event.ResourceProperties.dbClusterHostname,
        dbClusterPort: event.ResourceProperties.dbClusterPort,
        databaseName: 'postgres',
    });
    const userClientManager = new LazyPostgresClientFromSecretsManager({
        dbSecretArn: event.ResourceProperties.userSecretArn,
        dbClusterHostname: event.ResourceProperties.dbClusterHostname,
        dbClusterPort: event.ResourceProperties.dbClusterPort,
        databaseName: event.ResourceProperties.databaseName,
    });

    const userCredentials = await userClientManager.getCredentials();
    const adminCredentials = await adminClientManager.getCredentials();

    if (userCredentials.username === adminCredentials.username) {
        throw new Error('Cannot create user with same name as the admin user');
    }

    const adminClient = await adminClientManager.getClient();

    log('Creating database', {
        databaseName: event.ResourceProperties.databaseName,
        username: userCredentials.username,
        onCreateIfExists: event.ResourceProperties.onCreateIfExists,
    });

    const createUserQuery = `CREATE USER ${userCredentials.username} WITH PASSWORD '${userCredentials.password}' CREATEDB LOGIN;`;

    try {
        await adminClient.query(createUserQuery);
    } catch (e) {
        if (!isPostgresError(e)) {
            throw e;
        }

        if (event.ResourceProperties.onCreateIfExists === 'Adopt' && e.code === PostgresErrorCodes.DUPLICATE_OBJECT) {
            // User already exists, so we'll just adopt it. Set the password to the new value and grant CREATEDB and LOGIN
            await adminClient.query(`ALTER USER ${userCredentials.username} WITH PASSWORD '${userCredentials.password}';`);
            await adminClient.query(`ALTER USER ${userCredentials.username} WITH CREATEDB LOGIN;`);
        } else if (event.ResourceProperties.onCreateIfExists === 'DeleteAndRecreate' && e.code === PostgresErrorCodes.DUPLICATE_OBJECT) {
            await adminClient.query(`DROP USER ${userCredentials.username};`);
            await adminClient.query(createUserQuery);
        } else {
            throw e;
        }
    }

    const createDatabaseQuery = `CREATE DATABASE ${event.ResourceProperties.databaseName};`;

    const userClient = await userClientManager.getClient();
    try {
        await userClient.query(createDatabaseQuery);
    } catch (e) {
        if (!isPostgresError(e)) {
            throw e;
        }
        log('Error creating database', {
            // Pass the error such that is can be converted to JSON
            error: String(e),
            errorCode: String(e.code),
        });
        if (event.ResourceProperties.onCreateIfExists === 'Adopt' && e.code === PostgresErrorCodes.DUPLICATE_DATABASE) {
            // Database already exists, so we'll just adopt it
            log('Database already exists, adopting');
            await adminClient.query(`ALTER DATABASE ${event.ResourceProperties.databaseName} OWNER TO ${userCredentials.username};`);
        } else if (event.ResourceProperties.onCreateIfExists === 'DeleteAndRecreate') {
            if (e.code === PostgresErrorCodes.DUPLICATE_DATABASE) {
                log('Database already exists, deleting and recreating');
                await adminClient.query(`DROP DATABASE ${event.ResourceProperties.databaseName};`);
                await userClient.query(createDatabaseQuery);
            }
        } else {
            throw e;
        }
    }

    await adminClient.end();
    await userClient.end();

    return {
        PhysicalResourceId: [event.ResourceProperties.dbClusterHostname, event.ResourceProperties.databaseName, userCredentials.username].join('/'),
    };
};

const handleUpdate = async (event: UpdateEvent): Promise<Response> => {
    log('Handling update');
    const userClient = new LazyPostgresClientFromSecretsManager({
        dbSecretArn: event.ResourceProperties.userSecretArn,
        dbClusterHostname: event.ResourceProperties.dbClusterHostname,
        dbClusterPort: event.ResourceProperties.dbClusterPort,
        databaseName: event.ResourceProperties.databaseName,
    });

    const userCredentials = await userClient.getCredentials();

    if (event.PhysicalResourceId !== [event.ResourceProperties.dbClusterHostname, event.ResourceProperties.databaseName, userCredentials.username].join('/')) {
        throw new Error(`Cannot change database name or username`);
    }

    const adminClient = new LazyPostgresClientFromSecretsManager({
        dbSecretArn: event.ResourceProperties.dbSecretArn,
        dbClusterHostname: event.ResourceProperties.dbClusterHostname,
        dbClusterPort: event.ResourceProperties.dbClusterPort,
        databaseName: 'postgres',
    });

    if (event.ResourceProperties.onUpdateIfUserDoesNotExist === 'Create') {
        log('Creating user if it does not exist', { username: userCredentials.username });
        try {
            const client = await adminClient.getClient();
            await client.query(`CREATE USER ${userCredentials.username} WITH PASSWORD '${userCredentials.password}' CREATEDB LOGIN;`);
        } catch (e) {
            if (!isPostgresError(e)) {
                throw e;
            }
            // If the user already exists, do nothing
            if (e.code !== PostgresErrorCodes.DUPLICATE_OBJECT) {
                throw e;
            } else {
                log('User already exists, doing nothing');
            }
        }
    } else {
        log('Not creating user if it does not exist', { username: userCredentials.username });
    }

    if (event.ResourceProperties.onUpdateSetUserPassword === 'Always') {
        log('Setting user password', { username: userCredentials.username });
        const client = await adminClient.getClient();
        await client.query(`ALTER USER ${userCredentials.username} WITH PASSWORD '${userCredentials.password}';`);
    } else {
        log('Not setting user password', { username: userCredentials.username });
    }

    if (event.ResourceProperties.onUpdateSetUserPermissions === 'Always') {
        log('Setting user permissions', { username: userCredentials.username });
        const client = await adminClient.getClient();
        await client.query(`ALTER USER ${userCredentials.username} WITH CREATEDB LOGIN;`);
    } else {
        log('Not setting user permissions', { username: userCredentials.username });
    }

    if (event.ResourceProperties.onUpdateIfDatabaseDoesNotExist === 'Create') {
        log('Creating database if it does not exist', { databaseName: event.ResourceProperties.databaseName });
        try {
            const client = await userClient.getClient();
            await client.query(`CREATE DATABASE ${event.ResourceProperties.databaseName};`);
        } catch (e) {
            if (!isPostgresError(e)) {
                throw e;
            }
            // If the database already exists, do nothing
            if (e.code !== PostgresErrorCodes.DUPLICATE_DATABASE) {
                throw e;
            } else {
                log('Database already exists, doing nothing');
            }
        }
    } else {
        log('Not creating database if it does not exist', { databaseName: event.ResourceProperties.databaseName });
    }

    if (event.ResourceProperties.onUpdateSetDatabaseOwnership === 'Always') {
        log('Setting database ownership', { databaseName: event.ResourceProperties.databaseName });
        const client = await adminClient.getClient();
        await client.query(`ALTER DATABASE ${event.ResourceProperties.databaseName} OWNER TO ${userCredentials.username};`);
    } else {
        log('Not setting database ownership', { databaseName: event.ResourceProperties.databaseName });
    }

    await adminClient.end();
    await userClient.end();

    return {
        PhysicalResourceId: event.PhysicalResourceId,
    };
};

const handleDelete = async (event: DeleteEvent): Promise<Response> => {
    log('Handling delete');
    if (event.ResourceProperties.onDelete === 'Retain') {
        log('Retaining user and database');
        return {
            PhysicalResourceId: event.PhysicalResourceId,
        };
    }

    const adminClientManager = new LazyPostgresClientFromSecretsManager({
        dbSecretArn: event.ResourceProperties.dbSecretArn,
        dbClusterHostname: event.ResourceProperties.dbClusterHostname,
        dbClusterPort: event.ResourceProperties.dbClusterPort,
        databaseName: 'postgres',
    });
    const userClientManager = new LazyPostgresClientFromSecretsManager({
        dbSecretArn: event.ResourceProperties.userSecretArn,
        dbClusterHostname: event.ResourceProperties.dbClusterHostname,
        dbClusterPort: event.ResourceProperties.dbClusterPort,
        databaseName: event.ResourceProperties.databaseName,
    });

    const userCredentials = await userClientManager.getCredentials();
    const adminCredentials = await adminClientManager.getCredentials();

    if (userCredentials.username === adminCredentials.username) {
        throw new Error('Cannot create user with same name as the admin user');
    }

    const adminClient = await adminClientManager.getClient();

    log('Dropping database if exists', { databaseName: event.ResourceProperties.databaseName });
    await adminClient.query(`DROP DATABASE IF EXISTS ${event.ResourceProperties.databaseName};`);

    log('Dropping user if exists', { databaseName: event.ResourceProperties.databaseName });
    await adminClient.query(`DROP USER IF EXISTS ${userCredentials.username};`);

    await adminClient.end();

    return {
        PhysicalResourceId: event.PhysicalResourceId,
    };
};

export const handler = async (event: unknown): Promise<Response> => {
    const validatedEvent = decodeEvent(event);

    if (validatedEvent.RequestType === 'Create') {
        return await handleCreate(validatedEvent);
    } else if (validatedEvent.RequestType === 'Update') {
        return await handleUpdate(validatedEvent);
    } else if (validatedEvent.RequestType === 'Delete') {
        return await handleDelete(validatedEvent);
    }
    throw new Error('Invalid event type');
};
