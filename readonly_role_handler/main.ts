// A handler for the read-only role custom resource. Is called with the
// following properties from Cloudformation:
// - dbClusterHostname: hostname of the RDS cluster
// - dbClusterPort: port of the RDS cluster
// - adminSecretArn: ARN of the RDS cluster admin secret
// - ownerSecretArn: ARN of the secret for the role owning the tables
// - roleSecretArn: ARN of the secret holding the new role's credentials
// - databaseName: database the role may connect to
// - schemaName: schema holding the tables
// - tableNames: tables the role may SELECT from
//
// Two connections are needed rather than one, and the split is not where you
// might expect. Creating a role needs CREATEROLE, which the admin has and the
// owner does not. Every GRANT has to come from the owner: on Aurora the master
// user is not a true superuser, and PostgresUserAndDatabase hands database
// ownership to the application user, so the admin can grant neither CONNECT on
// the database nor SELECT on its tables. Verified against Postgres 16: an admin
// with CREATEDB and CREATEROLE but no superuser gets "permission denied for
// table" on the SELECT grant, and a bare "WARNING: no privileges were granted"
// on the CONNECT grant, which fails silently.

import { z } from 'zod';

import { LazyPostgresClientFromSecretsManager, log } from '../shared/postgres';
import { PostgresErrorCodes, isPostgresError, quoteIdentifier, quoteLiteral } from '../shared/sql';

interface Response {
    PhysicalResourceId: string;
}

const customResourcePropertiesSchema = z.object({
    dbClusterHostname: z.string(),
    dbClusterPort: z.string().regex(/^\d+$/).transform(Number),
    adminSecretArn: z.string(),
    ownerSecretArn: z.string(),
    roleSecretArn: z.string(),
    databaseName: z.string(),
    schemaName: z.string(),
    tableNames: z.array(z.string()).min(1),
    onCreateIfExists: z.enum(['Fail', 'Adopt']),
    onDelete: z.enum(['Drop', 'Retain']),
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

const adminClientManagerFor = (properties: CustomResourceProperties): LazyPostgresClientFromSecretsManager =>
    new LazyPostgresClientFromSecretsManager({
        dbSecretArn: properties.adminSecretArn,
        dbClusterHostname: properties.dbClusterHostname,
        dbClusterPort: properties.dbClusterPort,
        // Creating, altering and dropping a role are cluster-level, so which
        // database we are attached to does not matter, only that one exists.
        // Every GRANT and REVOKE runs on the owner connection instead.
        databaseName: 'postgres',
    });

const ownerClientManagerFor = (properties: CustomResourceProperties): LazyPostgresClientFromSecretsManager =>
    new LazyPostgresClientFromSecretsManager({
        dbSecretArn: properties.ownerSecretArn,
        dbClusterHostname: properties.dbClusterHostname,
        dbClusterPort: properties.dbClusterPort,
        databaseName: properties.databaseName,
    });

const roleClientManagerFor = (properties: CustomResourceProperties): LazyPostgresClientFromSecretsManager =>
    new LazyPostgresClientFromSecretsManager({
        dbSecretArn: properties.roleSecretArn,
        dbClusterHostname: properties.dbClusterHostname,
        dbClusterPort: properties.dbClusterPort,
        databaseName: properties.databaseName,
    });

/**
 * Whether an existing role of the same name may be taken over.
 *
 * On an update the resource already owns the role, so adopting it is the only
 * sensible behaviour. On a create there is nothing proving the role belongs to
 * this resource, and adopting would reset an unrelated role's password and hand
 * it SELECT on the tables, so that needs an explicit opt-in.
 */
type AdoptPolicy = 'Fail' | 'Adopt';

const applyGrants = async (properties: CustomResourceProperties, adoptPolicy: AdoptPolicy): Promise<void> => {
    const adminClientManager = adminClientManagerFor(properties);
    const ownerClientManager = ownerClientManagerFor(properties);
    const roleClientManager = roleClientManagerFor(properties);

    const roleCredentials = await roleClientManager.getCredentials();
    const adminCredentials = await adminClientManager.getCredentials();
    const ownerCredentials = await ownerClientManager.getCredentials();

    if (roleCredentials.username === adminCredentials.username) {
        throw new Error('Cannot create a read-only role with the same name as the admin user');
    }
    if (roleCredentials.username === ownerCredentials.username) {
        throw new Error('Cannot create a read-only role with the same name as the owner user');
    }

    const quotedRole = quoteIdentifier(roleCredentials.username);

    try {
        const adminClient = await adminClientManager.getClient();
        try {
            log('Creating role', { username: roleCredentials.username });
            await adminClient.query(`CREATE ROLE ${quotedRole} WITH LOGIN PASSWORD ${quoteLiteral(roleCredentials.password)};`);
        } catch (e: unknown) {
            if (!isPostgresError(e) || e.code !== PostgresErrorCodes.DUPLICATE_OBJECT) {
                throw e;
            }
            if (adoptPolicy === 'Fail') {
                throw new Error(
                    `Role "${roleCredentials.username}" already exists. It may belong to something else, and adopting it would reset its password and grant it SELECT on ${properties.tableNames.join(
                        ', ',
                    )}. Pass onCreateIfExists: 'Adopt' to take it over deliberately.`,
                );
            }
            log('Role already exists, adopting and resetting its password', { username: roleCredentials.username });
            await adminClient.query(`ALTER ROLE ${quotedRole} WITH LOGIN PASSWORD ${quoteLiteral(roleCredentials.password)};`);
        }
    } finally {
        await adminClientManager.end();
    }

    try {
        const ownerClient = await ownerClientManager.getClient();
        log('Granting CONNECT', { databaseName: properties.databaseName });
        await ownerClient.query(`GRANT CONNECT ON DATABASE ${quoteIdentifier(properties.databaseName)} TO ${quotedRole};`);

        log('Granting USAGE on schema', { schemaName: properties.schemaName });
        await ownerClient.query(`GRANT USAGE ON SCHEMA ${quoteIdentifier(properties.schemaName)} TO ${quotedRole};`);

        for (const tableName of properties.tableNames) {
            log('Granting SELECT on table', { schemaName: properties.schemaName, tableName });
            await ownerClient.query(`GRANT SELECT ON ${quoteIdentifier(properties.schemaName)}.${quoteIdentifier(tableName)} TO ${quotedRole};`);
        }
    } finally {
        await ownerClientManager.end();
    }
};

const handleCreate = async (event: CreateEvent): Promise<Response> => {
    log('Handling create');
    // Nothing here proves an existing role of this name belongs to us, so
    // taking one over is the caller's decision, not the default.
    await applyGrants(event.ResourceProperties, event.ResourceProperties.onCreateIfExists);

    const roleCredentials = await roleClientManagerFor(event.ResourceProperties).getCredentials();
    return {
        PhysicalResourceId: roleCredentials.username,
    };
};

const handleUpdate = async (event: UpdateEvent): Promise<Response> => {
    log('Handling update');
    // The grants are all idempotent, so an update is the same work as a create.
    // Tables dropped from tableNames keep their grant: revoking them would mean
    // tracking the previous property values, and leaving a stale SELECT on a
    // table is the less surprising failure of the two.
    //
    // Adopt unconditionally here: the physical resource id already says this
    // role is ours, so an existing one is expected rather than a collision.
    //
    // That reasoning only holds while the name has not moved. The physical
    // resource id is the role name from the create, and nothing in the custom
    // resource triggers a replacement when it changes, so a renamed role would
    // be created and granted while the id still pointed at the old one: the
    // original would be orphaned, and the delete would drop the wrong role.
    const roleCredentials = await roleClientManagerFor(event.ResourceProperties).getCredentials();
    if (roleCredentials.username !== event.PhysicalResourceId) {
        throw new Error(
            `Cannot rename the role from "${event.PhysicalResourceId}" to "${roleCredentials.username}" in place, as it would orphan the original. Remove the construct, deploy, then add it back under the new name.`,
        );
    }

    await applyGrants(event.ResourceProperties, 'Adopt');

    return {
        PhysicalResourceId: event.PhysicalResourceId,
    };
};

const handleDelete = async (event: DeleteEvent): Promise<Response> => {
    log('Handling delete');
    if (event.ResourceProperties.onDelete === 'Retain') {
        log('Retaining role');
        return {
            PhysicalResourceId: event.PhysicalResourceId,
        };
    }

    const properties = event.ResourceProperties;
    const adminClientManager = adminClientManagerFor(properties);
    const ownerClientManager = ownerClientManagerFor(properties);

    // The physical resource id, not the secret. The id is what this resource
    // actually created and is immutable for its lifetime, whereas the secret is
    // a live value someone could edit out of band; trusting it would let a
    // delete revoke from and drop a role this resource never owned.
    const roleName = event.PhysicalResourceId;
    const quotedRole = quoteIdentifier(roleName);

    // Postgres refuses to drop a role that still holds a privilege anywhere, and
    // a grant can only be revoked by whoever made it, so every REVOKE goes
    // through the owner and only the DROP is left to the admin.
    try {
        const adminClient = await adminClientManager.getClient();

        // REVOKE against a role that does not exist is an error, unlike DROP
        // ROLE IF EXISTS. Without this check a rollback after a failed create
        // would fail on the first revoke and leave the stack stuck.
        const existing = await adminClient.query('SELECT 1 FROM pg_roles WHERE rolname = $1', [roleName]);
        if (existing.rowCount === null || existing.rowCount === 0) {
            log('Role does not exist, nothing to drop', { username: roleName });
            return {
                PhysicalResourceId: event.PhysicalResourceId,
            };
        }

        try {
            const ownerClient = await ownerClientManager.getClient();
            // Every table in the schema, not just the current tableNames. An
            // update that drops a table from the list deliberately leaves its
            // grant behind, so revoking only the current list would leave the
            // role holding a privilege and DROP ROLE would fail with "role
            // cannot be dropped because some objects depend on it". This only
            // reaches tables the owner can revoke on, which is the same set it
            // could grant on, and it is a no-op when the role holds nothing.
            log('Revoking table privileges across schema', { schemaName: properties.schemaName });
            await ownerClient.query(`REVOKE ALL ON ALL TABLES IN SCHEMA ${quoteIdentifier(properties.schemaName)} FROM ${quotedRole};`);

            log('Revoking schema privileges', { schemaName: properties.schemaName });
            await ownerClient.query(`REVOKE ALL ON SCHEMA ${quoteIdentifier(properties.schemaName)} FROM ${quotedRole};`);
            log('Revoking database privileges', { databaseName: properties.databaseName });
            await ownerClient.query(`REVOKE ALL ON DATABASE ${quoteIdentifier(properties.databaseName)} FROM ${quotedRole};`);
        } finally {
            await ownerClientManager.end();
        }

        log('Dropping role', { username: roleName });
        await adminClient.query(`DROP ROLE IF EXISTS ${quotedRole};`);
    } finally {
        await adminClientManager.end();
    }

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
