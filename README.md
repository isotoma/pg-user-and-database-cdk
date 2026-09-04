# pg-user-and-database-cdk

[![docs](https://img.shields.io/badge/docs-!-brightgreen)](https://isotoma.github.io/pg-user-and-database-cdk/) [![npm](https://img.shields.io/npm/v/pg-user-and-database-cdk)](https://www.npmjs.com/package/pg-user-and-database-cdk) [![NPM](https://img.shields.io/npm/l/pg-user-and-database-cdk)](./LICENSE)

## Getting started

TODO

## PostgresReadOnlyRole

A login role with `SELECT` on named tables and nothing else. Use it where
something needs to read a few of an application's tables without holding the
application's own credentials, such as a metrics job.

```typescript
const app = new PostgresUserAndDatabase(this, 'UserAndDatabase', {
    dbCluster,
    dbSecret: dbClusterSecret,
    username: 'myapp',
    databaseName: 'myapp',
    vpc,
});

const reader = new PostgresReadOnlyRole(this, 'ReaderRole', {
    dbCluster,
    adminSecret: dbClusterSecret,
    ownerSecret: app.userSecret,
    roleName: 'myapp_reader',
    databaseName: 'myapp',
    tableNames: ['interesting_table'],
    vpc,
});

// reader.roleSecret holds host, port, dbname, username and password
```

`adminSecret` and `ownerSecret` are both required, and the split between them is
not the obvious one. Creating the role needs `CREATEROLE`, which the cluster
admin has and the application user does not. Every `GRANT` has to come from the
owner: on Aurora the master user is not a true superuser, and
`PostgresUserAndDatabase` transfers database ownership to the application user,
so the admin can grant neither `CONNECT` on the database nor `SELECT` on its
tables. The `CONNECT` grant is the trap, since attempting it as the admin fails
with nothing worse than `WARNING: no privileges were granted`.

`onDelete` defaults to `Retain`, unlike `PostgresUserAndDatabase`. Removing the
construct leaves the role in place rather than breaking whatever is still
connecting with it; pass `Drop` to revoke the grants and drop the role.

Adding a table to `tableNames` grants it on the next deploy. Removing one does
not revoke it: that would mean tracking the previous property values, and a
stale `SELECT` is the less surprising of the two failure modes. Use `Drop` and
recreate if a grant genuinely needs removing.
