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

That secret shape applies when the construct generates the secret, as above. Pass
your own `roleSecret` instead and `reader.roleSecret` is exactly that secret,
which only has to carry `username` and `password` — the connection details are
yours to supply.

`adminSecret` and `ownerSecret` are both required, and the split between them is
not the obvious one. Creating the role needs `CREATEROLE`, which the cluster
admin has and the application user does not. Every `GRANT` has to come from the
owner: on Aurora the master user is not a true superuser, and
`PostgresUserAndDatabase` transfers database ownership to the application user,
so the admin can grant neither `CONNECT` on the database nor `SELECT` on its
tables. The `CONNECT` grant is the trap, since attempting it as the admin fails
with nothing worse than `WARNING: no privileges were granted`.

One limitation on "nothing else": the role still inherits whatever `PUBLIC`
holds. On a stock Postgres 16 database that is `CONNECT` and `TEMPORARY`, so the
role can open temporary tables even though it was never granted anything beyond
`SELECT`. That cannot be fixed per-role, because there is no way to revoke a
`PUBLIC` grant from one role: it needs `REVOKE TEMPORARY ON DATABASE <db> FROM
PUBLIC`, which changes the database for every role and so is left to the caller
rather than done here. `PUBLIC` no longer has `CREATE` on the `public` schema,
which changed in Postgres 15.

`onCreateIfExists` defaults to `Fail`. Adopting an existing role means resetting
its password and granting it `SELECT` on the tables, so if the name turned out to
belong to something else you would have quietly taken it over. Note the
interaction with `onDelete`, which defaults to `Retain`: recreating a role that a
previous stack left behind needs `onCreateIfExists: 'Adopt'`. Updates always
adopt, since the resource already owns the role by then.

`onDelete` defaults to `Retain`, unlike `PostgresUserAndDatabase`. Removing the
construct leaves the role in place rather than breaking whatever is still
connecting with it; pass `Drop` to revoke the grants and drop the role.

With `onDelete: 'Drop'`, avoid removing the owning `PostgresUserAndDatabase` in
the same operation. The construct only takes `ownerSecret`, so CloudFormation has
no dependency on the owner user itself, and every `REVOKE` has to run as that
owner. If the owner is dropped first the revokes cannot authenticate and the
stack deletion stalls. Remove the reader role in one deployment and the owner in
the next, or drop the role by hand.

Adding a table to `tableNames` grants it on the next deploy. Removing one does
not revoke it: that would mean tracking the previous property values, and a
stale `SELECT` is the less surprising of the two failure modes. Use `Drop` and
recreate if a grant genuinely needs removing.

## Releasing

From an up-to-date `main`:

```bash
npm version minor    # or patch / major
git push && git push --tags
```

`npm version` bumps `package.json`, regenerates `CHANGELOG.md`, commits, and tags.
`.npmrc` sets `tag-version-prefix=""`, so the tag is `1.3.0` rather than `v1.3.0`,
which is the form `.github/workflows/publish.yaml` triggers on. Pushing the tag
builds and publishes to npm and deploys the typedoc output to `gh-pages`.

Do the bump and the tag together, which is what `npm version` is for. The publish
job builds into `build/`, copies `package.json` in, and publishes from there, so
npm publishes whatever the version field says and not what the tag says. Tagging
`1.3.0` while `package.json` still reads `1.2.0` fails at `npm publish`, because
that version already exists.

### Authentication

Publishing uses [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/)
over OIDC, so there is no `NPM_TOKEN` secret. The job requests `id-token: write`
and the npm CLI exchanges that for a short-lived publish token by itself, which
also means provenance attestations are generated automatically.

The trust relationship is configured on npmjs.com under the package's settings,
naming the organisation, the repository, and the workflow filename
(`publish.yaml`). Two things to know if it ever needs recreating: configurations
created after 3rd September 2026 default to allowing `npm stage publish` only, so
direct publishing has to be ticked explicitly or the job fails; and existing
configurations cannot be edited, only deleted and recreated.

Trusted publishing needs npm 11.5.1 or later. Below that the CLI quietly falls
back to token authentication and fails with a misleading `E404` on the `PUT`, so
the workflow asserts the version rather than letting that happen.
