// Connection handling shared by the custom resource handlers. Extracted
// verbatim from handler/main.ts so that readonly_role_handler can reuse it
// rather than carry a second copy of the same client management.
//
// Pure helpers live in ./sql instead: this module pulls in pg and the AWS SDK,
// and importing it from a test would drag both into the coverage report.

import * as pg from 'pg';
import * as secretsmanager from '@aws-sdk/client-secrets-manager';
import { z } from 'zod';

export interface DbCredentials {
    username: string;
    password: string;
}

// Class that lazily creates a Postgres client from a secret ARN,
// dbClusterHostname, dbClusterPort and databaseName, and caches the client
export interface LazyPostgresClientFromSecretsManagerProps {
    dbSecretArn: string;
    dbClusterHostname: string;
    dbClusterPort: number;
    databaseName: string;
}

export class LazyPostgresClientFromSecretsManager {
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
export const log = (message: string, data?: unknown): void => {
    console.log(JSON.stringify({ message, data }));
};
