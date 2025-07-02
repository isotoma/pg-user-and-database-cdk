import { Construct } from 'constructs';
import * as pathlib from 'path';
import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import type * as rds from 'aws-cdk-lib/aws-rds';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import type * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as cr from 'aws-cdk-lib/custom-resources';
import * as lambda from 'aws-cdk-lib/aws-lambda';

export interface PostgresUserAndDatabaseProps {
    dbCluster: rds.IDatabaseCluster;
    dbSecret: secretsmanager.ISecret;
    // Must have a secretString with the following keys:
    // - username
    // - password
    userSecret?: secretsmanager.ISecret;
    // Must set this if userSecret is not provided, and a password
    // will be generated with keys username and password (as well as
    // dbname, host, and port)
    username?: string;
    databaseName: string;
    vpc: ec2.IVpc;
    // Defaults to Fail
    onCreateIfExists?: 'Fail' | 'Adopt' | 'DeleteAndRecreate';
    // Defaults to Delete
    onDelete?: 'Delete' | 'Retain';
    // Defaults to Ignore
    onUpdateIfUserDoesNotExist?: 'Ignore' | 'Create';
    // Defaults to Ignore
    onUpdateIfDatabaseDoesNotExist?: 'Ignore' | 'Create';
    // Defaults to Never
    onUpdateSetUserPassword?: 'Always' | 'Never';
    // Defaults to Never
    onUpdateSetUserPermissions?: 'Always' | 'Never';
    // Defaults to Never
    onUpdateSetDatabaseOwnership?: 'Always' | 'Never';
    // Check for a new secret version every time the custom resource is updated
    // Defaults to false
    onUpdateCheckSecretVersion?: boolean;
}

export const DEFAULT_PASSWORD_EXCLUDE_CHARS = ' %+~`#$&*()|[]{}:;<>?!\'/@"\\';

export class PostgresUserAndDatabase extends Construct {
    readonly userSecret: secretsmanager.ISecret;

    constructor(scope: Construct, id: string, props: PostgresUserAndDatabaseProps) {
        super(scope, id);

        // Using a custom resource, create a user and database in the RDS cluster
        // https://docs.aws.amazon.com/cdk/api/latest/docs/custom-resources-readme.html

        const handler = new lambda.Function(this, 'OnEvent', {
            code: lambda.Code.fromAsset(pathlib.join(__dirname, 'handler')),
            runtime: new lambda.Runtime('nodejs22.x', lambda.RuntimeFamily.NODEJS, { supportsInlineCode: true }),
            handler: 'main.handler',
            vpc: props.vpc,
            timeout: cdk.Duration.seconds(30),
        });

        if (props.userSecret) {
            this.userSecret = props.userSecret;
        } else if (props.username) {
            this.userSecret =
                props.userSecret ??
                new secretsmanager.Secret(this, 'UserSecret', {
                    generateSecretString: {
                        passwordLength: 30,
                        secretStringTemplate: JSON.stringify({
                            username: props.username,
                            dbname: props.databaseName,
                            host: props.dbCluster.clusterEndpoint.hostname,
                            port: props.dbCluster.clusterEndpoint.port,
                        }),
                        generateStringKey: 'password',
                        excludeCharacters: DEFAULT_PASSWORD_EXCLUDE_CHARS,
                    },
                });
        } else {
            throw new Error('Must provide either userSecret or username');
        }

        props.dbSecret.grantRead(handler);
        this.userSecret.grantRead(handler);

        let secretLatestVersion: string | undefined = undefined;
        if (props.onUpdateCheckSecretVersion) {
            const secretLatestVersionHandler = new lambda.Function(this, 'OnEventSecretLatestVersion', {
                code: lambda.Code.fromAsset(pathlib.join(__dirname, 'latest_secret_version_handler')),
                runtime: new lambda.Runtime('nodejs22.x', lambda.RuntimeFamily.NODEJS, { supportsInlineCode: true }),
                handler: 'main.handler',
                timeout: cdk.Duration.seconds(30),
            });

            // Grant the function secretmanager:ListSecretVersionIds
            if (!secretLatestVersionHandler.role) {
                throw new Error('Lambda for SecretLatestVersion has no role');
            }

            const policyResult = secretLatestVersionHandler.role.addToPrincipalPolicy(
                new iam.PolicyStatement({
                    actions: ['secretsmanager:ListSecretVersionIds'],
                    resources: [this.userSecret.secretArn],
                }),
            );

            const secretLatestVersionProvider = new cr.Provider(this, 'SecretLatestVersionProvider', {
                onEventHandler: secretLatestVersionHandler,
            });

            const secretLatestVersionCustomResource = new cdk.CustomResource(this, 'SecretLatestVersionResource', {
                serviceToken: secretLatestVersionProvider.serviceToken,
                properties: {
                    secretArn: this.userSecret.secretArn,
                    datetime: `onUpdateCheckSecretVersion: ${new Date().toISOString()}`,
                },
            });

            if (policyResult.policyDependable) {
                secretLatestVersionCustomResource.node.addDependency(policyResult.policyDependable);
            }

            secretLatestVersion = secretLatestVersionCustomResource.getAttString('LatestVersionId');
        }

        handler.connections.allowToDefaultPort(props.dbCluster);

        const provider = new cr.Provider(this, 'Provider', {
            onEventHandler: handler,
        });

        const customResource = new cdk.CustomResource(this, 'Resource', {
            serviceToken: provider.serviceToken,
            properties: {
                dbClusterHostname: props.dbCluster.clusterEndpoint.hostname,
                dbClusterPort: props.dbCluster.clusterEndpoint.port,
                dbSecretArn: props.dbSecret.secretArn,
                userSecretArn: this.userSecret.secretArn,
                databaseName: props.databaseName,
                onDelete: props.onDelete ?? 'Delete',
                onCreateIfExists: props.onCreateIfExists ?? 'Fail',
                onUpdateIfUserDoesNotExist: props.onUpdateIfUserDoesNotExist ?? 'Ignore',
                onUpdateIfDatabaseDoesNotExist: props.onUpdateIfDatabaseDoesNotExist ?? 'Ignore',
                onUpdateSetUserPassword: props.onUpdateSetUserPassword ?? 'Never',
                onUpdateSetUserPermissions: props.onUpdateSetUserPermissions ?? 'Never',
                onUpdateSetDatabaseOwnership: props.onUpdateSetDatabaseOwnership ?? 'Never',
                ...(secretLatestVersion ? { secretLatestVersion } : {}),
            },
        });

        customResource.node.addDependency(...handler.connections.securityGroups);
        customResource.node.addDependency(this.userSecret);
    }
}
