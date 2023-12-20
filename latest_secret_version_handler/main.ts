// A handler for the custom resource. Is called with the following properties from Cloudformation:
// - secretArn: The ARN of the secret to identify the latest version of
// - datetime: The datetime as a string to ensure this function is always called

import * as secretsmanager from '@aws-sdk/client-secrets-manager';
import { z } from 'zod';

interface Response {
    PhysicalResourceId: string;
    Data: {
        LatestVersionId: string;
    };
}

const resourcePropertiesSchema = z.object({
    secretArn: z.string(),
    datetime: z.string(),
});

const onEventSchema = z.object({
    ResourceProperties: resourcePropertiesSchema,
});

const onCreateSchema = onEventSchema.extend({
    RequestType: z.literal('Create'),
});

const onUpdateSchema = onEventSchema.extend({
    RequestType: z.literal('Update'),
    PhysicalResourceId: z.string(),
});

const onDeleteSchema = onEventSchema.extend({
    RequestType: z.literal('Delete'),
    PhysicalResourceId: z.string(),
});

type OnCreateEvent = z.infer<typeof onCreateSchema>;
type OnUpdateEvent = z.infer<typeof onUpdateSchema>;
type OnDeleteEvent = z.infer<typeof onDeleteSchema>;

const onCreate = async (event: OnCreateEvent): Promise<Response> => {
    const { secretArn } = event.ResourceProperties;
    const client = new secretsmanager.SecretsManagerClient({});
    const response = await client.send(new secretsmanager.ListSecretVersionIdsCommand({
        SecretId: secretArn,
    }));
    const latestVersionId = response.Versions?.find((version) => version.VersionStages?.includes('AWSCURRENT'))?.VersionId;

    if (!latestVersionId) {
        throw new Error(`No latest version found for secret ${secretArn}`);
    }

    return {
        PhysicalResourceId: secretArn,
        Data: {
            LatestVersionId: latestVersionId,
        },
    };
};

const onUpdate = async (event: OnUpdateEvent): Promise<Response> => {
    return onCreate({
        ...event,
        RequestType: 'Create',
    });
};

const onDelete = async (event: OnDeleteEvent): Promise<Response> => {
    return {
        PhysicalResourceId: event.ResourceProperties.secretArn,
        Data: {
            LatestVersionId: event.PhysicalResourceId,
        },
    };
};
        

export const handler = async (event: unknown): Promise<Response> => {
    const onCreateEvent = onCreateSchema.safeParse(event);
    if (onCreateEvent.success) {
        return onCreate(onCreateEvent.data);
    }

    const onUpdateEvent = onUpdateSchema.safeParse(event);
    if (onUpdateEvent.success) {
        return onUpdate(onUpdateEvent.data);
    }

    const onDeleteEvent = onDeleteSchema.safeParse(event);
    if (onDeleteEvent.success) {
        return onDelete(onDeleteEvent.data);
    }

    throw new Error('Unexpected event type');
}
