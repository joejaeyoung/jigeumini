import { z } from 'zod';

export const timestamp = z.iso.datetime({ offset: true });
export const count = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
export const channelIdSchema = z.string().regex(/^[a-f0-9]{32}$/);
