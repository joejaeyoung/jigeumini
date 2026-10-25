import type { z } from 'zod';

export class ApiError extends Error {
  constructor(public readonly reason: 'network' | 'response' | 'contract') { super(reason); }
}
export async function read<T>(url: string, schema: z.ZodType<T>, signal: AbortSignal): Promise<T> {
  try {
    const response = await fetch(url, { signal: AbortSignal.any([signal, AbortSignal.timeout(8000)]), headers: { Accept: 'application/json' } });
    if (!response.ok) throw new ApiError('response');
    const parsed = schema.safeParse(await response.json());
    if (!parsed.success) throw new ApiError('contract');
    return parsed.data;
  } catch (error) {
    if (signal.aborted) throw error;
    throw error instanceof ApiError ? error : new ApiError('network');
  }
}
