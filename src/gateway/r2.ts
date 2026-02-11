import type { Sandbox } from '@cloudflare/sandbox';
import type { MoltbotEnv } from '../types';
import { R2_MOUNT_PATH, getR2BucketName } from '../config';
import { waitForProcess } from './utils';

/**
 * Check if R2 is already mounted by looking at the mount table.
 * Uses stdout marker pattern because getLogs() often returns empty.
 */
async function isR2Mounted(sandbox: Sandbox): Promise<boolean> {
  try {
    const proc = await sandbox.startProcess(
      `mount | grep -q "s3fs on ${R2_MOUNT_PATH}" && echo mounted || echo not-mounted`,
    );
    await waitForProcess(proc, 5000);
    const logs = await proc.getLogs();
    const mounted = !!(
      logs.stdout &&
      logs.stdout.includes('mounted') &&
      !logs.stdout.includes('not-mounted')
    );
    console.log('isR2Mounted check:', mounted, 'stdout:', logs.stdout?.slice(0, 100));
    return mounted;
  } catch (err) {
    console.log('isR2Mounted error:', err);
    return false;
  }
}

/**
 * Mount R2 bucket for persistent storage
 *
 * @param sandbox - The sandbox instance
 * @param env - Worker environment bindings
 * @returns true if mounted successfully, false otherwise
 */
export async function mountR2Storage(sandbox: Sandbox, env: MoltbotEnv): Promise<boolean> {
  if (!env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY || !env.CF_ACCOUNT_ID) {
    console.log(
      'R2 storage not configured (missing R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, or CF_ACCOUNT_ID)',
    );
    return false;
  }

  if (await isR2Mounted(sandbox)) {
    console.log('R2 bucket already mounted at', R2_MOUNT_PATH);
    return true;
  }

  const bucketName = getR2BucketName(env);
  try {
    console.log('Mounting R2 bucket', bucketName, 'at', R2_MOUNT_PATH);
    await sandbox.mountBucket(bucketName, R2_MOUNT_PATH, {
      endpoint: `https://${env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
      credentials: {
        accessKeyId: env.R2_ACCESS_KEY_ID,
        secretAccessKey: env.R2_SECRET_ACCESS_KEY,
      },
    });
    console.log('R2 bucket mounted successfully - moltbot data will persist across sessions');
    return true;
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    console.log('R2 mount error:', errorMessage);

    // Check again if it's mounted - the error might be misleading (e.g. "already mounted")
    if (await isR2Mounted(sandbox)) {
      console.log('R2 bucket is mounted despite error');
      return true;
    }

    console.error('Failed to mount R2 bucket:', err);
    return false;
  }
}
