import type { Sandbox } from '@cloudflare/sandbox';
import type { MoltbotEnv } from '../types';
import { R2_MOUNT_PATH } from '../config';
import { mountR2Storage } from './r2';
import { waitForProcess } from './utils';

export interface SyncResult {
  success: boolean;
  lastSync?: string;
  error?: string;
  details?: string;
}

async function runCommand(
  sandbox: Sandbox,
  cmd: string,
  timeoutMs: number,
): Promise<{ stdout: string; stderr: string; status: string }> {
  const proc = await sandbox.startProcess(cmd);
  await waitForProcess(proc, timeoutMs);
  const logs = await proc.getLogs();
  // proc.status is a stale snapshot; get fresh status
  const status = proc.getStatus ? await proc.getStatus() : proc.status;
  return {
    stdout: logs.stdout || '',
    stderr: logs.stderr || '',
    status,
  };
}

function buildSyncCmd(configDir: string): string {
  // Exclude .git dirs (workspace/.git/ has 50+ hook files, each slow over s3fs).
  return [
    `rsync -r --no-times --delete --exclude='*.lock' --exclude='*.log' --exclude='*.tmp' --exclude='.git' ${configDir}/ ${R2_MOUNT_PATH}/openclaw/`,
    `([ -d /root/clawd ] && rsync -r --no-times --delete --exclude='skills' --exclude='.git' /root/clawd/ ${R2_MOUNT_PATH}/workspace/ || true)`,
    `([ -d /root/clawd/skills ] && rsync -r --no-times --delete /root/clawd/skills/ ${R2_MOUNT_PATH}/skills/ || true)`,
    `date -Iseconds > ${R2_MOUNT_PATH}/.last-sync`,
  ].join(' && ');
}

/**
 * Fire-and-forget sync for use in cron handlers.
 *
 * Starts the rsync chain but does NOT poll for completion. The scheduled
 * handler has a strict time limit — polling competes with slow s3fs
 * operations and can exceed it, causing an unhandled exception that resets
 * the Durable Object and kills the container.
 *
 * The cron fires every 5 minutes, so if one sync fails, the next catches it.
 */
export async function fireAndForgetSync(sandbox: Sandbox, env: MoltbotEnv): Promise<void> {
  if (!env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY || !env.CF_ACCOUNT_ID) {
    console.log('[cron] R2 not configured, skipping');
    return;
  }

  const mounted = await mountR2Storage(sandbox, env);
  if (!mounted) {
    console.log('[cron] R2 mount failed, skipping');
    return;
  }

  const checkNew = await runCommand(
    sandbox,
    'test -f /root/.openclaw/openclaw.json && echo exists',
    5000,
  );
  let configDir = '';
  if (checkNew.stdout.includes('exists')) {
    configDir = '/root/.openclaw';
  } else {
    const checkLegacy = await runCommand(
      sandbox,
      'test -f /root/.clawdbot/clawdbot.json && echo exists',
      5000,
    );
    if (checkLegacy.stdout.includes('exists')) {
      configDir = '/root/.clawdbot';
    } else {
      console.log('[cron] No config file found, skipping');
      return;
    }
  }

  await sandbox.startProcess(buildSyncCmd(configDir));
  console.log('[cron] Sync command started (fire-and-forget)');
}

/**
 * Sync OpenClaw config and workspace from container to R2 for persistence.
 *
 * This function:
 * 1. Mounts R2 if not already mounted
 * 2. Verifies source has critical files (prevents overwriting good backup with empty data)
 * 3. Runs rsync to copy config, workspace, and skills to R2
 * 4. Writes a timestamp file for tracking
 *
 * Syncs up to three directories:
 * - Config: /root/.openclaw/ (or /root/.clawdbot/) → R2:/openclaw/
 * - Workspace: /root/clawd/ → R2:/workspace/ (if exists)
 * - Skills: /root/clawd/skills/ → R2:/skills/ (if exists)
 */
export async function syncToR2(
  sandbox: Sandbox,
  env: MoltbotEnv,
  pollIntervalMs: number = 2000,
  maxPolls: number = 90,
): Promise<SyncResult> {
  if (!env.R2_ACCESS_KEY_ID || !env.R2_SECRET_ACCESS_KEY || !env.CF_ACCOUNT_ID) {
    return { success: false, error: 'R2 storage is not configured' };
  }

  const mounted = await mountR2Storage(sandbox, env);
  if (!mounted) {
    return { success: false, error: 'Failed to mount R2 storage' };
  }

  // Determine which config directory exists
  // Use stdout-based detection: exitCode is unreliable (often undefined in sandbox API)
  let configDir = '';
  try {
    const checkNew = await runCommand(
      sandbox,
      'test -f /root/.openclaw/openclaw.json && echo exists',
      5000,
    );
    if (checkNew.stdout.includes('exists')) {
      configDir = '/root/.openclaw';
    } else {
      const checkLegacy = await runCommand(
        sandbox,
        'test -f /root/.clawdbot/clawdbot.json && echo exists',
        5000,
      );
      if (checkLegacy.stdout.includes('exists')) {
        configDir = '/root/.clawdbot';
      } else {
        return {
          success: false,
          error: 'Sync aborted: no config file found',
          details: 'Neither openclaw.json nor clawdbot.json found in config directory.',
        };
      }
    }
  } catch (err) {
    return {
      success: false,
      error: 'Failed to verify source files',
      details: err instanceof Error ? err.message : 'Unknown error',
    };
  }

  const syncCmd = buildSyncCmd(configDir);

  try {
    await sandbox.startProcess(syncCmd);
  } catch (err) {
    return {
      success: false,
      error: 'Sync error',
      details: err instanceof Error ? err.message : 'Unknown error',
    };
  }

  // Poll for the timestamp file to appear (proves the entire && chain completed)
  for (let i = 0; i < maxPolls; i++) {
    // eslint-disable-next-line no-await-in-loop -- intentional sequential polling
    await new Promise((r) => setTimeout(r, pollIntervalMs));
    try {
      const ts = await runCommand(sandbox, `cat ${R2_MOUNT_PATH}/.last-sync`, 10000); // eslint-disable-line no-await-in-loop -- intentional sequential polling
      const lastSync = ts.stdout.trim();
      if (lastSync && lastSync.match(/^\d{4}-\d{2}-\d{2}/)) {
        return { success: true, lastSync };
      }
    } catch {
      // cat failed, keep polling
    }
  }
  return {
    success: false,
    error: 'Sync timed out',
    details: 'Timestamp file not created within 3 minutes',
  };
}
