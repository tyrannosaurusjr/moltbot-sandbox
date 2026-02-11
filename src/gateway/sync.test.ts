import { describe, it, expect, beforeEach } from 'vitest';
import { syncToR2 } from './sync';
import {
  createMockEnv,
  createMockEnvWithR2,
  createMockProcess,
  createMockSandbox,
  suppressConsole,
} from '../test-utils';

describe('syncToR2', () => {
  beforeEach(() => {
    suppressConsole();
  });

  describe('configuration checks', () => {
    it('returns error when R2 is not configured', async () => {
      const { sandbox } = createMockSandbox();
      const env = createMockEnv();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('R2 storage is not configured');
    });

    it('returns error when mount fails', async () => {
      const { sandbox, startProcessMock, mountBucketMock } = createMockSandbox();
      startProcessMock.mockResolvedValue(createMockProcess(''));
      mountBucketMock.mockRejectedValue(new Error('Mount failed'));

      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Failed to mount R2 storage');
    });
  });

  describe('sanity checks', () => {
    it('returns error when source has no config file', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('mounted\n'))
        .mockResolvedValueOnce(createMockProcess('')) // No openclaw.json
        .mockResolvedValueOnce(createMockProcess('')); // No clawdbot.json either

      const env = createMockEnvWithR2();

      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(false);
      expect(result.error).toBe('Sync aborted: no config file found');
    });
  });

  describe('sync execution', () => {
    it('returns success when timestamp file is written', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      const timestamp = '2026-01-27T12:00:00+00:00';

      // Calls: mount check, config detect, rsync (all-in-one), cat timestamp
      startProcessMock
        .mockResolvedValueOnce(createMockProcess('mounted\n'))
        .mockResolvedValueOnce(createMockProcess('exists'))
        .mockResolvedValueOnce(createMockProcess('')) // rsync chain
        .mockResolvedValueOnce(createMockProcess(timestamp)); // cat timestamp

      const env = createMockEnvWithR2();
      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(true);
      expect(result.lastSync).toBe(timestamp);
    });

    it('falls back to legacy clawdbot config directory', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      const timestamp = '2026-01-27T12:00:00+00:00';

      startProcessMock
        .mockResolvedValueOnce(createMockProcess('mounted\n'))
        .mockResolvedValueOnce(createMockProcess('')) // No openclaw.json
        .mockResolvedValueOnce(createMockProcess('exists')) // clawdbot.json found
        .mockResolvedValueOnce(createMockProcess('')) // rsync chain
        .mockResolvedValueOnce(createMockProcess(timestamp));

      const env = createMockEnvWithR2();
      const result = await syncToR2(sandbox, env);

      expect(result.success).toBe(true);

      // rsync chain should reference .clawdbot
      const rsyncCall = startProcessMock.mock.calls[3][0];
      expect(rsyncCall).toContain('/root/.clawdbot/');
    });

    it('returns error when no timestamp after sync', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();

      startProcessMock
        .mockResolvedValueOnce(createMockProcess('mounted\n'))
        .mockResolvedValueOnce(createMockProcess('exists'))
        .mockResolvedValueOnce(createMockProcess('')) // rsync chain
        .mockResolvedValue(createMockProcess('')); // all cat polls return empty

      const env = createMockEnvWithR2();
      const result = await syncToR2(sandbox, env, 10, 3); // fast: 10ms interval, 3 polls

      expect(result.success).toBe(false);
      expect(result.error).toBe('Sync timed out');
    });

    it('verifies rsync command excludes .git', async () => {
      const { sandbox, startProcessMock } = createMockSandbox();
      const timestamp = '2026-01-27T12:00:00+00:00';

      startProcessMock
        .mockResolvedValueOnce(createMockProcess('mounted\n'))
        .mockResolvedValueOnce(createMockProcess('exists'))
        .mockResolvedValueOnce(createMockProcess(''))
        .mockResolvedValueOnce(createMockProcess(timestamp));

      const env = createMockEnvWithR2();
      await syncToR2(sandbox, env);

      const rsyncCall = startProcessMock.mock.calls[2][0];
      expect(rsyncCall).toContain("--exclude='.git'");
      expect(rsyncCall).toContain('/root/.openclaw/');
      expect(rsyncCall).toContain('/data/moltbot/openclaw/');
    });
  });
});
