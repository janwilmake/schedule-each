// scheduler.ts

type RateUnit = "hour" | "minute" | "second";

interface RateLimitOptions {
  rateLimit: number;
  unit: RateUnit;
}

interface RequestConfig {
  url: string;
  method: string;
  headers?: Record<string, string>;
  body?: unknown;
}

interface JobStatus {
  status: "not_found" | "processing" | "complete";
  progress?: {
    completed: number;
    total: number;
    startedAt: number;
    estimatedCompletion: number | null;
  };
  results?: Array<{
    url: string;
    method: string;
    status?: number;
    responseData?: unknown;
    error?: string;
    processedAt: number;
  }>;
}

interface DurableState {
  storage: {
    prepare: (sql: string) => SQLiteStatement;
    exec: (sql: string) => Promise<void>;
    getAlarm: () => Promise<number | null>;
    setAlarm: (time: number) => Promise<void>;
    deleteAlarm: () => Promise<void>;
  };
  blockConcurrencyWhile: (fn: () => Promise<void>) => Promise<void>;
}

interface SQLiteStatement {
  run: (...params: unknown[]) => Promise<void>;
  first: <T = unknown>(...params: unknown[]) => Promise<T | undefined>;
  all: <T = unknown>(...params: unknown[]) => Promise<T[]>;
}

export class RequestScheduler {
  private state: DurableState;
  private env: unknown;
  private processing: boolean;
  private lastCheckpoint: number;
  private currentJobId: string | null;

  constructor(state: DurableState, env: unknown) {
    this.state = state;
    this.env = env;
    this.processing = false;
    this.lastCheckpoint = Date.now();
    this.currentJobId = null;

    // Handle alarm wakeups for resuming processing
    this.state.blockConcurrencyWhile(async () => {
      const alarm = await this.state.storage.getAlarm();
      if (alarm) {
        // We have an alarm set, meaning we were previously processing
        await this.resumeProcessing();
      }
    });
  }

  private async initialize(): Promise<void> {
    // Initialize SQLite tables if they don't exist
    await this.state.storage
      .prepare(
        `
      CREATE TABLE IF NOT EXISTS jobs (
        jobId TEXT PRIMARY KEY,
        startedAt INTEGER,
        status TEXT,
        rateLimit INTEGER,
        rateUnit TEXT CHECK(rateUnit IN ('hour', 'minute', 'second')),
        lastProcessedIndex INTEGER DEFAULT -1
      )
    `,
      )
      .run();

    await this.state.storage
      .prepare(
        `
      CREATE TABLE IF NOT EXISTS requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        jobId TEXT,
        url TEXT,
        method TEXT,
        headers TEXT,
        body TEXT,
        status INTEGER,
        responseData TEXT,
        error TEXT,
        processedAt INTEGER,
        FOREIGN KEY (jobId) REFERENCES jobs(jobId)
      )
    `,
      )
      .run();

    // Get current job ID if exists
    const currentJob = await this.state.storage
      .prepare("SELECT jobId FROM jobs WHERE status = 'processing' LIMIT 1")
      .first<{ jobId: string }>();

    this.currentJobId = currentJob?.jobId ?? null;
  }

  async schedule(
    jobId: string,
    requests: RequestConfig[],
    options: RateLimitOptions = { rateLimit: 5000, unit: "hour" },
  ): Promise<{ jobId: string; resuming?: boolean }> {
    await this.initialize();

    // Validate requests array
    if (!Array.isArray(requests) || requests.length === 0) {
      throw new Error("Requests must be a non-empty array");
    }

    // Validate each request
    for (const req of requests) {
      if (!req.url || !req.method) {
        throw new Error("Each request must have at least a url and method");
      }
    }

    // Validate rate limit options
    if (!Number.isInteger(options.rateLimit) || options.rateLimit <= 0) {
      throw new Error("rateLimit must be a positive integer");
    }

    if (!["hour", "minute", "second"].includes(options.unit)) {
      throw new Error("unit must be one of: hour, minute, second");
    }

    // Check if job already exists
    const existingJob = await this.state.storage
      .prepare("SELECT lastProcessedIndex FROM jobs WHERE jobId = ?")
      .first<{ lastProcessedIndex: number }>(jobId);

    if (existingJob !== undefined) {
      return { jobId, resuming: true };
    }

    // Start transaction
    await this.state.storage.exec("BEGIN TRANSACTION");

    try {
      // Insert job
      await this.state.storage
        .prepare(
          `
        INSERT INTO jobs (
          jobId, 
          startedAt, 
          status, 
          rateLimit,
          rateUnit,
          lastProcessedIndex
        )
        VALUES (?, ?, ?, ?, ?, ?)
      `,
        )
        .run(
          jobId,
          Date.now(),
          "processing",
          options.rateLimit,
          options.unit,
          -1,
        );

      // Batch insert requests
      const stmt = await this.state.storage.prepare(`
        INSERT INTO requests (
          jobId, 
          url,
          method,
          headers,
          body
        )
        VALUES (?, ?, ?, ?, ?)
      `);

      for (const request of requests) {
        await stmt.run(
          jobId,
          request.url,
          request.method,
          request.headers ? JSON.stringify(request.headers) : null,
          request.body ? JSON.stringify(request.body) : null,
        );
      }

      await this.state.storage.exec("COMMIT");
    } catch (error) {
      await this.state.storage.exec("ROLLBACK");
      throw error;
    }

    // Start processing if not already running
    if (!this.processing) {
      await this.resumeProcessing();
    }

    return { jobId };
  }

  async status(jobId: string): Promise<JobStatus> {
    await this.initialize();

    interface JobRow {
      status: string;
      startedAt: number;
      completed: number;
      total: number;
    }

    const job = await this.state.storage
      .prepare(
        `
      SELECT j.*, 
             COUNT(CASE WHEN r.status IS NOT NULL OR r.error IS NOT NULL THEN 1 END) as completed,
             COUNT(*) as total
      FROM jobs j
      LEFT JOIN requests r ON j.jobId = r.jobId
      WHERE j.jobId = ?
      GROUP BY j.jobId
    `,
      )
      .first<JobRow>(jobId);

    if (!job) return { status: "not_found" };

    if (job.status === "complete") {
      interface RequestRow {
        url: string;
        method: string;
        status: number | null;
        responseData: string | null;
        error: string | null;
        processedAt: number;
      }

      const results = await this.state.storage
        .prepare(
          `
        SELECT url, method, status, responseData, error, processedAt
        FROM requests
        WHERE jobId = ?
        ORDER BY id
      `,
        )
        .all<RequestRow>(jobId);

      return {
        status: "complete",
        results: results.map((r) => ({
          ...r,
          responseData: r.responseData ? JSON.parse(r.responseData) : undefined,
          status: r.status ?? undefined,
          error: r.error ?? undefined,
        })),
      };
    }

    return {
      status: "processing",
      progress: {
        completed: job.completed,
        total: job.total,
        startedAt: job.startedAt,
        estimatedCompletion: this.estimateCompletion(
          {
            startedAt: job.startedAt,
            total: job.total,
          },
          job.completed,
        ),
      },
    };
  }

  private async resumeProcessing(): Promise<void> {
    if (this.processing) return;
    this.processing = true;

    try {
      while (true) {
        interface JobRow {
          jobId: string;
          rateLimit: number;
          rateUnit: RateUnit;
        }

        // Get next unfinished job
        const job = await this.state.storage
          .prepare(
            `
          SELECT * FROM jobs 
          WHERE status = 'processing'
          ORDER BY startedAt
          LIMIT 1
        `,
          )
          .first<JobRow>();

        if (!job) break;

        this.currentJobId = job.jobId;
        const delay = this.calculateDelay(job.rateLimit, job.rateUnit);

        interface RequestRow {
          id: number;
          url: string;
          method: string;
          headers: string | null;
          body: string | null;
        }

        // Get all unprocessed requests for this job
        const requests = await this.state.storage
          .prepare(
            `
          SELECT id, url, method, headers, body
          FROM requests 
          WHERE jobId = ? AND status IS NULL AND error IS NULL
          ORDER BY id
        `,
          )
          .all<RequestRow>(job.jobId);

        for (const request of requests) {
          try {
            const headers = request.headers ? JSON.parse(request.headers) : {};
            const body = request.body ? JSON.parse(request.body) : undefined;

            const response = await fetch(request.url, {
              method: request.method,
              headers,
              body: body ? JSON.stringify(body) : undefined,
            });

            let responseData = null;
            if (response.ok) {
              const contentType = response.headers.get("content-type");
              if (contentType?.includes("application/json")) {
                responseData = await response.json();
              } else {
                responseData = await response.text();
              }
            }

            // Update request with response
            await this.state.storage
              .prepare(
                `
              UPDATE requests 
              SET status = ?, responseData = ?, processedAt = ?
              WHERE id = ?
            `,
              )
              .run(
                response.status,
                responseData ? JSON.stringify(responseData) : null,
                Date.now(),
                request.id,
              );
          } catch (error) {
            // Update request with error
            const errorMessage =
              error instanceof Error ? error.message : String(error);
            await this.state.storage
              .prepare(
                `
              UPDATE requests 
              SET error = ?, processedAt = ?
              WHERE id = ?
            `,
              )
              .run(errorMessage, Date.now(), request.id);
          }

          // Update job progress
          await this.state.storage
            .prepare(
              `
            UPDATE jobs 
            SET lastProcessedIndex = lastProcessedIndex + 1
            WHERE jobId = ?
          `,
            )
            .run(job.jobId);

          // Set an alarm to wake us up in case of crashes
          await this.state.storage.setAlarm(Date.now() + delay + 5000);

          // Respect rate limit
          await new Promise((resolve) => setTimeout(resolve, delay));
        }

        // Mark job as complete
        await this.state.storage
          .prepare(
            `
          UPDATE jobs 
          SET status = 'complete'
          WHERE jobId = ?
        `,
          )
          .run(job.jobId);
      }

      this.currentJobId = null;
      this.processing = false;
      await this.state.storage.deleteAlarm();
    } catch (error) {
      // On error, make sure we have an alarm set to resume
      await this.state.storage.setAlarm(Date.now() + 5000);
      throw error;
    }
  }

  private estimateCompletion(
    job: { startedAt: number; total: number },
    completed: number,
  ): number | null {
    if (completed === 0) return null;

    const elapsed = Date.now() - job.startedAt;
    const rate = completed / elapsed;
    const remaining = job.total - completed;

    return Date.now() + remaining / rate;
  }

  private calculateDelay(rateLimit: number, unit: RateUnit): number {
    const unitInMs =
      unit === "hour" ? 3600000 : unit === "minute" ? 60000 : 1000;
    return Math.ceil(unitInMs / rateLimit);
  }
}
