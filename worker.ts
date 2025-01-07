// src/scheduler.ts
// First, import the RequestScheduler implementation from previous artifact
import { RequestScheduler } from "./scheduler";

// wrangler.toml should include:
// [[durable_objects.bindings]]
// name = "SCHEDULER"
// class_name = "RequestSchedulerDO"

export interface Env {
  SCHEDULER: DurableObjectNamespace;
}

// Durable Object implementation
export class RequestSchedulerDO {
  private scheduler: RequestScheduler;
  state: DurableObjectState;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.scheduler = new RequestScheduler(state, env);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    try {
      if (request.method === "POST") {
        // Schedule new batch of requests
        const body = await request.json();

        if (!Array.isArray(body.requests)) {
          return new Response("requests must be an array", { status: 400 });
        }

        if (!body.clientId) {
          return new Response("clientId is required", { status: 400 });
        }

        const result = await this.scheduler.schedule(
          body.clientId,
          body.requests,
          body.options,
        );

        return new Response(JSON.stringify(result), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      } else if (request.method === "GET") {
        // Get status of a job
        const clientId = url.pathname.slice(1); // Remove leading slash
        if (!clientId) {
          return new Response("jobId is required", { status: 400 });
        }

        const status = await this.scheduler.status(clientId);
        return new Response(JSON.stringify(status), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }

      return new Response("Method not allowed", { status: 405 });
    } catch (error) {
      console.error("Error in DO:", error);
      return new Response(
        JSON.stringify({
          error: error instanceof Error ? error.message : "Unknown error",
        }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        },
      );
    }
  }
}

// Worker implementation
export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    try {
      // CORS handling
      if (request.method === "OPTIONS") {
        return new Response(null, {
          headers: {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
          },
        });
      }

      // Get authorization header
      const auth = request.headers.get("Authorization");
      if (!auth) {
        return new Response("Authorization required", { status: 401 });
      }

      // Get the URL and path
      const url = new URL(request.url);
      const path = url.pathname.slice(1); // Remove leading slash

      // For POST requests, get the client ID from the body
      let clientId = path;
      if (request.method === "POST") {
        const body = await request.clone().json();
        clientId = body.clientId;
        if (!clientId) {
          return new Response("clientId is required", { status: 400 });
        }
      }

      // Create a stable ID for the Durable Object based on auth + clientId
      const id = env.SCHEDULER.idFromName(`${auth}:${clientId}`);
      const scheduler = env.SCHEDULER.get(id);

      // Forward the request to the Durable Object
      const response = await scheduler.fetch(request);

      // Add CORS headers to response
      const corsHeaders = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      };

      // Return response with CORS headers
      return new Response(response.body, {
        status: response.status,
        headers: {
          ...Object.fromEntries(response.headers),
          ...corsHeaders,
        },
      });
    } catch (error) {
      console.error("Error in worker:", error);
      return new Response(
        JSON.stringify({
          error: error instanceof Error ? error.message : "Unknown error",
        }),
        {
          status: 500,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
        },
      );
    }
  },
};

// Example wrangler.toml:
/*
name = "request-scheduler"
main = "src/worker.ts"
compatibility_date = "2024-01-07"

[[durable_objects.bindings]]
name = "SCHEDULER"
class_name = "RequestSchedulerDO"

[[migrations]]
tag = "v1"
new_classes = ["RequestSchedulerDO"]
*/

// Example usage:
/*
// Schedule new requests
curl -X POST https://your-worker.workers.dev/ \
  -H "Authorization: Bearer your-token" \
  -H "Content-Type: application/json" \
  -d '{
    "clientId": "test-job-123",
    "requests": [
      {
        "url": "https://api.example.com/users",
        "method": "GET",
        "headers": {
          "Accept": "application/json"
        }
      }
    ],
    "options": {
      "rateLimit": 100,
      "unit": "minute"
    }
  }'

// Check status
curl https://your-worker.workers.dev/test-job-123 \
  -H "Authorization: Bearer your-token"
*/
