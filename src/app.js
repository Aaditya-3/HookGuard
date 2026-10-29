import express from "express";

const app = express();

// Disable the X-Powered-By response header.
app.disable("x-powered-by");

// Parse incoming JSON request bodies.
app.use(express.json({ limit: "256kb" }));

// Check whether the application is running.
app.get("/health/live", (req, res) => {
  res.status(200).json({
    status: "ok",
  });
});

// Handle requests for routes that don't exist.
app.use((req, res) => {
  res.status(404).json({
    error: {
      code: "NOT_FOUND",
      message: "Route not found",
    },
  });
});

// Handle errors raised by middleware and route handlers.
app.use((err, req, res, next) => {
  if (err?.type === "entity.parse.failed") {
    return res.status(400).json({
      error: {
        code: "INVALID_JSON",
        message: "Request body must contain valid JSON",
      },
    });
  }

  console.error(err);

  return res.status(500).json({
    error: {
      code: "INTERNAL_ERROR",
      message: "An unexpected error occurred",
    },
  });
});

export default app;