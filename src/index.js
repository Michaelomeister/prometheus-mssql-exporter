const appLog = require("debug")("app");
const dbLog = require("debug")("db");
const queriesLog = require("debug")("queries");

const Connection = require("tedious").Connection;
const Request = require("tedious").Request;
const app = require("express")();
const client = require("prom-client");

const { entries, register } = require("./metrics");

let config = {
  connect: {
    server: process.env["SERVER"],
    authentication: {
      type: "default",
      options: {
        userName: process.env["USERNAME"],
        password: process.env["PASSWORD"],
      },
    },
    options: {
      port: parseInt(process.env["PORT"]) || 1433,
      encrypt: process.env["ENCRYPT"] !== undefined ? process.env["ENCRYPT"] === "true" : true,
      trustServerCertificate: process.env["TRUST_SERVER_CERTIFICATE"] !== undefined ? process.env["TRUST_SERVER_CERTIFICATE"] === "true" : true,
      rowCollectionOnRequestCompletion: true,
    },
  },
  port: parseInt(process.env["EXPOSE"]) || 4000,
};

if (!config.connect.server) {
  throw new Error("Missing SERVER information");
}
if (!config.connect.authentication.options.userName) {
  throw new Error("Missing USERNAME information");
}
if (!config.connect.authentication.options.password) {
  throw new Error("Missing PASSWORD information");
}

/**
 * Connects to a database server.
 *
 * @returns Promise<Connection>
 */
async function connect() {
  return new Promise((resolve, reject) => {
    dbLog(
      "Connecting to",
      config.connect.authentication.options.userName + "@" + config.connect.server + ":" + config.connect.options.port,
      "encrypt:",
      config.connect.options.encrypt,
      "trustServerCertificate:",
      config.connect.options.trustServerCertificate
    );

    let connection = new Connection(config.connect);
    connection.on("connect", (error) => {
      if (error) {
        console.error("Failed to connect to database:", error.message || error);
        reject(error);
      } else {
        dbLog("Connected to database");
        resolve(connection);
      }
    });
    connection.on("error", (error) => {
      console.error("Error while connected to database:", error.message || error);
      reject(error);
    });
    connection.on("end", () => {
      dbLog("Connection to database ended");
    });
    connection.connect();
  });
}

/**
 * Recursive function that executes all collectors sequentially
 *
 * @param connection database connection
 * @param collector single metric: {query: string, collect: function(rows, metric)}
 * @param name name of collector variable
 *
 * @returns Promise of collect operation (no value returned)
 */
async function measure(connection, collector, name) {
  return new Promise((resolve) => {
    queriesLog(`Executing metric ${name} query: ${collector.query}`);
    // Set timeout based on whether this is an async metric or standard metric
    const timeout = collector.asyncMetric ? collector.timeoutMs : 15000; // 15s default timeout

    let request = new Request(collector.query, (error, rowCount, rows) => {
      if (!error) {
        queriesLog(`Retrieved metric ${name} rows (${rows.length}): ${JSON.stringify(rows, null, 2)}`);

        if (rows.length > 0) {
          try {
            collector.collect(rows, collector.metrics);
          } catch (error) {
            console.error(`Error processing metric ${name} data`, collector.query, JSON.stringify(rows), error);
          }
        } else {
          console.error(`Query for metric ${name} returned 0 rows to process`, collector.query);
        }
        resolve();
      } else {
        console.error(`Error executing metric ${name} SQL query`, collector.query, error);
        resolve();
      }
    });

    // Set custom timeout for the request
    request.setTimeout(timeout);
    connection.execSql(request);
  });
}

/**
 * Function that collects from an active server.
 *
 * @param connection database connection
 *
 * @returns Promise of execution (no value returned)
 */
async function collect(connection) {
  const standardMetrics = [];
  const asyncMetrics = [];

  // Separate async metrics from standard metrics
  Object.entries(entries).forEach(([key, entry]) => {
    if (entry.asyncMetric) {
      asyncMetrics.push({ key, entry });
    } else {
      standardMetrics.push({ key, entry });
    }
  });

  // Process standard metrics first
  for (const { key, entry } of standardMetrics) {
    await measure(connection, entry, key);
  }

  // Process async metrics in the background
  asyncMetrics.forEach(({ key, entry }) => {
    // Create a separate connection for async metrics
    connect().then(asyncConnection => {
      measure(asyncConnection, entry, key)
        .then(() => {
          asyncConnection.close();
          console.log(`Async metric ${key} completed`);
        })
        .catch(error => {
          asyncConnection.close();
          console.error(`Error processing async metric ${key}:`, error);
        });
    }).catch(error => {
      console.error(`Error creating connection for async metric ${key}:`, error);
    });
  });
}

app.get("/", (req, res) => {
  res.redirect("/metrics");
});

app.get("/metrics", async (req, res) => {
  res.contentType(register.contentType);

  try {
    appLog("Received /metrics request");
    let connection = await connect();
    await collect(connection);
    connection.close();
    res.send(await register.metrics());
    appLog("Successfully processed /metrics request");
  } catch (error) {
    // error connecting
    appLog("Error handling /metrics request");
    const mssqlUp = entries.mssql_up.metrics.mssql_up;
    mssqlUp.set(0);
    res.header("X-Error", error.message || error);
    res.send(await register.getSingleMetricAsString(mssqlUp.name));
  }
});

const server = app.listen(config.port, function () {
  appLog(
    `Prometheus-MSSQL Exporter listening on local port ${config.port} monitoring ${config.connect.authentication.options.userName}@${config.connect.server}:${config.connect.options.port}`
  );
});

process.on("SIGINT", function () {
  server.close();
  process.exit(0);
});
