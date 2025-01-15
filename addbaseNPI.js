require("dotenv").config();
const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const sdk = require("node-appwrite");

// Initialize Appwrite client
const client = new sdk.Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT)
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const databases = new sdk.Databases(client);

// Constants
const DATABASE_ID = "NPIDB";
const COLLECTION_ID = "NPIDATA";
const CREATE_BATCH_SIZE = 500; // Number of documents per batch
const CHUNK_SIZE = 50; // Number of documents sent in parallel within a batch
const CHUNK_DELAY = 200; // Delay between chunks (in milliseconds)
const MAX_PARALLEL_BATCHES = 10; // Number of batches processed concurrently

// Load attribute dictionary
const attributeDictionary = require("./attributedictionary.json");

// Cache for NPIs to prevent duplicates
const processedNPIs = new Set();

// Function to initialize NPI cache
async function initializeNPICache() {
  let hasMore = true;
  let lastId = null;

  while (hasMore) {
    const response = await databases.listDocuments(
      DATABASE_ID,
      COLLECTION_ID,
      lastId ? [sdk.Query.cursorAfter(lastId)] : [],
      CREATE_BATCH_SIZE
    );

    if (response.documents.length === 0) {
      hasMore = false;
      break;
    }

    response.documents.forEach((doc) => processedNPIs.add(doc.NPI));
    lastId = response.documents[response.documents.length - 1].$id;
    console.log(`Cached ${processedNPIs.size} existing NPIs...`);
  }
}

// Function to send a batch of documents with chunked parallelism
async function sendBatch(documents) {
  try {
    const chunks = [];
    for (let i = 0; i < documents.length; i += CHUNK_SIZE) {
      chunks.push(documents.slice(i, i + CHUNK_SIZE));
    }

    for (const chunk of chunks) {
      const promises = chunk.map((document) =>
        databases.createDocument(
          DATABASE_ID,
          COLLECTION_ID,
          sdk.ID.unique(),
          document
        )
      );

      await Promise.all(promises); // Send the chunk in parallel
      console.log(`Chunk of ${chunk.length} documents added successfully.`);

      // Introduce delay between chunks
      await new Promise((resolve) => setTimeout(resolve, CHUNK_DELAY));
    }
  } catch (error) {
    console.error("Error adding documents:", error);
    console.log("Continuing with next batch...");
  }
}

// Function to process CSV and add documents with parallel workers
async function processCSVAndAddDocuments() {
  try {
    console.log("Initializing NPI cache...");
    await initializeNPICache();
    console.log(`Cached ${processedNPIs.size} existing NPIs`);

    const { month, year } = getCurrentMonthAndYear();
    const csvFilePath = path.join(
      __dirname,
      "downloads",
      `NPPES_Data_Dissemination_${month}_${year}`,
      "npidata_pfile_20050523-20250112.csv"
    );

    if (!fs.existsSync(csvFilePath)) {
      console.error("CSV file does not exist:", csvFilePath);
      return;
    }

    const documents = [];
    let totalProcessed = 0;
    let totalAdded = 0;
    let totalSkipped = 0;

    await new Promise((resolve, reject) => {
      fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on("data", (row) => {
          totalProcessed++;

          if (processedNPIs.has(row.NPI)) {
            totalSkipped++;
            return;
          }

          const document = {};
          for (const [csvHeader, attributeName] of Object.entries(
            attributeDictionary
          )) {
            if (row[csvHeader] !== undefined) {
              document[attributeName] = row[csvHeader] || null;
            }
          }

          documents.push(document);
          processedNPIs.add(row.NPI);

          if (documents.length >= CREATE_BATCH_SIZE * MAX_PARALLEL_BATCHES) {
            const batches = [];
            for (let i = 0; i < MAX_PARALLEL_BATCHES; i++) {
              const batchDocs = documents.splice(0, CREATE_BATCH_SIZE);
              if (batchDocs.length > 0) batches.push(sendBatch(batchDocs));
            }
            Promise.all(batches).then(() =>
              console.log(
                `Processed ${MAX_PARALLEL_BATCHES} batches concurrently.`
              )
            );
            totalAdded += CREATE_BATCH_SIZE * MAX_PARALLEL_BATCHES;
          }

          if (totalProcessed % 10000 === 0) {
            console.log(
              `Processed: ${totalProcessed}, Added: ${totalAdded}, Skipped: ${totalSkipped}`
            );
          }
        })
        .on("end", async () => {
          while (documents.length > 0) {
            const batches = [];
            for (let i = 0; i < MAX_PARALLEL_BATCHES; i++) {
              const batchDocs = documents.splice(0, CREATE_BATCH_SIZE);
              if (batchDocs.length > 0) batches.push(sendBatch(batchDocs));
            }
            await Promise.all(batches);
            totalAdded += batches.length * CREATE_BATCH_SIZE;
          }
          console.log(
            `Processing completed. Total processed: ${totalProcessed}, Added: ${totalAdded}, Skipped: ${totalSkipped}`
          );
          resolve();
        })
        .on("error", (error) => {
          console.error("Error reading CSV file:", error);
          reject(error);
        });
    });
  } catch (error) {
    console.error("Error in processing:", error);
    process.exit(1);
  }
}

// Function to get the current month and year
function getCurrentMonthAndYear() {
  const date = new Date();
  const month = date.toLocaleString("default", { month: "long" });
  const year = date.getFullYear();
  return { month, year };
}

// Start processing
processCSVAndAddDocuments();
