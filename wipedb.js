require("dotenv").config();
const sdk = require("node-appwrite");

// Initialize Appwrite client
const client = new sdk.Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT)
  .setProject(process.env.APPWRITE_PROJECT_ID)
  .setKey(process.env.APPWRITE_API_KEY);

const databases = new sdk.Databases(client);

const DATABASE_ID = "NPIDB";
const COLLECTION_ID = "NPIDATA";

// Constants for rate limiting and batch sizes
const BATCH_SIZE = 1000; // Number of documents to fetch per query
const DELETE_BATCH_SIZE = 60; // Number of deletions to process in parallel (rate limit: 60/minute)
const RATE_LIMIT_DELAY = 61000; // Delay between delete batches (61 seconds to be safe)

async function deleteAllDocuments() {
  try {
    let totalDeleted = 0;
    let hasMore = true;

    while (hasMore) {
      // Fetch a batch of documents
      const response = await databases.listDocuments(
        DATABASE_ID,
        COLLECTION_ID,
        [],
        BATCH_SIZE
      );

      const documents = response.documents;

      if (documents.length === 0) {
        console.log("No more documents to delete.");
        hasMore = false;
        break;
      }

      console.log(`Found ${documents.length} documents to process...`);

      // Process documents in smaller batches to respect rate limits
      for (let i = 0; i < documents.length; i += DELETE_BATCH_SIZE) {
        const batch = documents.slice(i, i + DELETE_BATCH_SIZE);

        console.log(`Deleting batch of ${batch.length} documents...`);

        // Delete the current batch
        const deletePromises = batch.map((doc) =>
          databases
            .deleteDocument(DATABASE_ID, COLLECTION_ID, doc.$id)
            .then(() => {
              totalDeleted++;
              return true;
            })
            .catch((error) => {
              console.error(`Failed to delete document ${doc.$id}:`, error);
              return false;
            })
        );

        // Wait for all deletions in the current batch to complete
        const results = await Promise.all(deletePromises);
        const successCount = results.filter((result) => result).length;

        console.log(
          `Successfully deleted ${successCount}/${batch.length} documents in this batch`
        );
        console.log(`Total documents deleted so far: ${totalDeleted}`);

        // If this isn't the last batch, wait for rate limit window
        if (i + DELETE_BATCH_SIZE < documents.length) {
          console.log(
            `Waiting ${RATE_LIMIT_DELAY / 1000} seconds for rate limit...`
          );
          await new Promise((resolve) => setTimeout(resolve, RATE_LIMIT_DELAY));
        }
      }
    }

    console.log(`Deletion complete. Total documents deleted: ${totalDeleted}`);
  } catch (error) {
    console.error("Error in deletion process:", error);
    process.exit(1);
  }
}

// Call the function to delete all documents
deleteAllDocuments();
