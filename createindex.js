require("dotenv").config();
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

async function createCreatedAtIndex() {
  try {
    console.log("Creating index for $createdAt...");

    await databases.createIndex(
      DATABASE_ID,
      COLLECTION_ID,
      "createdAt_desc", // Index key
      sdk.IndexType.Key, // Index type using SDK constant
      ["$createdAt"], // Attributes to index
      ["desc"] // Orders as array
    );

    console.log("Index created successfully!");
  } catch (error) {
    if (error.code === 409) {
      console.log("Index already exists.");
    } else {
      console.error("Error creating index:", error);
      console.error("Error details:", error.response);
    }
  }
}

// Create the index
createCreatedAtIndex();
