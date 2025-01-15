require("dotenv").config();
const sdk = require("node-appwrite");

// Initialize Appwrite client
const client = new sdk.Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT) // Your Appwrite Endpoint
  .setProject(process.env.APPWRITE_PROJECT_ID) // Your project ID
  .setKey(process.env.APPWRITE_API_KEY); // Your API key

const databases = new sdk.Databases(client);

// Constants for database and collection IDs
const DATABASE_ID = "NPIDB";
const COLLECTION_ID = "NPIDATA";

// Function to delete specified attributes
async function deleteAttributes() {
  const attributesToDelete = ["License_State_Code_2", "License_State_Code_5"];

  for (const attr of attributesToDelete) {
    try {
      await databases.deleteAttribute(DATABASE_ID, COLLECTION_ID, attr);
      console.log(`Attribute ${attr} deleted successfully.`);
    } catch (error) {
      console.error(`Error deleting attribute ${attr}:`, error);
    }
  }
}

// Call the function to delete attributes
deleteAttributes();
