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

async function listTenDocuments() {
  try {
    const response = await databases.listDocuments(DATABASE_ID, COLLECTION_ID, [
      sdk.Query.limit(10),
      sdk.Query.select([
        "NPI",
        "Provider_First_Name",
        "Provider_Last_Name_Legal",
        "$id",
      ]),
    ]);

    if (response.total === 0) {
      console.log("No documents found in the collection.");
      return;
    }

    console.log(`Total documents in collection: ${response.total}`);
    console.log("\nListing 10 documents:");

    response.documents.forEach((doc, index) => {
      console.log(`\nDocument ${index + 1}:`);
      console.log(`NPI: ${doc.NPI}`);
      console.log(
        `Provider Name: ${doc.Provider_First_Name} ${doc.Provider_Last_Name_Legal}`
      );
      console.log(`Document ID: ${doc.$id}`);
    });
  } catch (error) {
    console.error("Error fetching documents:", error);
    if (error.response) {
      console.error("Error details:", error.response);
    }
  }
}

// Execute the listing
listTenDocuments();
