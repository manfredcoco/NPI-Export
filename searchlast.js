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

async function getLastTenDocuments() {
  try {
    // First, get only the IDs with minimal data to reduce load
    const response = await databases.listDocuments(DATABASE_ID, COLLECTION_ID, [
      sdk.Query.select(["$id", "$createdAt"]), // Select only necessary fields for sorting
      sdk.Query.orderDesc("$createdAt"), // Use the index
      sdk.Query.limit(10), // Limit to 10 results
      sdk.Query.offset(0), // Start from the beginning
    ]);

    if (response.total === 0) {
      console.log("No documents found in the collection.");
      return;
    }

    // Then fetch full details for these specific documents
    const documentIds = response.documents.map((doc) => doc.$id);
    const detailedDocs = await Promise.all(
      documentIds.map((id) =>
        databases.getDocument(DATABASE_ID, COLLECTION_ID, id)
      )
    );

    console.log(`Total documents in collection: ${response.total}`);
    console.log("\nLast 10 documents added:");

    detailedDocs.forEach((doc, index) => {
      console.log(`\nDocument ${index + 1}:`);
      console.log(`NPI: ${doc.NPI}`);
      console.log(
        `Provider Name: ${doc.Provider_First_Name} ${doc.Provider_Last_Name_Legal}`
      );
      console.log(`Created At: ${new Date(doc.$createdAt).toLocaleString()}`);
      console.log(`Document ID: ${doc.$id}`);
    });
  } catch (error) {
    console.error("Error fetching documents:", error);
    if (error.response) {
      console.error("Error details:", error.response);
    }
  }
}

// Execute the search
getLastTenDocuments();
