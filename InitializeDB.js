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

// Function to ensure the collection exists
async function ensureCollectionExists() {
  try {
    await databases.getCollection(DATABASE_ID, COLLECTION_ID);
    console.log("Collection exists.");
  } catch (error) {
    if (error.code === 404) {
      console.log("Collection does not exist. Creating NPIDATA...");
      await databases.createCollection(DATABASE_ID, COLLECTION_ID, "NPIDATA");
      console.log("Collection created successfully.");
    } else {
      throw error;
    }
  }
}

// Function to ensure the collection exists and add attributes
async function ensureCollectionAndAddAttributes() {
  try {
    await ensureCollectionExists(); // Ensure collection exists before adding attributes

    // Add string attributes to the collection
    const attributes = [
      { name: "NPI", type: "string", size: 128, required: true },
      {
        name: "Provider_Name_Prefix_Text",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Provider_First_Name",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Provider_Last_Name_Legal",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Endpoint_Type_Description",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Endpoint_Provider_Credential",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Mailing_Address_Telephone",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "Mailing_Address_Fax",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "Practice_Location_Telephone",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "Practice_Location_Fax",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "Authorized_Official_Telephone",
        type: "string",
        size: 20,
        required: false,
      },
      { name: "Taxonomy_Code_1", type: "string", size: 50, required: false },
      { name: "License_Number_1", type: "string", size: 50, required: false },
      {
        name: "License_State_Code_1",
        type: "string",
        size: 20,
        required: false,
      },
      { name: "Taxonomy_Code_2", type: "string", size: 50, required: false },
      { name: "License_Number_2", type: "string", size: 50, required: false },
      {
        name: "License_State_Code_2",
        type: "string",
        size: 20,
        required: false,
      },
      { name: "Taxonomy_Code_3", type: "string", size: 50, required: false },
      { name: "License_Number_3", type: "string", size: 50, required: false },
      {
        name: "License_State_Code_3",
        type: "string",
        size: 20,
        required: false,
      },
      { name: "Taxonomy_Code_4", type: "string", size: 50, required: false },
      { name: "License_Number_4", type: "string", size: 50, required: false },
      {
        name: "License_State_Code_4",
        type: "string",
        size: 20,
        required: false,
      },
      { name: "Taxonomy_Code_5", type: "string", size: 50, required: false },
      { name: "License_Number_5", type: "string", size: 50, required: false },
      {
        name: "License_State_Code_5",
        type: "string",
        size: 20,
        required: false,
      },
      { name: "Is_Sole_Proprietor", type: "string", size: 5, required: false },
      {
        name: "Is_Organization_Subpart",
        type: "string",
        size: 5,
        required: false,
      },
      { name: "Last_Update_Date", type: "string", size: 10, required: false },
      {
        name: "NPI_Deactivation_Date",
        type: "string",
        size: 10,
        required: false,
      },
      {
        name: "NPI_Reactivation_Date",
        type: "string",
        size: 10,
        required: false,
      },
      {
        name: "Provider_Gender_Code",
        type: "string",
        size: 1,
        required: false,
      },
      {
        name: "Organization_Name_Legal",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "First_Line_Mailing_Address",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Second_Line_Mailing_Address",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Practice_Location_City",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Practice_Location_State",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "Practice_Location_Postal",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "Practice_Location_Country",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "First_Line_Practice_Location",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Second_Line_Practice_Location",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Mailing_Address_City",
        type: "string",
        size: 128,
        required: false,
      },
      {
        name: "Mailing_Address_State",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "Mailing_Address_Postal",
        type: "string",
        size: 20,
        required: false,
      },
      {
        name: "Mailing_Address_Country",
        type: "string",
        size: 20,
        required: false,
      },
    ];

    await addAttributesWithRetry(attributes);

    // Verify attributes
    const missingAttributes = await verifyAttributes(attributes);
    if (missingAttributes.length > 0) {
      console.log(
        "Missing attributes detected. Deleting collection and retrying..."
      );
      await databases.deleteCollection(DATABASE_ID, COLLECTION_ID);
      await ensureCollectionAndAddAttributes(); // Retry
    } else {
      console.log("Database initialization completed successfully!");

      // Run addbaseNPI.js after successful initialization
      console.log("Starting NPI data import process...");
      require("./addbaseNPI.js");
    }
  } catch (error) {
    console.error("Error ensuring collection and adding attributes:", error);
    process.exit(1);
  }
}

// Function to add attributes with retry logic
async function addAttributesWithRetry(attributes) {
  for (const attr of attributes) {
    let success = false;
    let retries = 0;
    const maxRetries = 3;

    while (!success && retries < maxRetries) {
      try {
        await databases.createStringAttribute(
          DATABASE_ID,
          COLLECTION_ID,
          attr.name,
          attr.size,
          attr.required
        );
        console.log(`Attribute ${attr.name} added successfully`);
        success = true;
      } catch (error) {
        if (error.code === 409) {
          console.log(`Attribute ${attr.name} already exists, skipping...`);
          success = true;
        } else if (error.code === 404) {
          console.error(
            `Collection not found for attribute ${attr.name}, retrying...`
          );
          retries++;
          await delay(2000); // Wait 2 seconds before retrying
        } else if (error.code === 500) {
          console.error(
            `Server error adding attribute ${attr.name}, retrying in 2 seconds...`
          );
          await delay(2000); // Wait 2 seconds before retrying
        } else {
          console.error(`Error adding attribute ${attr.name}:`, error);
          success = true; // Exit loop on non-retryable errors
        }
      }
      await delay(100); // Add a 100ms delay between each attribute addition
    }

    if (!success) {
      console.error(
        `Failed to add attribute ${attr.name} after ${maxRetries} attempts.`
      );
      process.exit(1); // Exit if unable to add an attribute after retries
    }
  }
}

// Function to verify attributes
async function verifyAttributes(expectedAttributes) {
  const response = await databases.getCollection(DATABASE_ID, COLLECTION_ID);
  const existingAttributes = response.attributes.map((attr) => attr.key);
  return expectedAttributes.filter(
    (attr) => !existingAttributes.includes(attr.name)
  );
}

// Helper function to create a delay
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Call the function to ensure collection and add attributes
ensureCollectionAndAddAttributes();
