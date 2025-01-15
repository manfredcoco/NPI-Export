const fs = require("fs");
const path = require("path");
const https = require("https");
const StreamZip = require("node-stream-zip");
const sdk = require("node-appwrite");
require("dotenv").config();

// Initialize Appwrite client
const client = new sdk.Client()
  .setEndpoint(process.env.APPWRITE_ENDPOINT) // Your Appwrite Endpoint
  .setProject(process.env.APPWRITE_PROJECT_ID) // Your project ID
  .setKey(process.env.APPWRITE_API_KEY); // Your API key

const databases = new sdk.Databases(client);

// Constants for database and collection IDs
const DATABASE_ID = "NPIDB";
const COLLECTION_ID = "IsInitialized";

// Function to get the current month and year
function getCurrentMonthAndYear() {
  const date = new Date();
  const month = date.toLocaleString("default", { month: "long" });
  const year = date.getFullYear();
  return { month, year };
}

// Function to download a file
function downloadFile(url, dest, callback) {
  const file = fs.createWriteStream(dest);
  https
    .get(url, (response) => {
      response.pipe(file);
      file.on("finish", () => {
        file.close(callback);
      });
    })
    .on("error", (err) => {
      fs.unlink(dest, () => {}); // Delete the file if there's an error
      if (callback) callback(err.message);
    });
}

// Function to unzip a file using node-stream-zip
async function unzipFile(zipPath, extractTo) {
  const zip = new StreamZip.async({ file: zipPath });

  try {
    const entries = await zip.entries();
    const totalEntries = Object.keys(entries).length;
    let extractedEntries = 0;

    for (const entry of Object.values(entries)) {
      await zip.extract(entry.name, path.join(extractTo, entry.name));
      extractedEntries++;
      console.log(
        `Extracted ${extractedEntries}/${totalEntries}: ${entry.name}`
      );
    }

    console.log("File unzipped successfully");

    // After successful extraction, run InitializeDB.js
    console.log("Running InitializeDB.js...");
    require("./InitializeDB.js");
  } catch (err) {
    console.error("Error during extraction:", err);
  } finally {
    await zip.close();
  }
}

// Function to check and manage the extraction directory
function manageExtractionDirectory(extractDir) {
  if (fs.existsSync(extractDir)) {
    const files = fs.readdirSync(extractDir);
    if (files.length === 10) {
      console.log(
        "Directory exists and contains 10 files. Running InitializeDB.js..."
      );
      require("./InitializeDB.js"); // Run InitializeDB.js
      return true;
    } else {
      console.log(
        "Directory exists but does not contain 10 files. Deleting..."
      );
      fs.rmSync(extractDir, { recursive: true, force: true });
    }
  }
  fs.mkdirSync(extractDir);
  return false;
}

// Function to ensure the database and collection exist
async function ensureDatabaseAndCollection() {
  try {
    // Check if the database exists
    let databaseExists = false;
    try {
      await databases.get(DATABASE_ID);
      databaseExists = true;
    } catch (error) {
      if (error.code === 404) {
        console.log("Database does not exist. Creating NPIDB...");
        await databases.create(DATABASE_ID, "NPIDB");
      } else {
        throw error;
      }
    }

    // Check if the collection exists
    let collectionExists = false;
    try {
      await databases.getCollection(DATABASE_ID, COLLECTION_ID);
      collectionExists = true;
    } catch (error) {
      if (error.code === 404) {
        console.log("Collection does not exist. Creating IsInitialized...");
        await databases.createCollection(
          DATABASE_ID,
          COLLECTION_ID,
          "IsInitialized"
        );
        await databases.createBooleanAttribute(
          DATABASE_ID,
          COLLECTION_ID,
          "IsInitialized",
          false
        );
      } else {
        throw error;
      }
    }
  } catch (error) {
    console.error("Error ensuring database and collection:", error);
    process.exit(1);
  }
}

// Function to check if the IsInitialized collection has a document with IsInitialized set to true
async function checkInitialization() {
  try {
    const response = await databases.listDocuments(DATABASE_ID, COLLECTION_ID, [
      sdk.Query.equal("IsInitialized", true),
    ]);

    if (response.documents.length > 0) {
      console.log(
        "Initialization already completed. Running weeklyappend.js..."
      );
      require("./weeklyappend.js");
      return true;
    }
  } catch (error) {
    console.error("Error checking initialization:", error);
  }
  return false;
}

// Main function to check, download, and unzip the file
async function checkAndDownloadFile() {
  await ensureDatabaseAndCollection();

  const isInitialized = await checkInitialization();
  if (isInitialized) return;

  const { month, year } = getCurrentMonthAndYear();
  const fileName = `NPPES_Data_Dissemination_${month}_${year}.zip`;
  const filePath = path.join(__dirname, "downloads", fileName);
  const url = `https://download.cms.gov/nppes/${fileName}`;
  const extractDir = path.join(
    __dirname,
    "downloads",
    `NPPES_Data_Dissemination_${month}_${year}`
  );

  // Manage the extraction directory
  const shouldContinue = manageExtractionDirectory(extractDir);
  if (shouldContinue) return;

  // Check if the file exists
  if (fs.existsSync(filePath)) {
    console.log("File already exists:", filePath);
    await unzipFile(filePath, extractDir);
  } else {
    console.log("File does not exist. Downloading:", url);
    downloadFile(url, filePath, async (error) => {
      if (error) {
        console.error("Error downloading file:", error);
      } else {
        console.log("File downloaded successfully:", filePath);
        await unzipFile(filePath, extractDir);
      }
    });
  }
}

// Ensure the downloads directory exists
const downloadsDir = path.join(__dirname, "downloads");
if (!fs.existsSync(downloadsDir)) {
  fs.mkdirSync(downloadsDir);
}

// Execute the main function
checkAndDownloadFile();
