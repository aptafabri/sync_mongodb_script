const { MongoClient } = require("mongodb");

// Remote MongoDB Connection URI
const remoteUri = "mongodb://44.202.109.88:27017/";
const remoteClient = new MongoClient(remoteUri);

// Local MongoDB Connection URI
const localUri = "mongodb://localhost:27017/";
const localClient = new MongoClient(localUri);

// Function to fetch data from remote and insert into local
async function syncDatabase() {
    try {
        await remoteClient.connect();
        await localClient.connect();

        const remoteDb = remoteClient.db("hydratrade");
        const localDb = localClient.db("hydratrade");

        // Get all collections from remote DB
        const collections = await remoteDb.listCollections().toArray();

        for (const collectionInfo of collections) {
            const collectionName = collectionInfo.name;
            console.log(`Syncing collection: ${collectionName}`);

            const remoteCollection = remoteDb.collection(collectionName);
            const localCollection = localDb.collection(collectionName);

            // Fetch all records from the remote collection
            const documents = await remoteCollection.find({}).toArray();

            if (documents.length > 0) {
                // Insert into local DB, avoiding duplicates
                const operations = documents.map((doc) => ({
                    updateOne: {
                        filter: { _id: doc._id }, // Avoid duplicate `_id`
                        update: { $set: doc },
                        upsert: true,
                    },
                }));

                await localCollection.bulkWrite(operations);
                console.log(`✔ Synced ${documents.length} documents in ${collectionName}`);
            } else {
                console.log(`⚠ No documents found in ${collectionName}`);
            }
        }
    } catch (error) {
        console.error("Error syncing database:", error);
    } finally {
        await remoteClient.close();
        await localClient.close();
        console.log("🔄 Sync complete.");
    }
}

// Run the sync process
syncDatabase();