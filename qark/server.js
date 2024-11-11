import baileys from '@whiskeysockets/baileys';
import { MongoClient } from 'mongodb';
import { Client as OpenSearchClient } from '@opensearch-project/opensearch';

import P from 'pino'
const logger = P({ timestamp: () => `,"time":"${new Date().toJSON()}"` }, P.destination('./wa-logs.txt'))

const { default: DisconnectReason, makeWASocket, useSingleFileAuthState, useMultiFileAuthState, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, WAMessageKey } = baileys;

// const { state, saveState } = useSingleFileAuthState('./credentials/auth_info.json');
const { state, saveCreds } = await useMultiFileAuthState('./qark/credentials')
const opensearchClient = new OpenSearchClient({ 
    node: 'http://localhost:9200',
    auth: {
        username: 'admin',
        password: 'admin'
    },
    timeout: 60000,
});
const mongoClient = new MongoClient('mongodb://localhost:27017');
await mongoClient.connect();
const userDB = mongoClient.db('authDB').collection('users');


const store = undefined
store?.readFromFile('./baileys_store_multi.json')
// save every 10s
setInterval(() => {
	store?.writeToFile('./baileys_store_multi.json')
}, 10_000)

// User Authentication Function
async function authenticateUser(username, password) {
    const user = await userDB.findOne({ username, password });
    return !!user;
}

// Initialize Baileys and Connect to OpenSearch
async function connectWhatsApp(username, password) {
    if (!await authenticateUser(username, password)) {
        // throw new Error('Authentication failed');
        console.log('Authentication failed');
    }

	// fetch latest version of WA Web
	const { version, isLatest } = await fetchLatestBaileysVersion()
	console.log(`using WA v${version.join('.')}, isLatest: ${isLatest}`)

	const sock = makeWASocket({
		version,
		logger,
		printQRInTerminal: true,
		auth: {
			creds: state.creds,
			/** caching makes the store faster to send/recv messages */
			keys: makeCacheableSignalKeyStore(state.keys, logger),
		},
        // msgRetryCounterCache,
		generateHighQualityLinkPreview: true,
		// ignore all broadcast messages -- to receive the same
		// comment the line below out
		// shouldIgnoreJid: jid => isJidBroadcast(jid),
		// implement to handle retries & poll updates
		getMessage,
	})

    store?.bind(sock.ev)

    console.log('Sock Events:', sock.ev);

    // received new messages
    // sock.ev.on('messages.upsert', async ({ messages }) => {
    //     for (const msg of messages) {
    //         if (msg.message?.conversation) {
    //             const content = msg.message.conversation;
    //             const timestamp = new Date();
                
    //             await processMessage(msg.key.remoteJid, content, timestamp);
    //         }
    //     }
    // });
    
    // Wrap the async function in a synchronous handler
    // sock.ev.on('creds.update', async () => {
    //     await saveCreds();
    // });

    sock.ev.process(
		// events is a map for event name => event data
		async(events) => {
 			// credentials updated -- save them
			if(events['creds.update']) {
				await saveCreds()
			}


            if(events['connection.update']) {
				const update = events['connection.update']
				const { connection, lastDisconnect } = update
				if(connection === 'close') {
					// reconnect if not logged out
					if(lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut) {
						connectWhatsApp()
					} else {
						console.log('Connection closed. You are logged out.')
					}
				}

				console.log('connection update', update)
			}

            if(events['messages.upsert']) {
                const upsert = events['messages.upsert']
                console.log('recv messages ', JSON.stringify(upsert, undefined, 2))
                for (const msg of upsert.messages) {
                    if (msg.message?.conversation) {
                        const content = msg.message.conversation;
                        const timestamp = new Date();
                        
                        await processMessage(msg.key.remoteJid, msg.key.id, content, timestamp);
                    }
                }
            }
        }
    )
}

async function processMessage(chatId, messageId, content, timestamp) {
    // Define the chunking logic based on size and time constraints
    console.log('processing message: ', chatId, messageId, content, timestamp);
    const chunk = await getOrCreateChunk(chatId, messageId, content, timestamp);
    console.log('chunk', chunk);
    await opensearchClient.index({
        index: 'whatsapp_messages',
        body: {
            chat_id: chatId,
            message_id: messageId,
            chunk_id: chunk._id,
            timestamp: timestamp,
            content: content
        }
    });
}


import { ObjectId } from 'mongodb';

const CHUNK_SIZE_LIMIT = 50; // Maximum messages per chunk
const TIME_THRESHOLD = 3 * 24 * 60 * 60 * 1000; // 3 days in milliseconds

// create mongdo db and its collections if doesn't exist
const db = mongoClient.db('whatsapp');

async function createCollectionsIfNotExist() {
  // Await the list of collections
  const collections = await db.listCollections().toArray();
  const collectionNames = collections.map(c => c.name);

  // Check if 'chunks' collection exists, if not, create it
  if (!collectionNames.includes('chunks')) {
    await db.createCollection('chunks');
    console.log('Collection "chunks" created.');
  }

  // Check if 'messages' collection exists, if not, create it
  if (!collectionNames.includes('messages')) {
    await db.createCollection('messages');
    console.log('Collection "messages" created.');
  }
}

await createCollectionsIfNotExist();

const chunksCollection = mongoClient.db('whatsapp').collection('chunks');
const messagesCollection = mongoClient.db('whatsapp').collection('messages');

// Function to get or create a chunk based on chat ID, message content, and timestamp
async function getOrCreateChunk(chatId, messageId, content, timestamp) {
    const currentTime = timestamp.getTime();

    // 1. Try to find an existing chunk for the chat that can still accept more messages
    let chunk = await chunksCollection.findOne({
        chat_id: chatId,
        message_count: { $lt: CHUNK_SIZE_LIMIT },
        last_message_timestamp: { $gte: currentTime - TIME_THRESHOLD }
    });

    // 2. If a suitable chunk exists, update it with the new message information
    if (chunk) {
        // Add the message to the `messages` collection
        await messagesCollection.insertOne({
            _id: messageId,
            chat_id: chatId,
            chunk_id: chunk._id,
            timestamp,
            content
        });

        // Update the chunk to include this new message
        await chunksCollection.updateOne(
            { _id: chunk._id },
            {
                $push: { message_ids: messageId },
                $set: { last_message_timestamp: currentTime },
                $inc: { message_count: 1 }
            }
        );

        return chunk;
    }

    // 3. If no suitable chunk exists, create a new chunk
    const newChunk = {
        chat_id: chatId,
        created_at: currentTime,
        last_message_timestamp: currentTime,
        message_count: 1,
        message_ids: [],
        embedding: null // Placeholder for embedding, can be set later
    };

    // Insert the new chunk into the `chunks` collection
    const { insertedId } = await chunksCollection.insertOne(newChunk);

    // Add the message to the `messages` collection and link it to the new chunk
    await messagesCollection.insertOne({
        _id: messageId,
        chat_id: chatId,
        chunk_id: insertedId,
        timestamp,
        content
    });

    // Update the chunk with the initial message's ID
    await chunksCollection.updateOne(
        { _id: insertedId },
        { $push: { message_ids: messageId } }
    );

    return { ...newChunk, _id: insertedId };
}


async function getMessage(key) {
    if(store) {
        const msg = await store.loadMessage(key.remoteJid, key.id)
        return msg?.message || undefined
    }

    // only if store is present
    return proto.Message.fromObject({})
}

connectWhatsApp('saurabh', 'saurabh');

