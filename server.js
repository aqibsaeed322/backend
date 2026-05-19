import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import Redis from 'ioredis';
import cors from 'cors';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const server = createServer(app);
const io = new Server(server, {
  cors: {
    origin: process.env.FRONTEND_URL || "http://localhost:5173",
    credentials: true
  },
  transports: ['websocket', 'polling'] // Important for production
});

// Initialize Redis (will use localhost if no URL provided)
let redis;
try {
  redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
  console.log('Redis connected successfully');
} catch (error) {
  console.error('Redis connection error:', error);
  // Fallback to in-memory if Redis fails
  console.log('Using in-memory fallback');
  redis = null;
}

// In-memory fallback (if Redis is not available)
const memoryQueues = {
  text: [],
  video: []
};
const memoryPartners = new Map(); // socketId -> partnerId

// Queues
const TEXT_QUEUE = 'waiting_users_text';
const VIDEO_QUEUE = 'waiting_users_video';

// Store current partner for each socket
const userPartners = new Map(); // socket.id -> partner socket.id
const userChatTypes = new Map(); // socket.id -> 'text' or 'video'

io.on('connection', (socket) => {
  console.log(`🟢 User connected: ${socket.id}`);
  console.log(`📊 Total connections: ${io.engine.clientsCount}`);

  // Start Text Chat
  socket.on('text:start', async () => {
    console.log(`📝 Text chat started: ${socket.id}`);
    userChatTypes.set(socket.id, 'text');
    
    if (redis) {
      await redis.rpush(TEXT_QUEUE, socket.id);
      await matchUsersWithRedis('text');
    } else {
      // In-memory fallback
      memoryQueues.text.push(socket.id);
      matchUsersWithMemory('text');
    }
  });

  // Start Video Chat
  socket.on('video:start', async () => {
    console.log(`🎥 Video chat started: ${socket.id}`);
    userChatTypes.set(socket.id, 'video');
    
    if (redis) {
      await redis.rpush(VIDEO_QUEUE, socket.id);
      await matchUsersWithRedis('video');
    } else {
      memoryQueues.video.push(socket.id);
      matchUsersWithMemory('video');
    }
  });

  // WebRTC Signaling
  socket.on('video:offer', (data) => {
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      console.log(`📤 Forwarding offer from ${socket.id} to ${partnerId}`);
      io.to(partnerId).emit('video:offer', {
        sdp: data.sdp,
        from: socket.id
      });
    }
  });

  socket.on('video:answer', (data) => {
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      console.log(`📤 Forwarding answer from ${socket.id} to ${partnerId}`);
      io.to(partnerId).emit('video:answer', {
        sdp: data.sdp,
        from: socket.id
      });
    }
  });

  socket.on('video:ice-candidate', (data) => {
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('video:ice-candidate', {
        candidate: data.candidate,
        from: socket.id
      });
    }
  });

  socket.on('video:toggle-mic', (isMuted) => {
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('video:mic-status', isMuted);
    }
  });

  socket.on('video:toggle-camera', (isOff) => {
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('video:camera-status', isOff);
    }
  });

  // Chat message
  socket.on('chat:message', (message) => {
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      console.log(`💬 Message from ${socket.id} to ${partnerId}: ${message}`);
      io.to(partnerId).emit('chat:message', message);
    }
  });

  // Partner location
  socket.on('partner:location', (location) => {
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('partner:location', location);
    }
  });

  // Typing indicator
  socket.on('typing', (isTyping) => {
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('typing', isTyping);
    }
  });

  // Skip / Next
  socket.on('next', async () => {
    console.log(`⏭️ Next request from ${socket.id}`);
    
    // Disconnect current partner
    const currentPartner = userPartners.get(socket.id);
    if (currentPartner) {
      const partnerSocket = io.sockets.sockets.get(currentPartner);
      if (partnerSocket) {
        partnerSocket.emit('partner:disconnected');
        userPartners.delete(currentPartner);
      }
      userPartners.delete(socket.id);
    }
    
    const chatType = userChatTypes.get(socket.id);
    
    if (redis) {
      if (chatType === 'text') {
        await redis.rpush(TEXT_QUEUE, socket.id);
        await matchUsersWithRedis('text');
      } else if (chatType === 'video') {
        await redis.rpush(VIDEO_QUEUE, socket.id);
        await matchUsersWithRedis('video');
      }
    } else {
      if (chatType === 'text') {
        memoryQueues.text.push(socket.id);
        matchUsersWithMemory('text');
      } else if (chatType === 'video') {
        memoryQueues.video.push(socket.id);
        matchUsersWithMemory('video');
      }
    }
  });

  // Disconnect
  socket.on('disconnect', async () => {
    console.log(`🔴 User disconnected: ${socket.id}`);
    
    // Notify partner
    const partnerId = userPartners.get(socket.id);
    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('partner:disconnected');
        userPartners.delete(partnerId);
      }
      userPartners.delete(socket.id);
    }
    
    // Remove from queues
    if (redis) {
      await redis.lrem(TEXT_QUEUE, 0, socket.id);
      await redis.lrem(VIDEO_QUEUE, 0, socket.id);
      await redis.del(`partner:${socket.id}`);
    } else {
      const textIndex = memoryQueues.text.indexOf(socket.id);
      if (textIndex !== -1) memoryQueues.text.splice(textIndex, 1);
      
      const videoIndex = memoryQueues.video.indexOf(socket.id);
      if (videoIndex !== -1) memoryQueues.video.splice(videoIndex, 1);
      
      memoryPartners.delete(socket.id);
    }
    
    userChatTypes.delete(socket.id);
  });
});

// Redis-based matching
async function matchUsersWithRedis(type) {
  const queue = type === 'text' ? TEXT_QUEUE : VIDEO_QUEUE;
  const queueLength = await redis.llen(queue);
  
  console.log(`🔄 Matching ${type} users. Queue size: ${queueLength}`);
  
  while (queueLength >= 2) {
    const user1 = await redis.lpop(queue);
    const user2 = await redis.lpop(queue);
    
    if (user1 && user2) {
      const socket1 = io.sockets.sockets.get(user1);
      const socket2 = io.sockets.sockets.get(user2);
      
      if (socket1 && socket2 && socket1.connected && socket2.connected) {
        console.log(`✅ Matched ${user1} with ${user2}`);
        
        // Store partners
        userPartners.set(user1, user2);
        userPartners.set(user2, user1);
        
        await redis.set(`partner:${user1}`, user2);
        await redis.set(`partner:${user2}`, user1);
        
        // Notify both users
        socket1.emit('chat:start', { status: 'connected', partnerId: user2, type });
        socket2.emit('chat:start', { status: 'connected', partnerId: user1, type });
        
        if (type === 'video') {
          socket1.emit('video:initiate', { partnerId: user2 });
          socket2.emit('video:initiate', { partnerId: user1 });
        }
      } else {
        console.log(`❌ Failed to pair - one user disconnected`);
        if (socket1 && socket1.connected) await redis.rpush(queue, user1);
        if (socket2 && socket2.connected) await redis.rpush(queue, user2);
      }
    }
  }
}

// In-memory matching (fallback when Redis is not available)
function matchUsersWithMemory(type) {
  const queue = type === 'text' ? memoryQueues.text : memoryQueues.video;
  
  console.log(`🔄 (Memory) Matching ${type} users. Queue size: ${queue.length}`);
  
  while (queue.length >= 2) {
    const user1 = queue.shift();
    const user2 = queue.shift();
    
    if (user1 && user2) {
      const socket1 = io.sockets.sockets.get(user1);
      const socket2 = io.sockets.sockets.get(user2);
      
      if (socket1 && socket2 && socket1.connected && socket2.connected) {
        console.log(`✅ (Memory) Matched ${user1} with ${user2}`);
        
        userPartners.set(user1, user2);
        userPartners.set(user2, user1);
        
        socket1.emit('chat:start', { status: 'connected', partnerId: user2, type });
        socket2.emit('chat:start', { status: 'connected', partnerId: user1, type });
        
        if (type === 'video') {
          socket1.emit('video:initiate', { partnerId: user2 });
          socket2.emit('video:initiate', { partnerId: user1 });
        }
      } else {
        console.log(`❌ (Memory) Failed to pair - one user disconnected`);
        if (socket1 && socket1.connected) queue.unshift(user1);
        if (socket2 && socket2.connected) queue.unshift(user2);
      }
    }
  }
}

function filterBadWords(message) {
  const badWords = ['fuck', 'shit', 'asshole', 'bitch', 'damn'];
  let filtered = message;
  badWords.forEach(word => {
    const regex = new RegExp(word, 'gi');
    filtered = filtered.replace(regex, '***');
  });
  return filtered;
}

// Health check endpoint with detailed stats
app.get('/health', (req, res) => {
  const stats = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    connections: io.engine.clientsCount,
    activePartners: userPartners.size / 2,
    redisConnected: redis !== null
  };
  
  if (redis) {
    stats.redisAvailable = true;
  } else {
    stats.redisAvailable = false;
    stats.memoryQueues = {
      text: memoryQueues.text.length,
      video: memoryQueues.video.length
    };
  }
  
  res.json(stats);
});

// Debug endpoint to see queue status
app.get('/debug', async (req, res) => {
  const result = {
    connections: io.engine.clientsCount,
    activePartners: Array.from(userPartners.keys()),
    userChatTypes: Array.from(userChatTypes.entries())
  };
  
  if (redis) {
    const textQueue = await redis.lrange(TEXT_QUEUE, 0, -1);
    const videoQueue = await redis.lrange(VIDEO_QUEUE, 0, -1);
    result.queues = { text: textQueue, video: videoQueue };
  } else {
    result.queues = { text: memoryQueues.text, video: memoryQueues.video };
  }
  
  res.json(result);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📡 WebSocket server ready`);
  console.log(`🔗 Frontend URL: ${process.env.FRONTEND_URL || 'http://localhost:5173'}`);
});