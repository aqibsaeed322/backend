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
  }
});

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

// Queues
const TEXT_QUEUE = 'waiting_users_text';
const VIDEO_QUEUE = 'waiting_users_video';

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);
  let currentPartner = null;
  let chatType = null;

  // Start Text Chat
  socket.on('text:start', async () => {
    chatType = 'text';
    await redis.rpush(TEXT_QUEUE, socket.id);
    await matchUsers('text');
  });

  // Start Video Chat
  socket.on('video:start', async () => {
    chatType = 'video';
    await redis.rpush(VIDEO_QUEUE, socket.id);
    await matchUsers('video');
  });

  // WebRTC Signaling
  socket.on('video:offer', (data) => {
    if (currentPartner) {
      io.to(currentPartner).emit('video:offer', {
        sdp: data.sdp,
        from: socket.id
      });
    }
  });

  socket.on('video:answer', (data) => {
    if (currentPartner) {
      io.to(currentPartner).emit('video:answer', {
        sdp: data.sdp,
        from: socket.id
      });
    }
  });

  socket.on('video:ice-candidate', (data) => {
    if (currentPartner) {
      io.to(currentPartner).emit('video:ice-candidate', {
        candidate: data.candidate,
        from: socket.id
      });
    }
  });

  socket.on('video:toggle-mic', (isMuted) => {
    if (currentPartner) {
      io.to(currentPartner).emit('video:mic-status', isMuted);
    }
  });

  socket.on('video:toggle-camera', (isOff) => {
    if (currentPartner) {
      io.to(currentPartner).emit('video:camera-status', isOff);
    }
  });

  // Text Chat Events
  socket.on('message', (data) => {
    if (currentPartner && chatType === 'text') {
      const filteredMessage = filterBadWords(data.message);
      io.to(currentPartner).emit('message', {
        message: filteredMessage,
        sender: 'partner',
        timestamp: Date.now()
      });
    }
  });

  socket.on('typing', (isTyping) => {
    if (currentPartner && chatType === 'text') {
      io.to(currentPartner).emit('typing', isTyping);
    }
  });

  // Skip / Next
  socket.on('next', async () => {
    await disconnectPartner(socket.id);
    if (chatType === 'text') {
      await redis.rpush(TEXT_QUEUE, socket.id);
      await matchUsers('text');
    } else if (chatType === 'video') {
      await redis.rpush(VIDEO_QUEUE, socket.id);
      await matchUsers('video');
    }
  });

  // Disconnect
  socket.on('disconnect', async () => {
    await disconnectPartner(socket.id);
    await removeFromQueue(socket.id);
  });
});

async function matchUsers(type) {
  const queue = type === 'text' ? TEXT_QUEUE : VIDEO_QUEUE;
  const queueLength = await redis.llen(queue);
  
  if (queueLength >= 2) {
    const user1 = await redis.lpop(queue);
    const user2 = await redis.lpop(queue);
    
    if (user1 && user2) {
      const socket1 = io.sockets.sockets.get(user1);
      const socket2 = io.sockets.sockets.get(user2);
      
      if (socket1 && socket2) {
        socket1.currentPartner = user2;
        socket2.currentPartner = user1;
        socket1.chatType = type;
        socket2.chatType = type;
        
        await redis.set(`partner:${user1}`, user2);
        await redis.set(`partner:${user2}`, user1);
        
        socket1.emit('chat:start', { status: 'connected', partnerId: user2, type });
        socket2.emit('chat:start', { status: 'connected', partnerId: user1, type });
        
        if (type === 'video') {
          socket1.emit('video:initiate', { partnerId: user2 });
          socket2.emit('video:initiate', { partnerId: user1 });
        }
      } else {
        if (socket1) await redis.rpush(queue, user1);
        if (socket2) await redis.rpush(queue, user2);
      }
    }
  }
}

async function disconnectPartner(userId) {
  const partnerId = await redis.get(`partner:${userId}`);
  if (partnerId) {
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) {
      partner.emit('partner:disconnected');
      partner.currentPartner = null;
    }
    await redis.del(`partner:${userId}`);
    await redis.del(`partner:${partnerId}`);
  }
}

async function removeFromQueue(userId) {
  await redis.lrem(TEXT_QUEUE, 0, userId);
  await redis.lrem(VIDEO_QUEUE, 0, userId);
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

app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});