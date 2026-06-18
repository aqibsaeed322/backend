import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
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
  transports: ['websocket', 'polling']
});

// Simple in-memory queues (No Redis needed)
const videoQueue = []; // Users waiting for video chat
const textQueue = [];  // Users waiting for text chat
const activePartners = new Map(); // socketId -> partnerId
const userModes = new Map();      // socketId -> 'text' | 'video'

// Helper to remove socket from all queues
function removeFromQueues(socketId) {
  const vIndex = videoQueue.indexOf(socketId);
  if (vIndex !== -1) videoQueue.splice(vIndex, 1);
  const tIndex = textQueue.indexOf(socketId);
  if (tIndex !== -1) textQueue.splice(tIndex, 1);
}

io.on('connection', (socket) => {
  console.log(`🟢 User connected: ${socket.id}`);
  console.log(`📊 Total users: ${io.engine.clientsCount}`);

  // Start Video Chat
  socket.on('video:start', () => {
    console.log(`🎥 Video chat started: ${socket.id}`);
    userModes.set(socket.id, 'video');
    removeFromQueues(socket.id);
    
    // Add to video queue
    videoQueue.push(socket.id);
    console.log(`📋 Video queue size: ${videoQueue.length}`);
    
    // Try to match
    matchVideoUsers();
  });

  // Start Text Chat
  socket.on('text:start', () => {
    console.log(`📝 Text chat started: ${socket.id}`);
    userModes.set(socket.id, 'text');
    removeFromQueues(socket.id);
    
    // Add to text queue
    textQueue.push(socket.id);
    console.log(`📋 Text queue size: ${textQueue.length}`);
    
    // Try to match
    matchTextUsers();
  });

  // WebRTC Signaling
  socket.on('video:offer', (data) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      console.log(`📤 Offer: ${socket.id} -> ${partnerId}`);
      io.to(partnerId).emit('video:offer', {
        sdp: data.sdp,
        from: socket.id
      });
    }
  });

  socket.on('video:answer', (data) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      console.log(`📤 Answer: ${socket.id} -> ${partnerId}`);
      io.to(partnerId).emit('video:answer', {
        sdp: data.sdp,
        from: socket.id
      });
    }
  });

  socket.on('video:ice-candidate', (data) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('video:ice-candidate', {
        candidate: data.candidate,
        from: socket.id
      });
    }
  });

  socket.on('video:toggle-mic', (isMuted) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('video:mic-status', isMuted);
    }
  });

  socket.on('video:toggle-camera', (isOff) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('video:camera-status', isOff);
    }
  });

  // Chat message (Video chat format)
  socket.on('chat:message', (message) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      console.log(`💬 Video Message: ${socket.id} -> ${partnerId}`);
      io.to(partnerId).emit('chat:message', message);
    }
  });

  // Message (Text chat format)
  socket.on('message', (data) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      console.log(`💬 Text Message: ${socket.id} -> ${partnerId}`);
      // Format text message as expected by TextChat.jsx client listener
      io.to(partnerId).emit('message', {
        message: data.message,
        sender: 'stranger',
        timestamp: Date.now()
      });
    }
  });

  // Partner location
  socket.on('partner:location', (location) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('partner:location', location);
    }
  });

  // Typing indicator
  socket.on('typing', (isTyping) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit('typing', isTyping);
    }
  });

  // Heart Spark effect
  socket.on('spark:heart', () => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      console.log(`💖 Heart Spark: ${socket.id} -> ${partnerId}`);
      io.to(partnerId).emit('spark:heart');
    }
  });

  // Skip / Next
  socket.on('next', () => {
    console.log(`⏭️ Next: ${socket.id}`);
    
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('partner:disconnected');
        activePartners.delete(partnerId);
      }
      activePartners.delete(socket.id);
    }
    
    removeFromQueues(socket.id);

    // Add back to queue based on mode
    const mode = userModes.get(socket.id);
    if (mode === 'text') {
      textQueue.push(socket.id);
      matchTextUsers();
    } else {
      userModes.set(socket.id, 'video'); // Default fallback
      videoQueue.push(socket.id);
      matchVideoUsers();
    }
  });

  // Handle User Reporting (Abuse/Violation Flagging)
  socket.on('report:user', (data) => {
    const { reason } = data;
    const partnerId = activePartners.get(socket.id);
    console.log(`⚠️ Abuse Report Filed! Reporter: ${socket.id} -> Offender: ${partnerId || 'N/A'}. Reason: ${reason}`);
    
    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('partner:disconnected');
        activePartners.delete(partnerId);
      }
      activePartners.delete(socket.id);
    }
  });


  // Disconnect
  socket.on('disconnect', () => {
    console.log(`🔴 Disconnected: ${socket.id}`);
    
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('partner:disconnected');
        activePartners.delete(partnerId);
      }
      activePartners.delete(socket.id);
    }
    
    removeFromQueues(socket.id);
    userModes.delete(socket.id);
  });
});

// Match Video users function
function matchVideoUsers() {
  console.log(`🔄 Matching Video... Queue: [${videoQueue.join(', ')}]`);
  
  while (videoQueue.length >= 2) {
    const user1 = videoQueue.shift();
    const user2 = videoQueue.shift();
    
    const socket1 = io.sockets.sockets.get(user1);
    const socket2 = io.sockets.sockets.get(user2);
    
    if (socket1 && socket2 && socket1.connected && socket2.connected) {
      console.log(`✅ MATCHED VIDEO! ${user1} <-> ${user2}`);
      
      activePartners.set(user1, user2);
      activePartners.set(user2, user1);
      
      socket1.emit('chat:start', { status: 'connected', partnerId: user2, type: 'video' });
      socket2.emit('chat:start', { status: 'connected', partnerId: user1, type: 'video' });
      
      socket1.emit('video:initiate', { partnerId: user2 });
      socket2.emit('video:initiate', { partnerId: user1 });
      
      socket1.emit('partner:location', { country: 'Stranger', flag: '' });
      socket2.emit('partner:location', { country: 'Stranger', flag: '' });
    } else {
      console.log(`❌ Failed to match video`);
      if (socket1 && socket1.connected) videoQueue.unshift(user1);
      if (socket2 && socket2.connected) videoQueue.unshift(user2);
      break;
    }
  }
}

// Match Text users function
function matchTextUsers() {
  console.log(`🔄 Matching Text... Queue: [${textQueue.join(', ')}]`);
  
  while (textQueue.length >= 2) {
    const user1 = textQueue.shift();
    const user2 = textQueue.shift();
    
    const socket1 = io.sockets.sockets.get(user1);
    const socket2 = io.sockets.sockets.get(user2);
    
    if (socket1 && socket2 && socket1.connected && socket2.connected) {
      console.log(`✅ MATCHED TEXT! ${user1} <-> ${user2}`);
      
      activePartners.set(user1, user2);
      activePartners.set(user2, user1);
      
      socket1.emit('chat:start', { status: 'connected', partnerId: user2, type: 'text' });
      socket2.emit('chat:start', { status: 'connected', partnerId: user1, type: 'text' });
    } else {
      console.log(`❌ Failed to match text`);
      if (socket1 && socket1.connected) textQueue.unshift(user1);
      if (socket2 && socket2.connected) textQueue.unshift(user2);
      break;
    }
  }
}

// Debug endpoint
app.get('/debug', (req, res) => {
  res.json({
    connections: io.engine.clientsCount,
    videoQueue: videoQueue,
    textQueue: textQueue,
    userModes: Array.from(userModes.entries()),
    activePartners: Array.from(activePartners.entries())
  });
});

app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    connections: io.engine.clientsCount
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🚀 Server running on port ${PORT}`);
  console.log(`📡 WebSocket: ws://localhost:${PORT}`);
  console.log(`🔗 Frontend URL: ${process.env.FRONTEND_URL || 'http://localhost:5173'}\n`);
});