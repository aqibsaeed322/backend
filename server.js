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

// Simple in-memory queue (No Redis needed)
const waitingQueue = []; // Users waiting for video chat
const activePartners = new Map(); // socketId -> partnerId

io.on('connection', (socket) => {
  console.log(`🟢 User connected: ${socket.id}`);
  console.log(`📊 Total users: ${io.engine.clientsCount}`);

  // Start Video Chat
  socket.on('video:start', () => {
    console.log(`🎥 Video chat started: ${socket.id}`);
    
    // Add to waiting queue
    waitingQueue.push(socket.id);
    console.log(`📋 Queue size: ${waitingQueue.length}`);
    
    // Try to match
    matchUsers();
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

  // Chat message
  socket.on('chat:message', (message) => {
    const partnerId = activePartners.get(socket.id);
    if (partnerId) {
      console.log(`💬 Message: ${socket.id} -> ${partnerId}`);
      io.to(partnerId).emit('chat:message', message);
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
    
    // Add back to queue
    waitingQueue.push(socket.id);
    matchUsers();
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
    
    // Remove from queue
    const index = waitingQueue.indexOf(socket.id);
    if (index !== -1) waitingQueue.splice(index, 1);
  });
});

// Match users function
function matchUsers() {
  console.log(`🔄 Matching... Queue: [${waitingQueue.join(', ')}]`);
  
  while (waitingQueue.length >= 2) {
    const user1 = waitingQueue.shift();
    const user2 = waitingQueue.shift();
    
    const socket1 = io.sockets.sockets.get(user1);
    const socket2 = io.sockets.sockets.get(user2);
    
    if (socket1 && socket2 && socket1.connected && socket2.connected) {
      console.log(`✅ MATCHED! ${user1} <-> ${user2}`);
      
      activePartners.set(user1, user2);
      activePartners.set(user2, user1);
      
      socket1.emit('chat:start', { status: 'connected', partnerId: user2, type: 'video' });
      socket2.emit('chat:start', { status: 'connected', partnerId: user1, type: 'video' });
      
      socket1.emit('video:initiate', { partnerId: user2 });
      socket2.emit('video:initiate', { partnerId: user1 });
      
      socket1.emit('partner:location', { country: 'Stranger', flag: '' });
      socket2.emit('partner:location', { country: 'Stranger', flag: '' });
    } else {
      console.log(`❌ Failed to match`);
      if (socket1 && socket1.connected) waitingQueue.unshift(user1);
      if (socket2 && socket2.connected) waitingQueue.unshift(user2);
      break;
    }
  }
}

// Debug endpoint
app.get('/debug', (req, res) => {
  res.json({
    connections: io.engine.clientsCount,
    waitingQueue: waitingQueue,
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