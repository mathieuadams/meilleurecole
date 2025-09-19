// src/routes/contactRoutes.js
const express = require('express');
const router = express.Router();
const nodemailer = require('nodemailer');

// Simple rate limiting
const requestCounts = new Map();
const RATE_LIMIT_WINDOW = 15 * 60 * 1000; // 15 minutes
const MAX_REQUESTS = 3;

function checkRateLimit(ip) {
  const now = Date.now();
  const userRequests = requestCounts.get(ip) || [];
  
  // Filter out old requests
  const recentRequests = userRequests.filter(time => now - time < RATE_LIMIT_WINDOW);
  
  if (recentRequests.length >= MAX_REQUESTS) {
    return false;
  }
  
  recentRequests.push(now);
  requestCounts.set(ip, recentRequests);
  return true;
}

// Clean up old entries periodically
setInterval(() => {
  const now = Date.now();
  for (const [ip, times] of requestCounts.entries()) {
    const recent = times.filter(time => now - time < RATE_LIMIT_WINDOW);
    if (recent.length === 0) {
      requestCounts.delete(ip);
    } else {
      requestCounts.set(ip, recent);
    }
  }
}, RATE_LIMIT_WINDOW);

// POST /api/contact
router.post('/contact', async (req, res) => {
  try {
    // Check rate limit
    const clientIp = req.ip || req.connection.remoteAddress;
    if (!checkRateLimit(clientIp)) {
      return res.status(429).json({
        success: false,
        error: 'Too many requests. Please try again in 15 minutes.'
      });
    }

    const { name, email, subject, message, website } = req.body;
    
    // Honeypot check
    if (website) {
      return res.json({ success: true });
    }
    
    // Basic validation
    if (!name || !email || !subject || !message) {
      return res.status(400).json({
        success: false,
        error: 'All fields are required'
      });
    }
    
    // Email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid email address'
      });
    }
    
    // Length validation
    if (message.length < 10 || message.length > 2000) {
      return res.status(400).json({
        success: false,
        error: 'Message must be between 10 and 2000 characters'
      });
    }
    
    // Create transporter
    const transporter = nodemailer.createTransporter({
      service: 'gmail',
      auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS
      }
    });
    
    // Email HTML content
    const htmlContent = `
      <h2>New Contact Form Submission</h2>
      <hr>
      <p><strong>Name:</strong> ${name}</p>
      <p><strong>Email:</strong> ${email}</p>
      <p><strong>Subject:</strong> ${subject}</p>
      <p><strong>Date:</strong> ${new Date().toLocaleString('en-GB')}</p>
      <hr>
      <h3>Message:</h3>
      <p style="white-space: pre-wrap;">${message}</p>
      <hr>
      <p><em>Reply directly to: ${email}</em></p>
    `;
    
    // Send email
    const mailOptions = {
      from: process.env.EMAIL_USER,
      to: process.env.EMAIL_TO || process.env.EMAIL_USER,
      replyTo: email,
      subject: `[Contact Form] ${subject} - from ${name}`,
      html: htmlContent,
      text: `Name: ${name}\nEmail: ${email}\nSubject: ${subject}\n\nMessage:\n${message}`
    };
    
    await transporter.sendMail(mailOptions);
    
    // Log for monitoring
    console.log(`Contact form: ${name} (${email}) - ${subject}`);
    
    res.json({
      success: true,
      message: 'Message sent successfully'
    });
    
  } catch (error) {
    console.error('Contact form error:', error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to send message. Please try again later.'
    });
  }
});

module.exports = router;