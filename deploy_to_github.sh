#!/bin/bash

echo "🚀 DEPLOYING TO GITHUB"
echo "======================"
echo ""

cd /Users/bentenner/Dictionary/2025-Q2

# Initialize git
echo "1️⃣ Initializing git..."
git init

# Add all files
echo "2️⃣ Adding files..."
git add .

# Commit
echo "3️⃣ Committing..."
git commit -m "Initial commit - Medicare Part D Intelligence Platform with year toggle"

# Add remote
echo "4️⃣ Adding remote..."
git remote add origin https://github.com/btenner84/Formulary.git

# Push to main
echo "5️⃣ Pushing to GitHub..."
git branch -M main
git push -u origin main

echo ""
echo "✅ SUCCESSFULLY PUSHED TO GITHUB!"
echo "📦 Repository: https://github.com/btenner84/Formulary"
echo ""
echo "🎯 NEXT STEP: Deploy to Railway"
echo "   1. Go to https://railway.app/"
echo "   2. Click 'Start a New Project'"
echo "   3. Choose 'Deploy from GitHub repo'"
echo "   4. Select 'btenner84/Formulary'"
echo ""

