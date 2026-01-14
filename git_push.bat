@echo off
echo ===================================
echo Git Push Script for Litres_Parser
echo ===================================
echo.

REM Check if git is initialized
if not exist .git (
    echo [1] Initializing git repository...
    git init
    git branch -M main
) else (
    echo [1] Git repository already initialized
)

REM Check remote
echo.
echo [2] Checking remote...
git remote -v
if errorlevel 1 (
    echo Adding remote origin...
    git remote add origin https://github.com/Temekutza/Litres_Parser.git
) else (
    git remote set-url origin https://github.com/Temekutza/Litres_Parser.git
)

REM Add all files
echo.
echo [3] Adding files to git...
git add .

REM Commit
echo.
echo [4] Creating commit...
git commit -m "Major update: improved parsing stability, fixed LiveLib/ratings, enhanced reviews"

REM Pull first (in case there are remote changes)
echo.
echo [5] Pulling latest changes from GitHub...
git pull origin main --rebase

REM Push
echo.
echo [6] Pushing to GitHub...
git push -u origin main

echo.
echo ===================================
echo DONE! Check https://github.com/Temekutza/Litres_Parser
echo ===================================
pause
