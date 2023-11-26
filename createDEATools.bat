echo off
cls

echo Running the DEA Tools Create Environment......... 

:: get the current path
set currentpath=%~dp0
echo %currentpath%

:: get the default repo directory
set /p "repopath=Supply your path to your repo [default c:\dev\DEA_Tools]:" || set "repopath=c:\dev\DEA_Tools"

:: determine if we need to create a virtual evironment
set /p "ve=Create Virtual Environment [/env/envDEATools] (Y/N):"
set result=false
if %ve%==y set result=true
if %ve%==Y set result=true
if %result%==true (
	:: see if folder exists or not
	if not exist \env (
		echo Creating Base Virtual Environment Folder [\env]
		mkdir \env
	)	
	if not exist \env\envDEATools (
		echo Creating Virtual Environment [envDEATools]
		cd \env
		echo on
		python -m venv envDEATools
		echo off
	)	
)

:: activate the virtual environment
echo Activating Virtual Environment [envDEATools]
call \env\envDEATools\Scripts\activate.bat

:: change into our dev environment where the repo is loaded
cd %repopath%
echo Validating Data Structure
if not exist "inputs" mkdir inputs
if not exist "outputs" mkdir outputs

:: do our pip upgrade for the virtual environment
echo Updating pip
echo on
python -m pip install --upgrade pip
echo off

:: lets install all our packages/modules
echo Installing Modules
echo on
pip install -r requirements.txt
echo off

:: deactivate the virtual environment
call deactivate

:: return where we started
cd %currentpath%
echo 
echo Done...........