@echo off
if not exist .paket\paket.exe (
  .paket\paket.bootstrapper.exe
  if errorlevel 1 (
    exit /b %errorlevel%
  )
)
cd src
if not exist packages\build\FAKE\tools\FAKE.exe (
  if not exist paket.lock (
    ..\.paket\paket.exe install
  ) else (
    ..\.paket\paket.exe restore
  )
  if errorlevel 1 (
    exit /b %errorlevel%
  )
)
cd ..
src\packages\build\FAKE\tools\FAKE.exe %* --fsiargs build.fsx
