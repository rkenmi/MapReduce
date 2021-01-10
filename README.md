"# mit-6.824" 

# MapReduce
## How to debug in JetBrains GoLand
1. Build `wc.go` with these parameters `-gcflags="all=-N -l"`. 
   Compiled version must have same flags as debug run.
    - What this looks like:
      ```go
      go build -gcflags="all=-N -l" --buildmode=plugin ../mrapps/wc.go
      ```
2. Now run Delve on a separate terminal (make sure to install before proceeding). Use the following command:
   ```go
   dlv debug --headless --listen=:2345 --api-version=2 --accept-multiclient mrsequential.go -- wc.so pg*.txt
   ```
   
   Note that this will run on port 2345 (this is the default debug port on GoLand IDE).
   The `--` separator is needed for application arguments that needs to be passed into `main` function
   
3. Now go to GoLand IDE, add breakpoints, and start a debugger instance on port 2345 using the Go Remote presets.