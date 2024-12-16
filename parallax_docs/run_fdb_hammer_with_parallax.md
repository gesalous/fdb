# Running fdb-hammer with Parallax

1. **Edit the Configuration File**
   - Open the configuration file:
     ```
     fdb-kv/tests/fdb/etc/fdb/config.yaml
     ```
   - Set the engine to `daos`.
   - *(Optional)* Change the root path to mounted NVMe folders for realistic performance testing.

2. **Build fdb-kv**
   - Read the `README.md` file in the project, and also check the `Dockerfile` to understand how to download the necessary dependencies for the project.
   - **IMPORTANT:** Use the flag:
     ```
     -DUSE_PARALLAX=ON
     ```
     when building the project.

3. **Build Parallax**
   - Build Parallax with the following flag enabled:
     ```
     -DKV_MAX_SIZE_128K=ON
     ```
   - Follow the Parallax build instructions if additional guidance is needed.

4. **Start the Parallax Server**
   - Run the Parallax server executable on `localhost:8080`.
   - Ensure the server is running before starting `fdb-hammer`.

5. **Run the fdb-hammer executable**
   - Execute the `fdb-hammer` binary with the required parameters.
   - **IMPORTANT:** Always set the `nparams` parameter to `4` when running the executable.
