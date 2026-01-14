# CS553 – Cloud Computing  
## Assignment 5 – Hashgen, Sort, & Search on Hadoop/Spark  

**Course:** CS553 – Cloud Computing  
**Team:** Om Ashokkumar Patel, Het Patel  

---

## 1. Repository layout and important notes

- The assignment asks for a file named `HadoopVault.java`.  
  In our repo the equivalent implementation is:

  - **`HashSort.java`** in the root directory  
    → this is our final Hadoop MapReduce sorter and plays the role of `HadoopVault.java`.

- We uploaded **all the code we used plus all experiments**:

  - **Root directory**  
    - Contains the code and configs that **compile and run correctly** and were used for the numbers in the report.  
    - Includes:
      - `HashSort.java`
      - `hashsort.jar`
      - `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`
      - PNG screenshots (job start, success, hexdump, vault verify, etc)
      - `Assignment 5.pdf` (our written report)

  - **`implemented`**
  	- It has everything asked in the assignment.
    - This folder contains **all the other implementations and scripts we tried**, including Hadoop and Spark code, helper shell scripts, and older versions that hit errors (especially at 64 GB).  
    - Some of this code does not fully complete on large datasets, but shows the work we attempted.  

If you just want to re-run the working pipeline, **use the files in the root**.  
If you want to see all our attempts and extra scripts, look in **`implemented`**.

---

## 2. High-level project summary

This assignment implements and evaluates:

- **Native programs**
  - `hashgen` (from HW4) for generating BLAKE3-based hash records
  - `vaultx` (provided binary) for high-performance single-node hash generation, sort, and search

- **Hadoop MapReduce**
  - `HashSort.java` (our HadoopVault)  
  - Reads 16-byte records (10-byte hash + 6-byte nonce) and sorts them using `BytesWritable` keys  
  - Writes sorted output as a `SequenceFile` to HDFS

- **Spark**
  - Spark sort implementation is included inside `implemented`  
  - Works on smaller datasets but we could not complete all large-scale (32 GB and 64 GB) runs because of memory, shuffle and storage limits in our LXD-based cluster

- **Performance and scaling**
  - Experiments on:
    - 1 small.instance (4 cores, 4 GB RAM)
    - 1 large.instance (32 cores, 32 GB RAM)
    - 8 small.instances + 1 tiny.instance for Hadoop
  - Comparison between:
    - `hashgen`
    - `vaultx`
    - Hadoop sort
    - Spark sort (partially, plus theoretical comparison using published results)

- **Search**
  - We ran search benchmarks with `hashgen` and `vaultx` on a single large.instance  
  - Distributed search with Hadoop and Spark is discussed theoretically, but not fully implemented because large 32 GB / 64 GB sorts did not complete reliably on the cluster

All of this is summarized and explained in **`Assignment 5.pdf`**.

## 3. Environment and configuration (short version)

- **Chameleon Cloud nodes**
  - `compute-skylake`: 24 cores, 128 GB RAM, 250 GB SSD  
  - `compute_icelake_r650`: Intel Xeon Platinum 8380, 160 vCPUs, 251 GiB RAM, ~419 GiB disk

- **Virtualization**
  - LXD containers: `tiny.instance`, `small.instance`, `large.instance` as required

- **Software**
  - Ubuntu 24.04  
  - Java 17 (Temurin)  
  - Hadoop 3.3.6 with:
    - `fs.defaultFS = hdfs://localhost:9000`
    - replication factor = 1
  - Spark 3.5.0 (standalone master `spark://large-1:7077`)

Config files in the root (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`, `spark-env.sh`) match the setup described in the report.

---

## 4. Hadoop sort (HashSort = HadoopVault)

### Build

```bash
javac -cp "$(hadoop classpath)" HashSort.java
jar cfe hashsort.jar HashSort HashSort*.class
````

### Run

```bash
# Example: sort 16 GB input previously put on HDFS
hadoop jar hashsort.jar HashSort /data-16GB.bin /data-16GB-sorted
```

* Input: HDFS file of 16-byte fixed-length records.
* Output: HDFS directory with sorted SequenceFile(s).

This is the main file used for the Hadoop performance numbers in the report.

---

## 5. Native hashgen and vaultx runs

We used:

* `hashgen` from HW4 for our own generation and sort pipeline.
* `vaultx` provided by the instructor as a highly optimized baseline.

Typical commands (paths may differ in your environment):

```bash
# Generate 16 GB with vaultx
./vaultx -t 32 -i 1 -m 2048 -k 30 -g data-16GB.tmp -f data-16GB.bin
./vaultx -f data-16GB.bin -v true

# Put on HDFS for Hadoop/Spark
hdfs dfs -put data-16GB.bin /data-16GB.bin
```

All performance numbers for vaultx and hashgen are documented in the PDF.

---

## 6. Spark sort and limitations

Spark sort code and scripts (for SparkVault) live inside **`implemented`**.
They implement:

* Hash generation or loading from HDFS
* RDD/DataFrame sort
* Writing sorted results back to HDFS

This worked at smaller scales, but full 32 GB and 64 GB experiments failed due to:

* Executor memory pressure and heavy spilling
* Shuffle directories running out of space in LXD containers
* HDFS entering safe mode while under heavy I/O

Because of these issues we did not rely on Spark results for the largest datasets and instead combined our partial data with predictions using published Spark vs Hadoop comparisons. The reasoning is explained in **Assignment 5 report.pdf**.

---

## 7. 64 GB experiments

We tried multiple times to run 64 GB datasets with:

* `hashgen`
* `vaultx`
* Hadoop sort
* Spark sort

Each time we hit practical limits:

* Local and LXD storage pools filled with temporary and spill files
* Some `.bin` outputs became 0 bytes after crashes
* NameNode went into safe mode and refused writes
* Spark jobs failed or hung due to out-of-memory and lack of disk space for shuffle

Because of that, the 64 GB rows in the tables are marked **N/A**, and we only discuss 64 GB behavior qualitatively (what we expect from literature) instead of showing our own timings.

---

## 8. How to reproduce the working pipeline

Minimal steps on a fresh node with Java and Hadoop installed:

```bash
# 1. Copy repo
git clone <repo-url> cs553-hw5
cd cs553-hw5

# 2. Copy Hadoop configs
cp core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml $HADOOP_HOME/etc/hadoop/

# 3. Generate data (example with vaultx)
./vaultx -t 32 -i 1 -m 2048 -k 30 -g data-16GB.tmp -f data-16GB.bin
hdfs dfs -put data-16GB.bin /data-16GB.bin

# 4. Build and run Hadoop sort
javac -cp "$(hadoop classpath)" HashSort.java
jar cfe hashsort.jar HashSort HashSort*.class
hadoop jar hashsort.jar HashSort /data-16GB.bin /data-16GB-sorted
```

Screenshots used in the report (PNG files in the root) show these steps and results; they are included as required evidence, but not described in detail here.
